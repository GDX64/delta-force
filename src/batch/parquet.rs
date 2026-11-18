//! Reading Parquet files into a table's columns.
use std::ops::Range;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
use delta_kernel::arrow::compute::{CastOptions, cast_with_options};
use delta_kernel::parquet::arrow::ParquetRecordBatchStreamBuilder;
use delta_kernel::parquet::arrow::arrow_reader::ArrowReaderOptions;
use delta_kernel::parquet::arrow::async_reader::AsyncFileReader;
use delta_kernel::parquet::errors::ParquetError;
use delta_kernel::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use super::Batches;
use super::columns::{assemble, match_columns};
use crate::file::LakeFile;
use crate::lake::{Column, DataType};
use crate::{Error, Result};

/// Read `file` as Parquet, as a stream of batches in the table's columns.
/// Only the footer and the column data are fetched, with ranged reads.
#[tracing::instrument(name = "open_parquet", skip_all, fields(path = file.path()))]
pub(super) async fn read(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Batches> {
    let path = file.path().to_owned();
    let invalid = move |message: String| Error::InvalidFile {
        file: path.clone(),
        message,
    };
    let storage = |e: ParquetError| Error::Storage(e.into());

    let reader = Reader {
        store: file.store(),
        location: file.location().clone(),
        size: file.size().await?,
    };
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(storage)?;
    let schema = builder.schema().clone();

    // Check the whole schema from the footer before reading any data.
    let names = schema.fields().iter().map(|f| f.name().as_str());
    let source = match_columns(names, columns).map_err(&invalid)?;
    for (column, source) in columns.iter().zip(&source) {
        if let Some(i) = *source {
            let found = schema.field(i).data_type();
            if !fits(found, column.data_type) {
                return Err(invalid(format!(
                    "column {:?}: expected {}, found {found}",
                    column.name, column.data_type
                )));
            }
        }
    }

    let columns = columns.clone();
    let stream = builder.build().map_err(storage)?;
    Ok(stream
        .map(move |batch| {
            let batch = batch.map_err(storage)?;
            let _span = tracing::trace_span!("convert_batch", rows = batch.num_rows()).entered();
            let convert = |i: usize| -> Result<ArrayRef, String> {
                let target = columns[source.iter().position(|s| *s == Some(i)).unwrap()].data_type;
                // `safe: false`: a value that doesn't fit is an error, not a null.
                let options = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                cast_with_options(batch.column(i), &target.to_arrow(), &options)
                    .map_err(|e| format!("column {:?}: {e}", schema.field(i).name()))
            };
            assemble(&columns, &source, convert, batch.num_rows()).map_err(&invalid)
        })
        .boxed())
}

/// Ranged reads of one Parquet file in the lake's store.
struct Reader {
    store: Arc<dyn ObjectStore>,
    location: Path,
    size: u64,
}

fn external(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async move {
            self.store
                .get_range(&self.location, range)
                .await
                .map_err(external)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        async move {
            self.store
                .get_ranges(&self.location, &ranges)
                .await
                .map_err(external)
        }
        .boxed()
    }

    /// Reads the footer with a single suffix-sized fetch where possible.
    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(64 * 1024))
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Whether a Parquet column of type `found` can fill a `target` column.
/// Narrowing integer conversions are allowed but checked on every value.
fn fits(found: &ArrowType, target: DataType) -> bool {
    use ArrowType::*;
    let integer = matches!(
        found,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
    );
    match target {
        DataType::Boolean => matches!(found, Boolean),
        DataType::Integer | DataType::Long => integer,
        DataType::Double => matches!(found, Float16 | Float32 | Float64 | Int8 | Int16 | Int32),
        DataType::String => match found {
            Utf8 | LargeUtf8 | Utf8View => true,
            Dictionary(_, value) => matches!(**value, Utf8 | LargeUtf8 | Utf8View),
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_array::{Int64Array, StringArray};
    use delta_kernel::parquet::arrow::ArrowWriter;
    use futures_util::TryStreamExt;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    /// A one-column Parquet file named `id` in an in-memory store.
    async fn file_with_id(array: ArrayRef) -> LakeFile {
        let batch = RecordBatch::try_from_iter([("id", array)]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let file = LakeFile::new(store.clone(), &Path::default(), "f.parquet");
        store
            .put(file.location(), PutPayload::from(bytes))
            .await
            .unwrap();
        file
    }

    fn id(data_type: DataType) -> Arc<[Column]> {
        vec![Column {
            name: "id".into(),
            data_type,
            nullable: false,
        }]
        .into()
    }

    async fn read_all(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Vec<RecordBatch>> {
        read(file, columns).await?.try_collect().await
    }

    #[tokio::test]
    async fn incompatible_types_fail_before_reading_data() {
        let file = file_with_id(Arc::new(StringArray::from(vec!["1"]))).await;
        let error = read_all(&file, &id(DataType::Long))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("column \"id\": expected long, found Utf8"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn narrowing_checks_every_value() {
        let fits = file_with_id(Arc::new(Int64Array::from(vec![1, 2]))).await;
        assert_eq!(
            read_all(&fits, &id(DataType::Integer)).await.unwrap()[0].num_rows(),
            2
        );

        let overflow = file_with_id(Arc::new(Int64Array::from(vec![1, i64::MAX]))).await;
        let error = read_all(&overflow, &id(DataType::Integer))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("f.parquet: column \"id\""), "{error}");
    }
}
