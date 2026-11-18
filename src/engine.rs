//! A tiny, synchronous [`Engine`] on top of any [`ObjectStore`].
//!
//! The kernel decides *what* to read and write; the engine does the actual I/O
//! and data processing through four handlers:
//!
//! * [`StoreStorage`]: list / read / put / head / delete files (via the store)
//! * [`StoreJson`]: read and write JSON commit files
//! * [`StoreParquet`]: read and write Parquet data and checkpoint files
//! * the kernel's Arrow `EvaluationHandler`, reused as-is for expressions
//!
//! The kernel hands the engine absolute URLs; the URL's path is used as the
//! object path inside the store. The kernel API is synchronous, so the store is
//! wrapped in a [`SyncObjectStore`] that blocks on the async calls.
use std::io::{BufReader, Cursor};
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::arrow_utils::{
    RowIndexBuilder, build_json_reorder_indices, fixup_json_read, fixup_parquet_read,
    json_arrow_schema, ordering_needs_row_indexes, parquet_read_plan, parse_json, to_json_bytes,
};
use delta_kernel::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use delta_kernel::engine::reader_options;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReaderBuilder,
};
use delta_kernel::parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use delta_kernel::parquet::basic::Compression as Codec;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::transaction::BoundWriteContext;
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    FileSlice, JsonHandler, KernelError, KernelResult, KernelResultIterator,
    KernelResultIteratorStatic, ParquetFooter, ParquetHandler, PredicateRef, StorageHandler,
};
use delta_kernel_default_engine::{
    build_add_file_metadata, parquet::DataFileMetadata, stats::collect_stats,
};
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use crate::compression::Compression;
use crate::sync_store::SyncObjectStore;

/// An [`Engine`] that reads and writes through an [`ObjectStore`].
pub struct ObjectStoreEngine {
    storage: Arc<StoreStorage>,
    json: Arc<StoreJson>,
    parquet: Arc<StoreParquet>,
    evaluation: Arc<ArrowEvaluationHandler>,
}

impl ObjectStoreEngine {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let io = SyncObjectStore::new(store);
        Self {
            storage: Arc::new(StoreStorage { io: io.clone() }),
            json: Arc::new(StoreJson { io: io.clone() }),
            parquet: Arc::new(StoreParquet { io }),
            evaluation: Arc::new(ArrowEvaluationHandler {}),
        }
    }

    /// Write `batch` as a new Parquet file in the table and return the `add`
    /// action metadata to hand to `Transaction::add_files`.
    ///
    /// Not part of the [`Engine`] trait: the kernel leaves "how do I turn my
    /// data into a data file" entirely to the engine.
    #[tracing::instrument(skip_all, fields(rows = batch.num_rows()))]
    pub fn write_data_file(
        &self,
        batch: RecordBatch,
        context: &BoundWriteContext,
        compression: Compression,
    ) -> KernelResult<Box<dyn EngineData>> {
        // Logical -> physical (column mapping, partition columns, ...).
        let span = tracing::info_span!("to_physical").entered();
        let input_schema = StructType::try_from_arrow(batch.schema().as_ref())?;
        let physical = self
            .evaluation
            .new_expression_evaluator(
                Arc::new(input_schema),
                context.logical_to_physical().clone(),
                context.physical_data_schema().clone().into(),
            )?
            .evaluate(&ArrowEngineData::new(batch))?;
        let physical: RecordBatch = (*ArrowEngineData::try_from_engine_data(physical)?).into();
        drop(span);

        let stats = tracing::info_span!("collect_stats").in_scope(|| {
            collect_stats(
                &physical,
                context.stats_columns(),
                context.physical_data_schema().as_ref(),
            )
        })?;

        let mut dir = context.write_dir();
        if !dir.path().ends_with('/') {
            dir.set_path(&format!("{}/", dir.path()));
        }
        let location = dir.join(&format!("{}.parquet", uuid::Uuid::new_v4()))?;
        self.parquet
            .write_batches(&location, &[physical], compression.codec())?;
        let meta = self.storage.head(&location)?;

        build_add_file_metadata(DataFileMetadata::new(meta, stats), context)
    }
}

impl Engine for ObjectStoreEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

// ---------------------------------------------- kernel <-> store adapters --

/// The kernel names objects by absolute URL; the URL's path is the object path.
fn object_path(url: &Url) -> KernelResult<Path> {
    Path::from_url_path(url.path())
        .map_err(|e| KernelError::generic(format!("bad object path in {url}: {e}")))
}

/// Map store errors onto the errors the kernel expects (it matches on
/// `FileAlreadyExists` to detect a lost commit race).
fn store_error(url: &Url, e: object_store::Error) -> KernelError {
    match e {
        object_store::Error::NotFound { .. } => KernelError::file_not_found(url),
        object_store::Error::AlreadyExists { .. } => {
            KernelError::FileAlreadyExists(url.to_string())
        }
        other => KernelError::generic(other.to_string()),
    }
}

fn get(store: &SyncObjectStore, url: &Url) -> KernelResult<Bytes> {
    store.get(&object_path(url)?).map_err(|e| store_error(url, e))
}

/// With `overwrite = false` this is the atomic "create if absent" that makes
/// concurrent commits safe: only one writer can win a given version.
fn put(store: &SyncObjectStore, url: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
    store
        .put(&object_path(url)?, data, overwrite)
        .map_err(|e| store_error(url, e))
}

fn head(store: &SyncObjectStore, url: &Url) -> KernelResult<FileMeta> {
    let meta = store.head(&object_path(url)?).map_err(|e| store_error(url, e))?;
    Ok(FileMeta {
        location: url.clone(),
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
    })
}

// ---------------------------------------------------------------- storage --

pub struct StoreStorage {
    io: SyncObjectStore,
}

impl StorageHandler for StoreStorage {
    /// Files in the same directory as `path` whose name is `>=` its file name,
    /// in lexicographic order (a trailing `/` lists the whole directory).
    fn list_from(&self, path: &Url) -> KernelResult<KernelResultIteratorStatic<FileMeta>> {
        let full = object_path(path)?;
        let (dir, start): (Path, String) = if path.path().ends_with('/') {
            (full, String::new())
        } else {
            let start = full.filename().unwrap_or_default().to_string();
            let parts: Vec<_> = full.parts().collect();
            (parts[..parts.len().saturating_sub(1)].iter().cloned().collect(), start)
        };
        // Non-recursive: only the files directly inside `dir`.
        let listing = self
            .io
            .list_with_delimiter(&dir)
            .map_err(|e| store_error(path, e))?;

        let mut files: Vec<FileMeta> = listing
            .objects
            .into_iter()
            .filter(|o| o.location.filename().is_some_and(|n| n >= start.as_str()))
            .map(|o| {
                let mut location = path.clone();
                location.set_path(&format!("/{}", o.location.as_ref()));
                FileMeta {
                    location,
                    last_modified: o.last_modified.timestamp_millis(),
                    size: o.size,
                }
            })
            .collect();
        files.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(Box::new(files.into_iter().map(Ok)))
    }

    fn read_files(&self, files: Vec<FileSlice>) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
        let io = self.io.clone();
        Ok(Box::new(files.into_iter().map(move |(url, range)| {
            let bytes = get(&io, &url)?;
            Ok(match range {
                Some(r) => bytes.slice(r.start as usize..(r.end as usize).min(bytes.len())),
                None => bytes,
            })
        })))
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        put(&self.io, path, data, overwrite)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        let (from, to) = (object_path(src)?, object_path(dest)?);
        self.io
            .copy_if_not_exists(&from, &to)
            .map_err(|e| store_error(dest, e))
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        head(&self.io, path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        let p = object_path(path)?;
        match self.io.delete(&p) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(store_error(path, e)),
        }
    }
}

// ------------------------------------------------------------------- json --

pub struct StoreJson {
    io: SyncObjectStore,
}

impl JsonHandler for StoreJson {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let arrow_schema = Arc::new(json_arrow_schema(&schema)?);
        let reorder = build_json_reorder_indices(&schema)?;
        let mut out = Vec::new();
        for file in files {
            let location = file.location.to_string();
            let reader = ReaderBuilder::new(arrow_schema.clone())
                .with_coerce_primitive(true)
                .build(BufReader::new(Cursor::new(get(&self.io, &file.location)?)))?;
            for batch in reader {
                out.push(
                    fixup_json_read(batch?, &reorder, &location)
                        .map(|d| Box::new(d) as Box<dyn EngineData>),
                );
            }
        }
        Ok(Box::new(out.into_iter()))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: KernelResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        let bytes = to_json_bytes(data)?;
        let size = bytes.len() as FileSize;
        put(&self.io, path, bytes.into(), overwrite)?;
        Ok(size)
    }
}

// ---------------------------------------------------------------- parquet --

pub struct StoreParquet {
    io: SyncObjectStore,
}

impl StoreParquet {
    /// Write `batches` as one Parquet file at `location` using `codec`.
    fn write_batches(
        &self,
        location: &Url,
        batches: &[RecordBatch],
        codec: Codec,
    ) -> KernelResult<FileSize> {
        let first = batches
            .first()
            .ok_or_else(|| KernelError::generic("cannot write a parquet file with no data"))?;
        let properties = WriterProperties::builder().set_compression(codec).build();
        let options = ArrowWriterOptions::new()
            .with_skip_arrow_metadata(true)
            .with_properties(properties);

        let mut buffer = Vec::new();
        let span = tracing::info_span!("encode_parquet", bytes = tracing::field::Empty).entered();
        let mut writer = ArrowWriter::try_new_with_options(&mut buffer, first.schema(), options)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?; // writes the footer
        span.record("bytes", buffer.len());
        drop(span);
        let size = buffer.len() as FileSize;
        put(&self.io, location, buffer.into(), true)?;
        Ok(size)
    }
}

impl ParquetHandler for StoreParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let mut out = Vec::new();
        for file in files {
            let data = get(&self.io, &file.location)?;
            let metadata = ArrowReaderMetadata::load(&data, reader_options())?;
            // Which columns to read, and how to reorder / fill in the result.
            let (ordering, mask) = parquet_read_plan(&schema, &metadata)?;
            let mut row_indexes = ordering_needs_row_indexes(&ordering)
                .then(|| RowIndexBuilder::new(metadata.metadata().row_groups()));

            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(data, metadata);
            if let Some(mask) = mask {
                builder = builder.with_projection(mask);
            }
            // Skip row groups whose min/max stats can't match the predicate.
            if let Some(predicate) = &predicate {
                builder = builder.with_row_group_filter(predicate.as_ref(), row_indexes.as_mut());
            }
            let mut row_indexes = row_indexes.map(|b| b.build()).transpose()?;

            let location = file.location.to_string();
            for batch in builder.build()? {
                out.push(
                    fixup_parquet_read(
                        batch?,
                        &ordering,
                        row_indexes.as_mut(),
                        Some(&location),
                        Some(&schema),
                    )
                    .map(|d| Box::new(d) as Box<dyn EngineData>),
                );
            }
        }
        Ok(Box::new(out.into_iter()))
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: KernelResultIteratorStatic<Box<dyn EngineData>>,
    ) -> KernelResult<FileSize> {
        let batches = data
            .map(|b| Ok((*ArrowEngineData::try_from_engine_data(b?)?).into()))
            .collect::<KernelResult<Vec<RecordBatch>>>()?;
        self.write_batches(&location, &batches, Compression::default().codec())
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        let data = get(&self.io, &file.location)?;
        let metadata = ArrowReaderMetadata::load(&data, reader_options())?;
        let schema = Arc::new(StructType::try_from_arrow(metadata.schema().as_ref())?);
        Ok(ParquetFooter { schema })
    }
}
