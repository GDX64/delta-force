//! Reading CSV files into a table's columns.
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use delta_kernel::arrow::csv::ReaderBuilder;
use delta_kernel::arrow::csv::reader::Format;

use super::columns::{assemble, match_columns};
use crate::file::LakeFile;
use crate::lake::Column;
use crate::{Error, Result};

/// Read `file` as CSV with a header row, as batches in the table's columns.
pub(super) async fn read(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Vec<RecordBatch>> {
    let bytes = file.read().await?;
    let (path, columns) = (file.path().to_owned(), columns.clone());
    // Parsing is CPU work: keep it off the async worker threads.
    tokio::task::spawn_blocking(move || parse(&bytes, &columns))
        .await
        .map_err(|e| Error::Storage(e.into()))?
        .map_err(|message| Error::InvalidFile { file: path, message })
}

fn parse(bytes: &[u8], columns: &[Column]) -> Result<Vec<RecordBatch>, String> {
    let format = Format::default().with_header(true);
    let (header, _) = format
        .infer_schema(Cursor::new(bytes), Some(0))
        .map_err(|e| e.to_string())?;
    let names = header.fields().iter().map(|f| f.name().as_str());
    let source = match_columns(names, columns)?;

    // Parse each CSV column straight into its table column's type.
    let mut fields = vec![None; header.fields().len()];
    for (column, source) in columns.iter().zip(&source) {
        if let Some(i) = *source {
            let name = header.field(i).name();
            fields[i] = Some(Field::new(name, column.data_type.to_arrow(), true));
        }
    }
    let schema = Schema::new(fields.into_iter().flatten().collect::<Vec<_>>());

    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(Cursor::new(bytes))
        .map_err(|e| e.to_string())?;
    reader
        .map(|batch| {
            let batch = batch.map_err(|e| e.to_string())?;
            assemble(columns, &source, |i| Ok(batch.column(i).clone()), batch.num_rows())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataType;

    fn columns() -> Vec<Column> {
        vec![
            Column { name: "id".into(), data_type: DataType::Long, nullable: false },
            Column { name: "score".into(), data_type: DataType::Double, nullable: true },
        ]
    }

    fn error(csv: &str) -> String {
        parse(csv.as_bytes(), &columns()).unwrap_err()
    }

    #[test]
    fn header_names_ignore_case_and_order() {
        let batches = parse(b"SCORE,Id\n0.5,7\n", &columns()).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[test]
    fn missing_nullable_column_is_null() {
        let batches = parse(b"id\n1\n2\n", &columns()).unwrap();
        assert_eq!(batches[0].column(1).null_count(), 2);
    }

    #[test]
    fn rejects_files_that_do_not_fit() {
        assert!(error("id,extra\n1,x\n").contains("\"extra\" is not in the table"));
        assert!(error("score\n0.5\n").contains("required column \"id\" is missing"));
        assert!(error("id,ID\n1,2\n").contains("appears twice"));
        assert!(error("id,score\n,0.5\n").contains("required column \"id\" has empty values"));
        assert!(error("id\nabc\n").contains("abc"));
    }
}
