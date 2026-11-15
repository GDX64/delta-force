//! Reading CSV files into a table's columns.
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use arrow_schema::{Field, Schema};
use delta_kernel::arrow::csv::ReaderBuilder;
use delta_kernel::arrow::csv::reader::Format;

use crate::file::LakeFile;
use crate::lake::{Column, arrow_schema};
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

    // Which table column each CSV column fills, matched by name ignoring case.
    let mut source = vec![None; columns.len()];
    let mut fields = Vec::with_capacity(header.fields().len());
    for (position, field) in header.fields().iter().enumerate() {
        let name = field.name();
        let target = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("column {name:?} is not in the table"))?;
        if source[target].replace(position).is_some() {
            return Err(format!("column {:?} appears twice", columns[target].name));
        }
        // Parse straight into the table's type; emptiness is checked below.
        fields.push(Field::new(name, columns[target].data_type.to_arrow(), true));
    }
    for (column, source) in columns.iter().zip(&source) {
        if source.is_none() && !column.nullable {
            return Err(format!("required column {:?} is missing", column.name));
        }
    }

    let reader = ReaderBuilder::new(Arc::new(Schema::new(fields)))
        .with_format(format)
        .build(Cursor::new(bytes))
        .map_err(|e| e.to_string())?;
    let schema = Arc::new(arrow_schema(columns));
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        let arrays = columns
            .iter()
            .zip(&source)
            .map(|(column, source)| match source {
                Some(i) => {
                    let array = batch.column(*i).clone();
                    if !column.nullable && array.null_count() > 0 {
                        return Err(format!("required column {:?} has empty values", column.name));
                    }
                    Ok(array)
                }
                None => Ok(new_null_array(&column.data_type.to_arrow(), batch.num_rows())),
            })
            .collect::<Result<Vec<ArrayRef>, String>>()?;
        batches.push(RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?);
    }
    Ok(batches)
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
