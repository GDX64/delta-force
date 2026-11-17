//! Matching a file's columns to a table's, shared by every file format.
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};

use crate::lake::{Column, arrow_schema};
use crate::{Error, Result};

/// For each table column, the index of the file column that fills it.
/// File columns are matched by name, ignoring case and order. Fails on
/// unknown or duplicate file columns and on missing required columns.
pub(super) fn match_columns<'a>(
    names: impl IntoIterator<Item = &'a str>,
    columns: &[Column],
) -> Result<Vec<Option<usize>>, String> {
    let mut source = vec![None; columns.len()];
    for (position, name) in names.into_iter().enumerate() {
        let target = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("column {name:?} is not in the table"))?;
        if source[target].replace(position).is_some() {
            return Err(format!("column {:?} appears twice", columns[target].name));
        }
    }
    for (column, source) in columns.iter().zip(&source) {
        if source.is_none() && !column.nullable {
            return Err(format!("required column {:?} is missing", column.name));
        }
    }
    Ok(source)
}

/// Build a batch of the table columns the file has, in table order, from
/// file `arrays` already in the table's types. A null in a required column
/// is an error. Columns the file lacks are left out: see [`complete`].
pub(super) fn assemble(
    columns: &[Column],
    source: &[Option<usize>],
    mut take: impl FnMut(usize) -> Result<ArrayRef, String>,
    rows: usize,
) -> Result<RecordBatch, String> {
    let (mut present, mut arrays) = (Vec::new(), Vec::new());
    for (column, source) in columns.iter().zip(source) {
        if let Some(i) = *source {
            let array = take(i)?;
            if !column.nullable && array.null_count() > 0 {
                return Err(format!("required column {:?} has empty values", column.name));
            }
            present.push(column.clone());
            arrays.push(array);
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::new(arrow_schema(&present)), arrays, &options)
        .map_err(|e| e.to_string())
}

/// `batch` from [`assemble`] in all of the table's columns: the ones the
/// file lacks (nullable, or the file would have been rejected) are null.
pub(crate) fn complete(columns: &[Column], batch: &RecordBatch) -> Result<RecordBatch> {
    let arrays = columns
        .iter()
        .map(|c| match batch.column_by_name(&c.name) {
            Some(array) => array.clone(),
            None => new_null_array(&c.data_type.to_arrow(), batch.num_rows()),
        })
        .collect();
    RecordBatch::try_new(Arc::new(arrow_schema(columns)), arrays)
        .map_err(|e| Error::Storage(e.into()))
}
