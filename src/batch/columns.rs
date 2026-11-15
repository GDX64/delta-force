//! Matching a file's columns to a table's, shared by every file format.
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};

use crate::lake::{Column, arrow_schema};

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

/// Build a batch in the table's columns from file `arrays` already in the
/// table's types: missing nullable columns become null, and a null in a
/// required column is an error.
pub(super) fn assemble(
    columns: &[Column],
    source: &[Option<usize>],
    mut take: impl FnMut(usize) -> Result<ArrayRef, String>,
    rows: usize,
) -> Result<RecordBatch, String> {
    let arrays = columns
        .iter()
        .zip(source)
        .map(|(column, source)| match source {
            Some(i) => {
                let array = take(*i)?;
                if !column.nullable && array.null_count() > 0 {
                    return Err(format!("required column {:?} has empty values", column.name));
                }
                Ok(array)
            }
            None => Ok(new_null_array(&column.data_type.to_arrow(), rows)),
        })
        .collect::<Result<Vec<_>, String>>()?;
    RecordBatch::try_new(Arc::new(arrow_schema(columns)), arrays).map_err(|e| e.to_string())
}
