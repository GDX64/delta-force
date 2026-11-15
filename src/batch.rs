//! [`RowBatch`]: rows to write, stored as columns.
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};

use crate::lake::{Column, DataType, arrow_schema};
use crate::{Error, Result, Row, Value};

/// Rows to write to a table, stored column by column as they are pushed.
///
/// Get one from [`Table::batch`](crate::Table::batch), so it knows the
/// table's columns and can check each row as it arrives.
pub struct RowBatch {
    columns: Arc<[Column]>,
    builders: Vec<ColumnBuilder>,
    len: usize,
    /// Reused per push: the row's values, checked and in column order.
    scratch: Vec<Value>,
}

impl RowBatch {
    pub(crate) fn new(columns: Arc<[Column]>) -> Self {
        let builders = columns
            .iter()
            .map(|c| ColumnBuilder::new(c.data_type))
            .collect();
        Self {
            columns,
            builders,
            len: 0,
            scratch: Vec::new(),
        }
    }

    /// Add a row. Columns left out are null. On error the batch is unchanged.
    pub fn push(&mut self, mut row: Row) -> Result<()> {
        self.scratch.clear();
        for column in self.columns.iter() {
            let value = row.take(&column.name).unwrap_or(Value::Null);
            if value.is_null() && !column.nullable {
                return Err(Error::MissingValue(column.name.clone()));
            }
            let found = value.kind();
            let value = coerce(column.data_type, value).ok_or_else(|| Error::TypeMismatch {
                column: column.name.clone(),
                expected: column.data_type.to_string(),
                found: found.to_owned(),
            })?;
            self.scratch.push(value);
        }
        if let Some((name, _)) = row.columns().next() {
            return Err(Error::ColumnNotFound(name.to_owned()));
        }
        // Everything is checked; append now so a failed push leaves no trace.
        for (builder, value) in self.builders.iter_mut().zip(self.scratch.drain(..)) {
            builder.append(value);
        }
        self.len += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn finish(self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect();
        RecordBatch::try_new(Arc::new(arrow_schema(&self.columns)), arrays)
            .map_err(|e| Error::Storage(e.into()))
    }
}

/// Convert `value` to the variant `data_type` stores, where lossless.
fn coerce(data_type: DataType, value: Value) -> Option<Value> {
    /// Largest integer a double holds exactly.
    const MAX_EXACT: i64 = 1 << 53;
    Some(match (data_type, value) {
        (_, Value::Null) => Value::Null,
        (DataType::Boolean, v @ Value::Boolean(_)) => v,
        (DataType::Integer, v) => Value::Integer(i32::try_from(v.as_i64()?).ok()?),
        (DataType::Long, v) => Value::Long(v.as_i64()?),
        (DataType::Double, v @ Value::Double(_)) => v,
        (DataType::Double, v) => Value::Double(v.as_i64().filter(|i| i.abs() <= MAX_EXACT)? as f64),
        (DataType::String, v @ Value::String(_)) => v,
        _ => return None,
    })
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Integer(Int32Builder),
    Long(Int64Builder),
    Double(Float64Builder),
    String(StringBuilder),
}

impl ColumnBuilder {
    fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(BooleanBuilder::new()),
            DataType::Integer => Self::Integer(Int32Builder::new()),
            DataType::Long => Self::Long(Int64Builder::new()),
            DataType::Double => Self::Double(Float64Builder::new()),
            DataType::String => Self::String(StringBuilder::new()),
        }
    }

    /// Append a value already [`coerce`]d to this column's type.
    fn append(&mut self, value: Value) {
        match (self, value) {
            (Self::Boolean(b), Value::Boolean(v)) => b.append_value(v),
            (Self::Integer(b), Value::Integer(v)) => b.append_value(v),
            (Self::Long(b), Value::Long(v)) => b.append_value(v),
            (Self::Double(b), Value::Double(v)) => b.append_value(v),
            (Self::String(b), Value::String(v)) => b.append_value(v),
            (Self::Boolean(b), Value::Null) => b.append_null(),
            (Self::Integer(b), Value::Null) => b.append_null(),
            (Self::Long(b), Value::Null) => b.append_null(),
            (Self::Double(b), Value::Null) => b.append_null(),
            (Self::String(b), Value::Null) => b.append_null(),
            _ => unreachable!("value was coerced to the column type"),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Integer(mut b) => Arc::new(b.finish()),
            Self::Long(mut b) => Arc::new(b.finish()),
            Self::Double(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => ArrayBuilder::finish(&mut b),
        }
    }
}
