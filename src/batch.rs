//! Columnar batches for writing: [`RowBatch`] and [`ChangeBatch`].
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::concat_batches;

use crate::file::{Format, LakeFile};
use crate::lake::{Column, DataType, arrow_schema, value_at};
use crate::{Change, Error, Result, Row, Value};

mod columns;
mod csv;
mod parquet;

/// Rows to write to a table, stored column by column as they are pushed.
///
/// Get one from [`Table::batch`](crate::Table::batch), so it knows the
/// table's columns and can check each row as it arrives.
pub struct RowBatch {
    builders: Builders,
    /// Rows read from files, already in the table's columns.
    chunks: Vec<RecordBatch>,
}

impl RowBatch {
    pub(crate) fn new(columns: Arc<[Column]>) -> Self {
        Self { builders: Builders::new(columns), chunks: Vec::new() }
    }

    /// Add every row of a file in the lake. The format comes from the
    /// extension: `.csv` or `.parquet`. Columns are matched by name,
    /// ignoring case and order; missing nullable columns are null and empty
    /// CSV fields are null. Parquet types are converted where they fit
    /// (`int32` into `long`, ...). On error the batch is unchanged.
    pub async fn push_file(&mut self, file: &LakeFile) -> Result<()> {
        let chunks = match file.format()? {
            Format::Csv => csv::read(file, &self.builders.columns).await?,
            Format::Parquet => parquet::read(file, &self.builders.columns).await?,
        };
        self.chunks.extend(chunks);
        Ok(())
    }

    /// Add a row. Columns left out are null. On error the batch is unchanged.
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.builders.check(row, true)?;
        self.builders.append_checked();
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.builders.len + self.chunks.iter().map(RecordBatch::num_rows).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn finish(self) -> Result<RecordBatch> {
        let schema = Arc::new(arrow_schema(&self.builders.columns));
        let mut batches = self.chunks;
        batches.push(self.builders.finish()?);
        concat_batches(&schema, &batches).map_err(|e| Error::Storage(e.into()))
    }
}

/// Changes to apply to a table, stored column by column as they are pushed.
///
/// Get one from [`Table::change_batch`](crate::Table::change_batch). Each
/// batch is applied in one commit.
pub struct ChangeBatch {
    builders: Builders,
    kinds: Vec<Kind>,
    /// Row-major: whether change `i` set column `c` (`i * columns + c`).
    /// Updates and deletes only carry some columns.
    present: Vec<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Insert,
    Update,
    Delete,
}

impl ChangeBatch {
    pub(crate) fn new(columns: Arc<[Column]>) -> Self {
        Self { builders: Builders::new(columns), kinds: Vec::new(), present: Vec::new() }
    }

    /// Add a change. An insert needs every non-nullable column; an update or
    /// delete only the columns it sets. On error the batch is unchanged.
    pub fn push(&mut self, change: Change) -> Result<()> {
        let (kind, row) = match change {
            Change::Insert(row) => (Kind::Insert, row),
            Change::Update(row) => (Kind::Update, row),
            Change::Delete(row) => (Kind::Delete, row),
        };
        self.builders.check(row, kind == Kind::Insert)?;
        self.present.extend_from_slice(&self.builders.present);
        self.kinds.push(kind);
        self.builders.append_checked();
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.kinds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// The changes in push order, for planning a merge.
    pub(crate) fn into_changes(self) -> Result<Vec<Change>> {
        let columns = self.builders.columns.clone();
        let data = self.builders.finish()?;
        let mut changes = Vec::with_capacity(self.kinds.len());
        for (i, kind) in self.kinds.into_iter().enumerate() {
            let mut row = Row::new();
            for (c, column) in columns.iter().enumerate() {
                if self.present[i * columns.len() + c] {
                    row.set(column.name.clone(), value_at(data.column(c), i, &column.name)?);
                }
            }
            changes.push(match kind {
                Kind::Insert => Change::Insert(row),
                Kind::Update => Change::Update(row),
                Kind::Delete => Change::Delete(row),
            });
        }
        Ok(changes)
    }
}

/// One Arrow builder per column, with the per-row checks both batches share.
struct Builders {
    columns: Arc<[Column]>,
    builders: Vec<ColumnBuilder>,
    len: usize,
    /// Set by [`Builders::check`]: the row's values, coerced, in column order.
    scratch: Vec<Value>,
    /// Set by [`Builders::check`]: which columns the row had.
    present: Vec<bool>,
}

impl Builders {
    fn new(columns: Arc<[Column]>) -> Self {
        let builders = columns.iter().map(|c| ColumnBuilder::new(c.data_type)).collect();
        Self { columns, builders, len: 0, scratch: Vec::new(), present: Vec::new() }
    }

    /// Check `row` against the columns without appending anything. With
    /// `complete`, every non-nullable column must have a value; otherwise only
    /// the columns the row sets are checked.
    fn check(&mut self, mut row: Row, complete: bool) -> Result<()> {
        self.scratch.clear();
        self.present.clear();
        for column in self.columns.iter() {
            let value = row.take(&column.name);
            let present = value.is_some();
            let value = value.unwrap_or(Value::Null);
            if value.is_null() && !column.nullable && (complete || present) {
                return Err(Error::MissingValue(column.name.clone()));
            }
            let found = value.kind();
            let value = coerce(column.data_type, value).ok_or_else(|| Error::TypeMismatch {
                column: column.name.clone(),
                expected: column.data_type.to_string(),
                found: found.to_owned(),
            })?;
            self.scratch.push(value);
            self.present.push(present);
        }
        if let Some((name, _)) = row.columns().next() {
            return Err(Error::ColumnNotFound(name.to_owned()));
        }
        Ok(())
    }

    /// Append the row last [`check`](Builders::check)ed.
    fn append_checked(&mut self) {
        for (builder, value) in self.builders.iter_mut().zip(self.scratch.drain(..)) {
            builder.append(value);
        }
        self.len += 1;
    }

    fn finish(self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self.builders.into_iter().map(ColumnBuilder::finish).collect();
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
