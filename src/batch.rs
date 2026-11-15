//! Columnar batches for writing: [`RowBatch`] and [`ChangeBatch`].
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use delta_kernel::arrow::compute::concat_batches;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use crate::delta::{DeltaTable, StagedFile};
use crate::file::{Format, LakeFile};
use crate::lake::{Column, DataType, arrow_schema, value_at};
use crate::{Change, Error, Result, Row, Value};

mod columns;
mod csv;
mod parquet;

/// Batches read from a file, in the table's columns.
type Batches = BoxStream<'static, Result<RecordBatch>>;

/// Rows to write to a table, stored column by column as they are pushed.
///
/// Get one from [`Table::batch`](crate::Table::batch), so it knows the
/// table's columns and can check each row as it arrives.
pub struct RowBatch {
    builders: Builders,
    /// Rows read from files, already in the table's columns.
    chunks: Vec<RecordBatch>,
    /// Where full data files go while files are pushed; `None` keeps
    /// everything in memory.
    sink: Option<Sink>,
    /// Data files already written, waiting for the commit.
    staged: Vec<StagedFile>,
    staged_rows: usize,
}

/// Writes data files for a table, uncommitted, `rows_per_file` at a time.
struct Sink {
    table: DeltaTable,
    rows_per_file: usize,
}

impl RowBatch {
    /// A batch kept wholly in memory.
    pub(crate) fn new(columns: Arc<[Column]>) -> Self {
        Self {
            builders: Builders::new(columns),
            chunks: Vec::new(),
            sink: None,
            staged: Vec::new(),
            staged_rows: 0,
        }
    }

    /// A batch that writes out a data file every `rows_per_file` file rows.
    pub(crate) fn for_table(columns: Arc<[Column]>, table: DeltaTable, rows_per_file: usize) -> Self {
        Self { sink: Some(Sink { table, rows_per_file }), ..Self::new(columns) }
    }

    /// Add every row of a file in the lake. The format comes from the
    /// extension: `.csv` or `.parquet`. Columns are matched by name,
    /// ignoring case and order; missing nullable columns are null and empty
    /// CSV fields are null. Parquet types are converted where they fit
    /// (`int32` into `long`, ...).
    ///
    /// The file is read as a stream. Every
    /// [`rows_per_file`](crate::TableConfig::rows_per_file) rows are written
    /// out as a data file right away, invisible until
    /// [`Table::write`](crate::Table::write) commits them. On error the
    /// batch is unchanged (data files already written stay uncommitted).
    pub async fn push_file(&mut self, file: &LakeFile) -> Result<()> {
        let columns = self.builders.columns.clone();
        let mut batches = match file.format()? {
            Format::Csv => csv::read(file, &columns).await?,
            Format::Parquet => parquet::read(file, &columns).await?,
        };
        // This file's rows, kept apart until the whole file is read.
        let (mut chunks, mut staged) = (Vec::new(), Vec::new());
        while let Some(batch) = batches.next().await {
            chunks.push(batch?);
            if let Some(sink) = &self.sink {
                sink.stage_full(&mut chunks, &mut staged).await?;
            }
        }
        self.staged_rows += staged.iter().map(|(_, rows)| rows).sum::<usize>();
        self.staged.extend(staged.into_iter().map(|(file, _)| file));
        self.chunks.extend(chunks);

        // Rows left over from several files can fill a data file too.
        if let Some(sink) = &self.sink {
            let mut staged = Vec::new();
            let result = sink.stage_full(&mut self.chunks, &mut staged).await;
            self.staged_rows += staged.iter().map(|(_, rows)| rows).sum::<usize>();
            self.staged.extend(staged.into_iter().map(|(file, _)| file));
            result?;
        }
        Ok(())
    }

    /// Add a row. Columns left out are null. On error the batch is unchanged.
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.builders.check(row, true)?;
        self.builders.append_checked();
        Ok(())
    }

    /// Rows in the batch, including those already written out.
    pub fn len(&self) -> usize {
        self.staged_rows + self.in_memory()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn in_memory(&self) -> usize {
        self.builders.len + rows(&self.chunks)
    }

    /// The rows still in memory, as one batch.
    pub(crate) fn finish(self) -> Result<RecordBatch> {
        let schema = Arc::new(arrow_schema(&self.builders.columns));
        let mut batches = self.chunks;
        batches.push(self.builders.finish()?);
        concat_batches(&schema, &batches).map_err(|e| Error::Storage(e.into()))
    }

    /// The data files already written, and the rows still in memory, if any.
    pub(crate) fn finish_staged(mut self) -> Result<(Vec<StagedFile>, Option<RecordBatch>)> {
        let staged = std::mem::take(&mut self.staged);
        let rest = if self.in_memory() > 0 { Some(self.finish()?) } else { None };
        Ok((staged, rest))
    }
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

impl Sink {
    /// While `chunks` hold a full file's rows, write the first
    /// `rows_per_file` of them as a data file and remove them. Each written
    /// file goes to `staged` with its row count.
    async fn stage_full(
        &self,
        chunks: &mut Vec<RecordBatch>,
        staged: &mut Vec<(StagedFile, usize)>,
    ) -> Result<()> {
        while rows(chunks) >= self.rows_per_file {
            let (part, rest) = split_rows(chunks, self.rows_per_file);
            let table = self.table.clone();
            let file = tokio::task::spawn_blocking(move || -> Result<StagedFile> {
                let batch = concat_batches(&part[0].schema(), &part)
                    .map_err(|e| Error::Storage(e.into()))?;
                Ok(table.stage(batch)?)
            })
            .await
            .map_err(|e| Error::Storage(e.into()))??;
            staged.push((file, self.rows_per_file));
            *chunks = rest;
        }
        Ok(())
    }
}

/// The first `n` rows of `batches`, and the rest. Slicing copies no data.
pub(crate) fn split_rows(batches: &[RecordBatch], n: usize) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let (mut head, mut tail) = (Vec::new(), Vec::new());
    let mut left = n;
    for batch in batches {
        if left >= batch.num_rows() {
            left -= batch.num_rows();
            head.push(batch.clone());
        } else if left > 0 {
            head.push(batch.slice(0, left));
            tail.push(batch.slice(left, batch.num_rows() - left));
            left = 0;
        } else {
            tail.push(batch.clone());
        }
    }
    (head, tail)
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
