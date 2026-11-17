//! Columnar batches for writing: [`RowBatch`] and [`ChangeBatch`].
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};

use crate::file::{Format, LakeFile};
use crate::lake::{Column, DataType, arrow_schema, batch_rows, value_at};
use columns::complete;
use crate::{Change, Error, Result, Row, Value};

mod columns;
mod csv;
mod parquet;

/// Batches of rows in the table's columns.
pub(crate) type Batches = BoxStream<'static, Result<RecordBatch>>;

/// Rows to write to a table: rows pushed one by one, stored column by
/// column, and files to read them from.
///
/// Get one from [`Table::batch`](crate::Table::batch), so it knows the
/// table's columns and can check each row as it arrives. Building a batch
/// does no I/O: pushed files are read only when the rows are needed, by
/// [`Table::write`](crate::Table::write).
pub struct RowBatch {
    builders: Builders,
    /// Pushed files, read lazily by [`RowBatch::into_stream`].
    files: Vec<LakeFile>,
}

impl RowBatch {
    pub(crate) fn new(columns: Arc<[Column]>) -> Self {
        Self { builders: Builders::new(columns), files: Vec::new() }
    }

    /// Add every row of a file in the lake. The format comes from the
    /// extension: `.csv` or `.parquet`. Columns are matched by name,
    /// ignoring case and order; missing nullable columns are null and empty
    /// CSV fields are null. Parquet types are converted where they fit
    /// (`int32` into `long`, ...).
    ///
    /// Nothing is read now: the file is streamed when the batch is written,
    /// so a missing or invalid file fails [`Table::write`](crate::Table::write).
    pub fn push_file(&mut self, file: &LakeFile) {
        self.files.push(file.clone());
    }

    /// Add a row. Columns left out are null. On error the batch is unchanged.
    pub fn push(&mut self, row: Row) -> Result<()> {
        self.builders.check(row, true)?;
        self.builders.append_checked();
        Ok(())
    }

    /// Whether nothing was pushed.
    pub fn is_empty(&self) -> bool {
        self.builders.len == 0 && self.files.is_empty()
    }

    /// The pushed rows, as one batch. Pushed files are not read.
    pub(crate) fn finish(self) -> Result<RecordBatch> {
        self.builders.finish()
    }

    /// Every row, in the table's columns: each pushed file in turn, read as
    /// it is pulled, then the pushed rows.
    pub(crate) fn into_stream(self) -> Batches {
        let columns = self.builders.columns.clone();
        let files = stream::iter(self.files)
            .then(move |file| {
                let columns = columns.clone();
                async move {
                    let batches = read_file(&file, &columns).await?;
                    Ok::<_, Error>(batches.map(move |batch| complete(&columns, &batch?)))
                }
            })
            .try_flatten();
        let rows = (self.builders.len > 0).then(|| self.builders.finish());
        files.chain(stream::iter(rows)).boxed()
    }
}

/// Read `file` as a stream of batches holding the table `columns` it has.
async fn read_file(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Batches> {
    match file.format()? {
        Format::Csv => csv::read(file, columns).await,
        Format::Parquet => parquet::read(file, columns).await,
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
/// Get one from [`Table::change_batch`](crate::Table::change_batch) and add
/// it to [`Changes`](crate::Changes). Each batch is applied in one commit.
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

impl Kind {
    /// The operation named in a change file: `insert`, `update`, `delete`
    /// or their initials, ignoring case.
    fn parse(name: &str) -> Option<Self> {
        let name = name.trim();
        let is = |full: &str| name.eq_ignore_ascii_case(full) || name.eq_ignore_ascii_case(&full[..1]);
        if is("insert") {
            Some(Kind::Insert)
        } else if is("update") {
            Some(Kind::Update)
        } else if is("delete") {
            Some(Kind::Delete)
        } else {
            None
        }
    }

    fn change(self, row: Row) -> Change {
        match self {
            Kind::Insert => Change::Insert(row),
            Kind::Update => Change::Update(row),
            Kind::Delete => Change::Delete(row),
        }
    }
}

/// Changes, a chunk at a time.
pub(crate) type ChangeStream = BoxStream<'static, Result<Vec<Change>>>;

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
            changes.push(kind.change(row));
        }
        Ok(changes)
    }
}

/// The columns to read a change file with: every table column, nullable
/// (a delete only carries the key), then the operation column.
pub(crate) fn change_file_columns(columns: &[Column], operation: &str) -> Result<Arc<[Column]>> {
    if columns.iter().any(|c| c.name.eq_ignore_ascii_case(operation)) {
        return Err(Error::InvalidConfig(format!(
            "operation column {operation:?} is also a table column"
        )));
    }
    let mut read: Vec<Column> = columns.iter().map(|c| Column { nullable: true, ..c.clone() }).collect();
    read.push(Column { name: operation.to_owned(), data_type: DataType::String, nullable: false });
    Ok(read.into())
}

/// The changes in a change file, read as they are pulled. `read` comes from
/// [`change_file_columns`] for the table `columns`.
pub(crate) fn change_file(
    file: LakeFile,
    columns: Arc<[Column]>,
    read: Arc<[Column]>,
    key: String,
    operation: String,
) -> ChangeStream {
    stream::once(async move {
        let batches = read_file(&file, &read).await?;
        let mut reader = FileChanges::new(file, columns, key, operation);
        Ok::<_, Error>(batches.map(move |batch| reader.changes(&batch?)))
    })
    .try_flatten()
    .boxed()
}

/// Turns batches read from a change file into checked changes.
struct FileChanges {
    file: LakeFile,
    /// Checks each change like [`ChangeBatch::push`]; nothing is appended.
    builders: Builders,
    key: String,
    operation: String,
    /// Rows read so far, for error messages.
    rows: usize,
}

impl FileChanges {
    fn new(file: LakeFile, columns: Arc<[Column]>, key: String, operation: String) -> Self {
        Self { file, builders: Builders::new(columns), key, operation, rows: 0 }
    }

    fn changes(&mut self, batch: &RecordBatch) -> Result<Vec<Change>> {
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in batch_rows(batch)? {
            self.rows += 1;
            let change = self.change(row).map_err(|message| Error::InvalidFile {
                file: self.file.path().to_owned(),
                message: format!("row {}: {message}", self.rows),
            })?;
            changes.push(change);
        }
        Ok(changes)
    }

    fn change(&mut self, mut row: Row) -> Result<Change, String> {
        let operation = match row.take(&self.operation) {
            Some(Value::String(name)) => name,
            _ => unreachable!("the operation column is required and a string"),
        };
        let kind = Kind::parse(&operation)
            .ok_or_else(|| format!("unknown operation {operation:?} (expected insert, update or delete)"))?;
        if kind == Kind::Delete {
            let key = row.take(&self.key).unwrap_or(Value::Null);
            row = Row::new().with(self.key.clone(), key);
        }
        self.builders.check(row.clone(), kind == Kind::Insert).map_err(|e| e.to_string())?;
        Ok(kind.change(row))
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
