//! Columnar batches for writing: [`RowBatch`] and [`ChangeBatch`].
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};

use crate::changes::check_key;
use crate::file::{Format, LakeFile};
use crate::lake::Schema;
use crate::lake::{Column, DataType, arrow_schema, batch_rows, value_at};
use crate::{Change, ChangesConfig, Error, Result, Row, Value};
use columns::complete;

mod columns;
mod csv;
mod parquet;

/// Batches of rows in the table's columns.
pub(crate) type Batches = BoxStream<'static, Result<RecordBatch>>;

/// Rows to write to a table: rows pushed one by one, stored column by
/// column, and files to read them from.
///
/// Built from the table's [`Schema`], so it can check each row as it
/// arrives. Building a batch does no I/O: pushed files are read only when the rows are needed, by
/// [`Table::write`](crate::Table::write).
pub struct RowBatch {
    builders: Builders,
    /// Pushed files, read lazily by [`RowBatch::into_stream`].
    files: Vec<LakeFile>,
}

impl RowBatch {
    /// An empty batch for a table with `schema`, from
    /// [`Table::schema`](crate::Table::schema). Fill it with
    /// [`RowBatch::push`] or [`RowBatch::push_file`], then hand it to
    /// [`Table::write`](crate::Table::write).
    pub fn new(schema: Schema) -> Self {
        Self {
            builders: Builders::new(schema.columns().clone()),
            files: Vec::new(),
        }
    }

    pub(crate) fn schema(&self) -> Schema {
        Schema::new(self.builders.columns.clone())
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
pub(crate) fn split_rows(
    batches: &[RecordBatch],
    n: usize,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
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

/// Keyed changes to apply to a table in one commit: changes pushed one by
/// one, stored column by column, and change files (CDC feeds) to read them
/// from, applied in push order.
///
/// Building a batch does no I/O: pushed files are read while the batch is
/// applied by [`Table::apply`](crate::Table::apply).
pub struct ChangesBatch {
    config: ChangesConfig,
    parts: Vec<Part>,
    /// Changes pushed with [`ChangesBatch::push`].
    len: usize,
}

enum Part {
    Pushed(Pushed),
    File(LakeFile),
}

/// A run of changes pushed one by one.
struct Pushed {
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
        let is =
            |full: &str| name.eq_ignore_ascii_case(full) || name.eq_ignore_ascii_case(&full[..1]);
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

impl ChangesBatch {
    pub fn new(config: ChangesConfig) -> Self {
        Self {
            config,
            parts: Vec::new(),
            len: 0,
        }
    }

    /// Add a change. Every change needs the key; an insert needs every
    /// non-nullable column, an update or delete only the columns it sets.
    /// On error the batch is unchanged.
    pub fn push(&mut self, change: Change) -> Result<()> {
        let key = self.config.key_column()?;
        let (kind, row) = match change {
            Change::Insert(row) => (Kind::Insert, row),
            Change::Update(row) => (Kind::Update, row),
            Change::Delete(row) => (Kind::Delete, row),
        };
        check_key(&row, key)?;
        if !matches!(self.parts.last(), Some(Part::Pushed(_))) {
            let columns = self.config.schema().columns().clone();
            self.parts.push(Part::Pushed(Pushed {
                builders: Builders::new(columns),
                kinds: Vec::new(),
                present: Vec::new(),
            }));
        }
        let Some(Part::Pushed(pushed)) = self.parts.last_mut() else {
            unreachable!()
        };
        if let Err(e) = pushed.builders.check(row, kind == Kind::Insert) {
            if pushed.kinds.is_empty() {
                self.parts.pop();
            }
            return Err(e);
        }
        pushed.present.extend_from_slice(&pushed.builders.present);
        pushed.kinds.push(kind);
        pushed.builders.append_checked();
        self.len += 1;
        Ok(())
    }

    /// Add every change in a file in the lake (a change data capture feed).
    /// The format comes from the extension: `.csv` or `.parquet`. The column
    /// named by [`ChangesConfig::operation`] says what each row does:
    /// `insert`, `update` or `delete`, or `I`, `U`, `D`, ignoring case. The
    /// other columns are matched by name, ignoring case and order, like
    /// [`RowBatch::push_file`].
    ///
    /// An insert needs every non-nullable column. An update sets the columns
    /// the file has (an empty CSV field sets null); the others keep their
    /// values. A delete only needs the key.
    ///
    /// Nothing is read now: the file is streamed while the batch is applied,
    /// so a missing or invalid file fails [`Table::apply`](crate::Table::apply).
    pub fn push_file(&mut self, file: &LakeFile) {
        self.parts.push(Part::File(file.clone()));
    }

    /// Changes added with [`ChangesBatch::push`]. Rows in pushed files are
    /// not counted: they aren't read until the batch is applied.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether nothing was pushed.
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    pub(crate) fn config(&self) -> &ChangesConfig {
        &self.config
    }

    /// The changes in push order, a chunk at a time: each pushed file is read
    /// as it is pulled. Fails now, before anything is read, if the config
    /// can't apply the batch.
    pub(crate) fn into_stream(self) -> Result<ChangeStream> {
        let key = self.config.key_column()?.to_owned();
        let columns = self.config.schema().columns().clone();
        let has_files = self.parts.iter().any(|p| matches!(p, Part::File(_)));
        let files = match (has_files, self.config.operation_column()) {
            (false, _) => None,
            (true, None) => {
                return Err(Error::InvalidConfig(
                    "change files need an operation column (ChangesConfig::operation)".into(),
                ));
            }
            (true, Some(operation)) => Some((
                change_file_columns(&columns, operation)?,
                operation.to_owned(),
            )),
        };
        let parts = self.parts.into_iter().map(move |part| match part {
            Part::Pushed(pushed) => stream::once(async move { pushed.into_changes() }).boxed(),
            Part::File(file) => {
                let (read, operation) = files.clone().expect("checked above");
                let reader = FileChanges::new(file, columns.clone(), key.clone(), operation);
                reader.into_stream(read)
            }
        });
        Ok(stream::iter(parts).flatten().boxed())
    }
}

impl Pushed {
    /// The changes in push order, for planning a merge.
    fn into_changes(self) -> Result<Vec<Change>> {
        let columns = self.builders.columns.clone();
        let data = self.builders.finish()?;
        let mut changes = Vec::with_capacity(self.kinds.len());
        for (i, kind) in self.kinds.into_iter().enumerate() {
            let mut row = Row::new();
            for (c, column) in columns.iter().enumerate() {
                if self.present[i * columns.len() + c] {
                    row.set(
                        column.name.clone(),
                        value_at(data.column(c), i, &column.name)?,
                    );
                }
            }
            changes.push(kind.change(row));
        }
        Ok(changes)
    }
}

/// The columns to read a change file with: every table column, nullable
/// (a delete only carries the key), then the operation column.
fn change_file_columns(columns: &[Column], operation: &str) -> Result<Arc<[Column]>> {
    if columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(operation))
    {
        return Err(Error::InvalidConfig(format!(
            "operation column {operation:?} is also a table column"
        )));
    }
    let mut read: Vec<Column> = columns
        .iter()
        .map(|c| Column {
            nullable: true,
            ..c.clone()
        })
        .collect();
    read.push(Column {
        name: operation.to_owned(),
        data_type: DataType::String,
        nullable: false,
    });
    Ok(read.into())
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
        Self {
            file,
            builders: Builders::new(columns),
            key,
            operation,
            rows: 0,
        }
    }

    /// The file's changes, read with the `read` columns as they are pulled.
    fn into_stream(mut self, read: Arc<[Column]>) -> ChangeStream {
        stream::once(async move {
            let batches = read_file(&self.file, &read).await?;
            Ok::<_, Error>(batches.map(move |batch| self.changes(&batch?)))
        })
        .try_flatten()
        .boxed()
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
        let kind = Kind::parse(&operation).ok_or_else(|| {
            format!("unknown operation {operation:?} (expected insert, update or delete)")
        })?;
        check_key(&row, &self.key).map_err(|e| e.to_string())?;
        if kind == Kind::Delete {
            let key = row.take(&self.key).unwrap_or(Value::Null);
            row = Row::new().with(self.key.clone(), key);
        }
        self.builders
            .check(row.clone(), kind == Kind::Insert)
            .map_err(|e| e.to_string())?;
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
        let builders = columns
            .iter()
            .map(|c| ColumnBuilder::new(c.data_type))
            .collect();
        Self {
            columns,
            builders,
            len: 0,
            scratch: Vec::new(),
            present: Vec::new(),
        }
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
