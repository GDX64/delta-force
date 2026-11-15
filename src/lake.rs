//! The public API: [`DeltaForce`], [`TableConfig`], [`Table`] and [`DataType`].
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use futures_util::StreamExt;
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use url::Url;

use crate::batch::{ChangeBatch, RowBatch};
use crate::changes::plan;
use crate::delta::{DeltaTable, MergePlan};
use crate::Changes;
use crate::engine::ObjectStoreEngine;
use crate::{Error, Result, Row, Rows, Value};

/// How a column is stored in the table (Delta's own types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataType {
    Boolean,
    Integer,
    Long,
    Double,
    String,
}

impl DataType {
    fn to_arrow(self) -> ArrowType {
        match self {
            DataType::Boolean => ArrowType::Boolean,
            DataType::Integer => ArrowType::Int32,
            DataType::Long => ArrowType::Int64,
            DataType::Double => ArrowType::Float64,
            DataType::String => ArrowType::Utf8,
        }
    }

    fn from_arrow(t: &ArrowType) -> Option<Self> {
        Some(match t {
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int32 => DataType::Integer,
            ArrowType::Int64 => DataType::Long,
            ArrowType::Float64 => DataType::Double,
            ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => DataType::String,
            _ => return None,
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DataType::Boolean => "boolean",
            DataType::Integer => "integer",
            DataType::Long => "long",
            DataType::Double => "double",
            DataType::String => "string",
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// The definition of a new table: its name and columns.
#[derive(Debug, Clone)]
pub struct TableConfig {
    name: String,
    columns: Vec<Column>,
}

impl TableConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), columns: Vec::new() }
    }

    /// A column that must always have a value.
    pub fn column(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.add(name.into(), data_type, false)
    }

    /// A column that may be null.
    pub fn nullable(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.add(name.into(), data_type, true)
    }

    fn add(mut self, name: String, data_type: DataType, nullable: bool) -> Self {
        self.columns.push(Column { name, data_type, nullable });
        self
    }
}

/// A collection of Delta tables under one storage root.
#[derive(Clone)]
pub struct DeltaForce {
    engine: Arc<ObjectStoreEngine>,
    root: Url,
}

impl DeltaForce {
    /// Use the local directory `path` as the root, creating it if missing.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = std::path::absolute(path.as_ref())?;
        let root = Url::from_directory_path(&path)
            .map_err(|()| Error::Storage(format!("invalid path {}", path.display()).into()))?;
        blocking(move || Ok(std::fs::create_dir_all(&path)?)).await?;
        Ok(Self::with_store(Arc::new(LocalFileSystem::new()), root))
    }

    /// A root that lives only in memory. Handy for tests.
    pub fn in_memory() -> Self {
        let root = Url::parse("memory:///").expect("valid url");
        Self::with_store(Arc::new(InMemory::new()), root)
    }

    fn with_store(store: Arc<dyn ObjectStore>, root: Url) -> Self {
        Self { engine: Arc::new(ObjectStoreEngine::new(store)), root }
    }

    /// Create a new table. Fails with [`Error::TableExists`] if it exists.
    pub async fn create(&self, config: TableConfig) -> Result<Table> {
        validate_columns(&config.columns)?;
        let table = self.delta_table(&config.name)?;
        let schema = arrow_schema(&config.columns);
        let columns: Arc<[Column]> = config.columns.into();
        let name = config.name.clone();
        let inner = blocking(move || {
            if table.version()?.is_some() {
                return Err(Error::TableExists(name));
            }
            table.create(&schema)?;
            Ok(table)
        })
        .await?;
        Ok(Table { name: config.name, inner, columns })
    }

    /// Open an existing table. Fails with [`Error::TableNotFound`] if missing.
    pub async fn table(&self, name: &str) -> Result<Table> {
        let inner = self.delta_table(name)?;
        let (probe, owned) = (inner.clone(), name.to_owned());
        let columns = blocking(move || match probe.version()? {
            Some(_) => table_columns(&probe.arrow_schema()?),
            None => Err(Error::TableNotFound(owned)),
        })
        .await?;
        Ok(Table { name: name.to_owned(), inner, columns: columns.into() })
    }

    fn delta_table(&self, name: &str) -> Result<DeltaTable> {
        let valid = !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !valid {
            return Err(Error::InvalidTableName(name.to_owned()));
        }
        let url = self
            .root
            .join(&format!("{name}/"))
            .map_err(|e| Error::Storage(e.into()))?;
        Ok(DeltaTable::new(self.engine.clone(), url))
    }
}

/// A handle to one table.
#[derive(Clone)]
pub struct Table {
    name: String,
    inner: DeltaTable,
    /// The schema as of when the table was opened or created.
    columns: Arc<[Column]>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The latest committed version.
    pub async fn version(&self) -> Result<u64> {
        let (inner, name) = (self.inner.clone(), self.name.clone());
        blocking(move || inner.version()?.ok_or(Error::TableNotFound(name))).await
    }

    /// An empty batch for this table's columns. Fill it with
    /// [`RowBatch::push`], then hand it to [`Table::write`].
    pub fn batch(&self) -> RowBatch {
        RowBatch::new(self.columns.clone())
    }

    /// Append a batch in one commit. Returns the new version.
    pub async fn write(&self, batch: RowBatch) -> Result<u64> {
        if batch.is_empty() {
            return Err(Error::NoRows);
        }
        let batch = batch.finish()?;
        let inner = self.inner.clone();
        blocking(move || Ok(inner.write(batch)?)).await
    }

    /// An empty change batch for this table's columns. Fill it with
    /// [`ChangeBatch::push`], then send it through [`Changes`].
    pub fn change_batch(&self) -> ChangeBatch {
        ChangeBatch::new(self.columns.clone())
    }

    /// Read `changes` as a stream and apply them by key, one commit per
    /// batch. Resolves when the stream ends. On error, earlier batches stay
    /// committed.
    ///
    /// Merge-on-read: replaced and deleted rows are marked in deletion
    /// vectors and new values are appended; existing files aren't rewritten.
    pub async fn apply(&self, changes: Changes) -> Result<Applied> {
        let key = changes.config().key_column()?.to_owned();
        let mut stream = changes.into_stream();
        let mut applied = Applied::default();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.is_empty() {
                continue;
            }
            let (inner, key) = (self.inner.clone(), key.clone());
            let count = batch.len();
            blocking(move || {
                let batch = batch.into_changes()?;
                let columns = table_columns(&inner.arrow_schema()?)?;
                if !columns.iter().any(|c| c.name == key) {
                    return Err(Error::ColumnNotFound(key));
                }
                inner.merge(|files| {
                    let mut existing = Vec::new();
                    for (index, file) in files.iter().enumerate() {
                        let mut positions = file.positions.iter();
                        for batch in &file.data {
                            for row in batch_rows(batch)? {
                                let position = *positions.next().expect("one position per row");
                                existing.push(((index, position), row));
                            }
                        }
                    }
                    let plan = plan(existing, &batch, &key)?;
                    let mut merge = MergePlan::default();
                    for (index, position) in plan.delete {
                        merge.deletes.entry(files[index].path.clone()).or_default().push(position);
                    }
                    if !plan.append.is_empty() {
                        let mut rows = RowBatch::new(columns.into());
                        for row in plan.append {
                            rows.push(row)?;
                        }
                        merge.append = Some(rows.finish()?);
                    }
                    Ok::<_, Error>(merge)
                })?;
                Ok(())
            })
            .await?;
            applied.changes += count;
            applied.commits += 1;
        }
        Ok(applied)
    }

    /// All rows at the latest version.
    pub async fn rows(&self) -> Result<Rows> {
        let inner = self.inner.clone();
        blocking(move || {
            let mut rows = Vec::new();
            for batch in inner.read()? {
                rows.extend(batch_rows(&batch)?);
            }
            Ok(Rows::from(rows))
        })
        .await
    }
}

/// What [`Table::apply`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    /// Changes applied.
    pub changes: usize,
    /// Commits made (one per non-empty batch).
    pub commits: usize,
}

/// The kernel is synchronous; run it off the async worker threads.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Storage(e.into()))?
}

fn validate_columns(columns: &[Column]) -> Result<()> {
    if columns.is_empty() {
        return Err(Error::InvalidSchema("a table needs at least one column".into()));
    }
    for (i, c) in columns.iter().enumerate() {
        if c.name.is_empty() {
            return Err(Error::InvalidSchema("column names cannot be empty".into()));
        }
        if columns[..i].iter().any(|p| p.name == c.name) {
            return Err(Error::InvalidSchema(format!("duplicate column {:?}", c.name)));
        }
    }
    Ok(())
}

pub(crate) fn arrow_schema(columns: &[Column]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.to_arrow(), c.nullable))
            .collect::<Vec<_>>(),
    )
}

fn table_columns(schema: &Schema) -> Result<Vec<Column>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let data_type = DataType::from_arrow(f.data_type()).ok_or_else(|| {
                Error::InvalidSchema(format!("unsupported type {} for {:?}", f.data_type(), f.name()))
            })?;
            Ok(Column { name: f.name().clone(), data_type, nullable: f.is_nullable() })
        })
        .collect()
}

// ------------------------------------------------------------ arrow -> rows --

fn batch_rows(batch: &RecordBatch) -> Result<Vec<Row>> {
    let schema = batch.schema();
    let mut rows = vec![Row::new(); batch.num_rows()];
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        for (i, row) in rows.iter_mut().enumerate() {
            row.set(field.name().clone(), value_at(array.as_ref(), i, field.name())?);
        }
    }
    Ok(rows)
}

pub(crate) fn value_at(array: &dyn Array, i: usize, column: &str) -> Result<Value> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        ArrowType::Boolean => Value::Boolean(array.as_boolean().value(i)),
        ArrowType::Int32 => Value::Integer(array.as_primitive::<Int32Type>().value(i)),
        ArrowType::Int64 => Value::Long(array.as_primitive::<Int64Type>().value(i)),
        ArrowType::Float64 => Value::Double(array.as_primitive::<Float64Type>().value(i)),
        ArrowType::Utf8 => Value::String(array.as_string::<i32>().value(i).to_owned()),
        ArrowType::LargeUtf8 => Value::String(array.as_string::<i64>().value(i).to_owned()),
        ArrowType::Utf8View => Value::String(array.as_string_view().value(i).to_owned()),
        other => {
            return Err(Error::InvalidSchema(format!("unsupported type {other} for {column:?}")));
        }
    })
}
