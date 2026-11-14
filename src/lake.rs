//! The public API: [`DeltaForce`], [`TableConfig`], [`Table`] and [`DataType`].
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use url::Url;

use crate::delta::DeltaTable;
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
struct Column {
    name: String,
    data_type: DataType,
    nullable: bool,
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
        let name = config.name.clone();
        let inner = blocking(move || {
            if table.version()?.is_some() {
                return Err(Error::TableExists(name));
            }
            table.create(&schema)?;
            Ok(table)
        })
        .await?;
        Ok(Table { name: config.name, inner })
    }

    /// Open an existing table. Fails with [`Error::TableNotFound`] if missing.
    pub async fn table(&self, name: &str) -> Result<Table> {
        let inner = self.delta_table(name)?;
        let (probe, owned) = (inner.clone(), name.to_owned());
        blocking(move || match probe.version()? {
            Some(_) => Ok(()),
            None => Err(Error::TableNotFound(owned)),
        })
        .await?;
        Ok(Table { name: name.to_owned(), inner })
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

    /// Append rows in one commit. Returns the new version.
    pub async fn write(&self, rows: impl IntoIterator<Item = Row>) -> Result<u64> {
        let rows: Vec<Row> = rows.into_iter().collect();
        if rows.is_empty() {
            return Err(Error::NoRows);
        }
        let inner = self.inner.clone();
        blocking(move || {
            let columns = table_columns(&inner.arrow_schema()?)?;
            let batch = to_batch(&columns, &rows)?;
            Ok(inner.write(batch)?)
        })
        .await
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

fn arrow_schema(columns: &[Column]) -> Schema {
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

// ------------------------------------------------------------ rows -> arrow --

fn to_batch(columns: &[Column], rows: &[Row]) -> Result<RecordBatch> {
    for row in rows {
        if let Some((name, _)) = row.columns().find(|(n, _)| !columns.iter().any(|c| c.name == *n)) {
            return Err(Error::ColumnNotFound(name.to_owned()));
        }
    }
    let null = Value::Null;
    let arrays = columns
        .iter()
        .map(|column| {
            let mut builder = ColumnBuilder::new(column.data_type, rows.len());
            for row in rows {
                let value = row.value(&column.name).unwrap_or(&null);
                if value.is_null() && !column.nullable {
                    return Err(Error::MissingValue(column.name.clone()));
                }
                if !builder.append(value) {
                    return Err(Error::TypeMismatch {
                        column: column.name.clone(),
                        expected: column.data_type.to_string(),
                        found: value.kind().to_owned(),
                    });
                }
            }
            Ok(builder.finish())
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(Arc::new(arrow_schema(columns)), arrays)
        .map_err(|e| Error::Storage(e.into()))
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Integer(Int32Builder),
    Long(Int64Builder),
    Double(Float64Builder),
    String(StringBuilder),
}

impl ColumnBuilder {
    fn new(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Integer => Self::Integer(Int32Builder::with_capacity(capacity)),
            DataType::Long => Self::Long(Int64Builder::with_capacity(capacity)),
            DataType::Double => Self::Double(Float64Builder::with_capacity(capacity)),
            DataType::String => Self::String(StringBuilder::with_capacity(capacity, capacity * 8)),
        }
    }

    /// Append `value`, converting where lossless. `false` if it doesn't fit.
    fn append(&mut self, value: &Value) -> bool {
        /// Largest integer a double holds exactly.
        const MAX_EXACT: i64 = 1 << 53;
        match (self, value) {
            (Self::Boolean(b), Value::Null) => b.append_null(),
            (Self::Integer(b), Value::Null) => b.append_null(),
            (Self::Long(b), Value::Null) => b.append_null(),
            (Self::Double(b), Value::Null) => b.append_null(),
            (Self::String(b), Value::Null) => b.append_null(),
            (Self::Boolean(b), Value::Boolean(v)) => b.append_value(*v),
            (Self::Integer(b), v) if v.as_i64().is_some() => match i32::try_from(v.as_i64().unwrap()) {
                Ok(v) => b.append_value(v),
                Err(_) => return false,
            },
            (Self::Long(b), v) if v.as_i64().is_some() => b.append_value(v.as_i64().unwrap()),
            (Self::Double(b), Value::Double(v)) => b.append_value(*v),
            (Self::Double(b), v) if v.as_i64().is_some_and(|i| i.abs() <= MAX_EXACT) => {
                b.append_value(v.as_i64().unwrap() as f64)
            }
            (Self::String(b), Value::String(v)) => b.append_value(v),
            _ => return false,
        }
        true
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

fn value_at(array: &dyn Array, i: usize, column: &str) -> Result<Value> {
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
