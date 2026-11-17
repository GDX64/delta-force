//! The public API: [`DeltaForce`], [`TableConfig`], [`Table`] and [`DataType`].
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use delta_kernel::arrow::compute::concat_batches;
use futures_util::StreamExt;
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use url::Url;

use crate::Changes;
use crate::batch::{ChangeBatch, ChangeFiles, RowBatch, split_rows};
use crate::changes::Planner;
use crate::delta::{Append, DeltaTable, MergePlan, Settings};
use crate::engine::ObjectStoreEngine;
use crate::stream::RowStream;
use crate::{Compression, Error, LakeFile, Result, Row, Rows, Value};

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
    pub(crate) fn to_arrow(self) -> ArrowType {
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
    settings: Settings,
}

impl TableConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            settings: Settings::default(),
        }
    }

    /// A column that must always have a value.
    pub fn column(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.add(name.into(), data_type, false)
    }

    /// A column that may be null.
    pub fn nullable(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.add(name.into(), data_type, true)
    }

    /// How data files are compressed. Defaults to [`Compression::Zstd`].
    pub fn compression(mut self, compression: Compression) -> Self {
        self.settings.compression = compression;
        self
    }

    /// The most rows in one data file. While a file is pushed into a
    /// [`RowBatch`], every `rows` rows are written out as a data file, so
    /// memory holds at most this many rows; [`Table::write`] commits them
    /// all together. Defaults to 4,000,000.
    pub fn rows_per_file(mut self, rows: usize) -> Self {
        self.settings.rows_per_file = rows;
        self
    }

    fn add(mut self, name: String, data_type: DataType, nullable: bool) -> Self {
        self.columns.push(Column {
            name,
            data_type,
            nullable,
        });
        self
    }
}

/// A collection of Delta tables under one storage root.
#[derive(Clone)]
pub struct DeltaForce {
    engine: Arc<ObjectStoreEngine>,
    store: Arc<dyn ObjectStore>,
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

    /// A lake whose root is `store`, so it can read virtual files.
    #[cfg(any(test, feature = "test-util"))]
    pub fn with_test_store(store: crate::test_util::TestStore) -> Self {
        let root = Url::parse("test:///").expect("valid url");
        Self::with_store(Arc::new(store), root)
    }

    fn with_store(store: Arc<dyn ObjectStore>, root: Url) -> Self {
        Self {
            engine: Arc::new(ObjectStoreEngine::new(store.clone())),
            store,
            root,
        }
    }

    /// A file in the lake, by its path relative to the lake root (for
    /// example `"landing/events.csv"`). Nothing is read until it is used.
    pub fn file(&self, path: &str) -> LakeFile {
        let root = object_store::path::Path::from_url_path(self.root.path()).unwrap_or_default();
        LakeFile::new(self.store.clone(), &root, path)
    }

    /// Store `contents` as a file in the lake, replacing any existing one.
    pub async fn put_file(&self, path: &str, contents: impl Into<Vec<u8>>) -> Result<LakeFile> {
        let file = self.file(path);
        file.put(contents.into()).await?;
        Ok(file)
    }

    /// Create a new table. Fails with [`Error::TableExists`] if it exists.
    pub async fn create(&self, config: TableConfig) -> Result<Table> {
        validate_columns(&config.columns)?;
        if config.settings.rows_per_file == 0 {
            return Err(Error::InvalidConfig(
                "rows_per_file must be at least 1".into(),
            ));
        }
        let table = self.delta_table(&config.name)?;
        let schema = arrow_schema(&config.columns);
        let columns: Arc<[Column]> = config.columns.into();
        let settings = config.settings;
        let name = config.name.clone();
        let inner = blocking(move || {
            if table.version()?.is_some() {
                return Err(Error::TableExists(name));
            }
            table.create(&schema, settings)?;
            Ok(table)
        })
        .await?;
        Ok(Table {
            name: config.name,
            inner,
            columns,
            settings,
        })
    }

    /// Open an existing table. Fails with [`Error::TableNotFound`] if missing.
    pub async fn table(&self, name: &str) -> Result<Table> {
        let inner = self.delta_table(name)?;
        let (probe, owned) = (inner.clone(), name.to_owned());
        let (columns, settings) = blocking(move || match probe.version()? {
            Some(_) => {
                let settings = Settings {
                    compression: probe.compression()?,
                    rows_per_file: probe.rows_per_file()?,
                };
                Ok((table_columns(&probe.arrow_schema()?)?, settings))
            }
            None => Err(Error::TableNotFound(owned)),
        })
        .await?;
        Ok(Table {
            name: name.to_owned(),
            inner,
            columns: columns.into(),
            settings,
        })
    }

    fn delta_table(&self, name: &str) -> Result<DeltaTable> {
        let valid = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
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
    settings: Settings,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How new data files are compressed.
    pub fn compression(&self) -> Compression {
        self.settings.compression
    }

    /// The most rows in one data file. See [`TableConfig::rows_per_file`].
    pub fn rows_per_file(&self) -> usize {
        self.settings.rows_per_file
    }

    /// The latest committed version.
    pub async fn version(&self) -> Result<u64> {
        let (inner, name) = (self.inner.clone(), self.name.clone());
        blocking(move || inner.version()?.ok_or(Error::TableNotFound(name))).await
    }

    /// An empty batch for this table's columns. Fill it with
    /// [`RowBatch::push`] or [`RowBatch::push_file`], then hand it to
    /// [`Table::write`].
    pub fn batch(&self) -> RowBatch {
        RowBatch::new(self.columns.clone())
    }

    /// Append a batch in one commit. Returns the new version.
    ///
    /// The batch's rows are pulled as a stream and written out as a data
    /// file every [`rows_per_file`](TableConfig::rows_per_file) rows, so
    /// memory holds about one data file. Readers see none of them until the
    /// commit. On error nothing is committed.
    pub async fn write(&self, batch: RowBatch) -> Result<u64> {
        if batch.is_empty() {
            return Err(Error::NoRows);
        }
        let rows_per_file = self.settings.rows_per_file;
        let inner = self.inner.clone();
        let mut append = blocking(move || Ok(inner.append()?)).await?;
        let mut rows = batch.into_stream();
        let (mut pending, mut files) = (Vec::new(), 0);
        while let Some(batch) = rows.next().await {
            pending.push(batch?);
            while row_count(&pending) >= rows_per_file {
                let (file, rest) = split_rows(&pending, rows_per_file);
                pending = rest;
                append = write_file(append, file).await?;
                files += 1;
            }
        }
        if row_count(&pending) > 0 {
            append = write_file(append, pending).await?;
            files += 1;
        }
        if files == 0 {
            return Err(Error::NoRows);
        }
        blocking(move || Ok(append.commit()?)).await
    }

    /// An empty change batch for this table's columns. Fill it with
    /// [`ChangeBatch::push`] or [`ChangeBatch::push_file`], then send it
    /// through [`Changes`].
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
        let operation = changes.config().operation_column().map(str::to_owned);
        let mut stream = changes.into_stream();
        let mut applied = Applied::default();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.is_empty() {
                continue;
            }
            let config = ChangeFiles { key: &key, operation: operation.as_deref() };
            let mut changes = batch.into_stream(config)?;
            let (inner, key) = (self.inner.clone(), key.clone());
            // Change files are pulled from the blocking thread, as the merge
            // plan consumes them.
            let runtime = tokio::runtime::Handle::current();
            let count = blocking(move || {
                let columns = table_columns(&inner.arrow_schema()?)?;
                if !columns.iter().any(|c| c.name == key) {
                    return Err(Error::ColumnNotFound(key));
                }
                let mut count = 0;
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
                    let mut planner = Planner::new(existing, &key)?;
                    while let Some(chunk) = runtime.block_on(changes.next()) {
                        for change in chunk? {
                            planner.apply(change)?;
                            count += 1;
                        }
                    }
                    let plan = planner.finish();
                    let mut merge = MergePlan::default();
                    for (index, position) in plan.delete {
                        merge
                            .deletes
                            .entry(files[index].path.clone())
                            .or_default()
                            .push(position);
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
                Ok(count)
            })
            .await?;
            applied.changes += count;
            applied.commits += 1;
        }
        Ok(applied)
    }

    /// The rows at the latest version, one batch at a time.
    pub fn row_stream(&self) -> RowStream {
        RowStream::new(self.inner.clone())
    }

    /// All rows at the latest version, in memory. For large tables prefer
    /// [`Table::row_stream`].
    pub async fn rows(&self) -> Result<Rows> {
        let mut stream = self.row_stream();
        let mut all = Vec::new();
        while let Some(rows) = stream.next().await? {
            all.extend(rows);
        }
        Ok(Rows::from(all))
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
        return Err(Error::InvalidSchema(
            "a table needs at least one column".into(),
        ));
    }
    for (i, c) in columns.iter().enumerate() {
        if c.name.is_empty() {
            return Err(Error::InvalidSchema("column names cannot be empty".into()));
        }
        if columns[..i].iter().any(|p| p.name == c.name) {
            return Err(Error::InvalidSchema(format!(
                "duplicate column {:?}",
                c.name
            )));
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
                Error::InvalidSchema(format!(
                    "unsupported type {} for {:?}",
                    f.data_type(),
                    f.name()
                ))
            })?;
            Ok(Column {
                name: f.name().clone(),
                data_type,
                nullable: f.is_nullable(),
            })
        })
        .collect()
}

// ------------------------------------------------------------ arrow -> rows --

pub(crate) fn batch_rows(batch: &RecordBatch) -> Result<Vec<Row>> {
    let schema = batch.schema();
    let mut rows = vec![Row::new(); batch.num_rows()];
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        for (i, row) in rows.iter_mut().enumerate() {
            row.set(
                field.name().clone(),
                value_at(array.as_ref(), i, field.name())?,
            );
        }
    }
    Ok(rows)
}

/// Write `rows` as one data file of `append`, uncommitted.
async fn write_file(mut append: Append, rows: Vec<RecordBatch>) -> Result<Append> {
    blocking(move || {
        let batch =
            concat_batches(&rows[0].schema(), &rows).map_err(|e| Error::Storage(e.into()))?;
        append.write_file(batch)?;
        Ok(append)
    })
    .await
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
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
            return Err(Error::InvalidSchema(format!(
                "unsupported type {other} for {column:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pushed_files_are_split_into_data_files_of_rows_per_file() {
        let lake = DeltaForce::in_memory();
        let config = TableConfig::new("t")
            .column("id", DataType::Long)
            .rows_per_file(2);
        let table = lake.create(config).await.unwrap();
        lake.put_file("a.csv", "id\n1\n2\n3\n").await.unwrap();
        lake.put_file("b.csv", "id\n4\n5\n6\n").await.unwrap();

        let mut batch = table.batch();
        batch.push_file(&lake.file("a.csv")); // [1, 2]; 3 left over
        batch.push_file(&lake.file("b.csv")); // [3, 4], [5, 6]
        batch.push(crate::row! { "id" => 7 }).unwrap(); // [7]
        assert_eq!(table.write(batch).await.unwrap(), 1);

        let mut files = table.inner.file_rows().unwrap();
        files.sort();
        assert_eq!(files, [1, 2, 2, 2]);
    }

    #[tokio::test]
    async fn pushed_files_are_read_only_on_write() {
        let lake = DeltaForce::in_memory();
        let table = lake
            .create(TableConfig::new("t").column("id", DataType::Long))
            .await
            .unwrap();
        let mut batch = table.batch();
        batch.push_file(&lake.file("later.csv")); // doesn't exist yet
        lake.put_file(
            "later.csv",
            "id
1
",
        )
        .await
        .unwrap();
        assert_eq!(table.write(batch).await.unwrap(), 1);

        let mut batch = table.batch();
        batch.push_file(&lake.file("missing.csv"));
        let error = table.write(batch).await.unwrap_err();
        assert!(matches!(error, Error::FileNotFound(_)), "{error}");
        assert_eq!(table.version().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn files_without_rows_write_nothing() {
        let lake = DeltaForce::in_memory();
        let table = lake
            .create(TableConfig::new("t").column("id", DataType::Long))
            .await
            .unwrap();
        lake.put_file(
            "empty.csv",
            "id
",
        )
        .await
        .unwrap();
        let mut batch = table.batch();
        batch.push_file(&lake.file("empty.csv"));
        assert!(matches!(table.write(batch).await, Err(Error::NoRows)));
    }

    #[tokio::test]
    async fn write_reads_the_log_once_however_many_files() {
        let store = crate::test_util::TestStore::new(InMemory::new());
        let lake = DeltaForce::with_test_store(store.clone());
        let config = TableConfig::new("t")
            .column("id", DataType::Long)
            .rows_per_file(2);
        let table = lake.create(config).await.unwrap();
        lake.put_file(
            "ids.csv",
            "id
1 
2
3
4
5
",
        )
        .await
        .unwrap();
        let log_gets = || -> u64 {
            let stats = store.stats();
            stats
                .files
                .iter()
                .filter(|(path, _)| path.contains("_delta_log/"))
                .map(|(_, f)| f.reads.gets)
                .sum()
        };

        let before = log_gets();
        let mut batch = table.batch();
        batch.push_file(&lake.file("ids.csv")); // 3 data files
        table.write(batch).await.unwrap();
        assert_eq!(log_gets() - before, 1);
        assert_eq!(table.inner.file_rows().unwrap().len(), 3);
    }

    /// A table `t` with `id` (key), required `name` and nullable `score`,
    /// holding ids 1 and 2.
    async fn cdc_table(lake: &DeltaForce) -> Table {
        let config = TableConfig::new("t")
            .column("id", DataType::Long)
            .column("name", DataType::String)
            .nullable("score", DataType::Double);
        let table = lake.create(config).await.unwrap();
        let mut batch = table.batch();
        batch.push(crate::row! { "id" => 1, "name" => "a", "score" => 0.5 }).unwrap();
        batch.push(crate::row! { "id" => 2, "name" => "b", "score" => 0.5 }).unwrap();
        table.write(batch).await.unwrap();
        table
    }

    async fn apply_file(lake: &DeltaForce, table: &Table, csv: &str) -> Result<Applied> {
        lake.put_file("cdc.csv", csv).await.unwrap();
        let mut changes = table.change_batch();
        changes.push_file(&lake.file("cdc.csv"));
        let config = crate::ChangesConfig::new().key("id").operation("op");
        table.apply(Changes::in_memory(config, [changes])).await
    }

    #[tokio::test]
    async fn change_files_and_pushed_changes_apply_in_push_order() {
        use crate::{Change, row};
        let lake = DeltaForce::in_memory();
        let table = cdc_table(&lake).await;
        lake.put_file("cdc.csv", "op,id,name
update,3,c2
DELETE,1,
").await.unwrap();

        let mut changes = table.change_batch();
        changes.push(Change::insert(row! { "id" => 3, "name" => "c" })).unwrap();
        changes.push_file(&lake.file("cdc.csv"));
        changes.push(Change::insert(row! { "id" => 1, "name" => "a2" })).unwrap();
        let config = crate::ChangesConfig::new().key("id").operation("op");
        let applied = table.apply(Changes::in_memory(config, [changes])).await.unwrap();

        assert_eq!(applied, Applied { changes: 4, commits: 1 });
        assert_eq!(
            table.rows().await.unwrap().sorted_by("id"),
            [
                row! { "id" => 1, "name" => "a2", "score" => null },
                row! { "id" => 2, "name" => "b", "score" => 0.5 },
                row! { "id" => 3, "name" => "c2", "score" => null },
            ]
        );
    }

    #[tokio::test]
    async fn file_updates_set_only_the_columns_the_file_has() {
        let lake = DeltaForce::in_memory();
        let table = cdc_table(&lake).await;
        // No `score` column: scores are kept. An empty name would be null.
        apply_file(&lake, &table, "op,id,name
U,1,z
").await.unwrap();
        let rows = table.rows().await.unwrap().sorted_by("id");
        assert_eq!(rows.to_vec()[0], crate::row! { "id" => 1, "name" => "z", "score" => 0.5 });

        let error = apply_file(&lake, &table, "op,id,name
U,1,
").await.unwrap_err();
        assert!(error.to_string().contains("row 1: column \"name\" is not nullable"), "{error}");
    }

    #[tokio::test]
    async fn bad_change_files_fail_without_committing() {
        let lake = DeltaForce::in_memory();
        let table = cdc_table(&lake).await;
        let error = |csv: &'static str| {
            let (lake, table) = (lake.clone(), table.clone());
            async move { apply_file(&lake, &table, csv).await.unwrap_err().to_string() }
        };
        let unknown = error("op,id,name
i,3,c
upsert,4,d
").await;
        assert!(unknown.contains("cdc.csv: row 2: unknown operation \"upsert\""), "{unknown}");
        let incomplete = error("op,id
i,3
").await;
        assert!(incomplete.contains("row 1: column \"name\" is not nullable"), "{incomplete}");
        let no_key = error("op,id,name
d,,
").await;
        assert!(no_key.contains("row 1: column \"id\""), "{no_key}");
        let no_op = error("id,name
3,c
").await;
        assert!(no_op.contains("required column \"op\" is missing"), "{no_op}");
        assert_eq!(table.version().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn change_files_need_an_operation_column() {
        let lake = DeltaForce::in_memory();
        let table = cdc_table(&lake).await;
        let mut changes = table.change_batch();
        changes.push_file(&lake.file("never-read.csv"));
        let config = crate::ChangesConfig::new().key("id");
        let error = table.apply(Changes::in_memory(config, [changes])).await.unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)), "{error}");

        let mut changes = table.change_batch();
        changes.push_file(&lake.file("never-read.csv"));
        let config = crate::ChangesConfig::new().key("id").operation("Name");
        let error = table.apply(Changes::in_memory(config, [changes])).await.unwrap_err();
        assert!(error.to_string().contains("also a table column"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn change_files_stream_on_a_multi_threaded_runtime() {
        let lake = DeltaForce::in_memory();
        let table = cdc_table(&lake).await;
        let applied = apply_file(&lake, &table, "op,id,name
d,1,
d,2,
").await.unwrap();
        assert_eq!(applied.changes, 2);
        assert!(table.rows().await.unwrap().is_empty());
    }

    #[test]
    fn split_rows_slices_across_batches() {
        let ids = |v: Vec<i64>| {
            let array: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(v));
            RecordBatch::try_from_iter([("id", array)]).unwrap()
        };
        let (head, tail) = crate::batch::split_rows(&[ids(vec![1, 2]), ids(vec![3, 4, 5])], 3);
        assert_eq!(
            head.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            [2, 1]
        );
        assert_eq!(tail.iter().map(|b| b.num_rows()).collect::<Vec<_>>(), [2]);
    }
}
