//! The internal Arrow-level table on top of [`delta_kernel`].
//!
//! The kernel owns the protocol logic (log replay, checkpoints, scans, commits);
//! this module drives it through our [`ObjectStoreEngine`].
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use delta_kernel::{
    Snapshot,
    committer::FileSystemCommitter,
    engine::arrow_conversion::{TryFromArrow, TryIntoArrow},
    engine::arrow_data::ArrowEngineData,
    schema::StructType,
    transaction::{CommitResult, UpdateTableOperation, create_table::create_table},
};
use url::Url;

use crate::engine::ObjectStoreEngine;

const ENGINE_INFO: &str = concat!("delta-force/", env!("CARGO_PKG_VERSION"));

/// A Delta table: a directory of data files plus a `_delta_log/` of commits.
#[derive(Clone)]
pub(crate) struct DeltaTable {
    engine: Arc<ObjectStoreEngine>,
    url: Url,
}

impl DeltaTable {
    /// `root` is the table root as the kernel sees it; its path is the object
    /// path inside the store. Nothing is read until you call a method.
    pub fn new(engine: Arc<ObjectStoreEngine>, mut root: Url) -> Self {
        // The kernel treats the root as a directory.
        if !root.path().ends_with('/') {
            root.set_path(&format!("{}/", root.path()));
        }
        Self { engine, url: root }
    }

    fn snapshot(&self) -> delta_kernel::KernelResult<Arc<Snapshot>> {
        Snapshot::builder_for(self.url.as_str()).build(self.engine.as_ref())
    }

    /// The latest committed version, or `None` if the table does not exist yet.
    pub fn version(&self) -> anyhow::Result<Option<u64>> {
        match self.snapshot() {
            Ok(snapshot) => Ok(Some(snapshot.version())),
            Err(e) if is_missing_table(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The table's schema at its latest version.
    pub fn arrow_schema(&self) -> anyhow::Result<Schema> {
        Ok(self.snapshot()?.schema().as_ref().try_into_arrow()?)
    }

    /// Create the table with the given schema. Returns the new version (0).
    /// Fails if the table already exists.
    pub fn create(&self, schema: &Schema) -> anyhow::Result<u64> {
        // The kernel requires a local table directory to exist up front.
        if self.url.scheme() == "file" {
            let dir = self
                .url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("invalid table path {}", self.url))?;
            std::fs::create_dir_all(dir)?;
        }
        let schema = Arc::new(StructType::try_from_arrow(schema)?);
        let result = create_table(self.url.as_str(), schema, ENGINE_INFO, true)
            .build(self.engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(self.engine.as_ref())?;
        committed_version(result)
    }

    /// Append a batch as a new Parquet file in one commit. Returns the version.
    pub fn write(&self, batch: RecordBatch) -> anyhow::Result<u64> {
        let engine = self.engine.as_ref();
        let mut txn = self
            .snapshot()?
            .update_table(UpdateTableOperation::Write, ENGINE_INFO, true)
            .build(engine, Box::new(FileSystemCommitter::new()))?;

        let context = txn.write_state()?.write_context_builder().build()?;
        txn.add_files(engine.write_data_file(batch, &context)?);

        committed_version(txn.commit(engine)?)
    }

    /// Read the whole table at its latest version.
    pub fn read(&self) -> anyhow::Result<Vec<RecordBatch>> {
        let scan = self.snapshot()?.scan_builder().build()?;
        scan.execute(self.engine.clone())?
            .map(|data| {
                let data = ArrowEngineData::try_from_engine_data(data?)?;
                Ok(RecordBatch::from(data))
            })
            .collect()
    }
}

fn committed_version<S: std::fmt::Debug>(result: CommitResult<S>) -> anyhow::Result<u64> {
    match result {
        CommitResult::Committed(c) => Ok(c.commit_version()),
        CommitResult::Conflicted(c) => {
            anyhow::bail!("commit conflicted with version {}", c.conflict_version())
        }
        CommitResult::Retryable(_) => anyhow::bail!("commit failed with a retryable error"),
    }
}

/// Whether a snapshot error just means "there is no table here".
fn is_missing_table(e: &delta_kernel::KernelError) -> bool {
    matches!(
        e,
        delta_kernel::KernelError::EmptyLog
            | delta_kernel::KernelError::FileNotFound(_)
            | delta_kernel::KernelError::InvalidTableLocation(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field};
    use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    fn table_at(store: Arc<dyn ObjectStore>, root: Url) -> DeltaTable {
        DeltaTable::new(Arc::new(ObjectStoreEngine::new(store)), root)
    }

    fn local(path: &std::path::Path) -> DeltaTable {
        std::fs::create_dir_all(path).unwrap();
        let root = Url::from_directory_path(std::path::absolute(path).unwrap()).unwrap();
        table_at(Arc::new(LocalFileSystem::new()), root)
    }

    fn new_table() -> (TempDir, DeltaTable) {
        let dir = TempDir::new().unwrap();
        let table = local(&dir.path().join("table"));
        (dir, table)
    }

    fn id_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    fn ids(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(values)) as ArrayRef)])
            .unwrap()
    }

    #[test]
    fn version_of_missing_table_is_none() {
        let (_dir, table) = new_table();
        assert_eq!(table.version().unwrap(), None);
    }

    #[test]
    fn create_fails_if_the_table_already_exists() {
        let (_dir, table) = new_table();
        assert_eq!(table.create(&id_schema()).unwrap(), 0);

        assert!(table.create(&id_schema()).is_err());
        assert_eq!(table.version().unwrap(), Some(0));
    }

    #[test]
    fn write_commits_new_versions_and_read_returns_them() {
        let (_dir, table) = new_table();
        table.create(&id_schema()).unwrap();

        assert_eq!(table.write(ids(vec![1, 2, 3])).unwrap(), 1);
        assert_eq!(table.write(ids(vec![4])).unwrap(), 2);

        let rows: usize = table.read().unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
    }

    #[test]
    fn reopening_the_table_sees_earlier_commits() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("table");
        let table = local(&path);
        table.create(&id_schema()).unwrap();
        table.write(ids(vec![7])).unwrap();

        let reopened = local(&path);
        assert_eq!(reopened.version().unwrap(), Some(1));
        assert_eq!(reopened.arrow_schema().unwrap().field(0).name(), "id");
    }

    #[test]
    fn works_on_an_in_memory_store() {
        let table = table_at(Arc::new(InMemory::new()), Url::parse("memory:///").unwrap());
        table.create(&id_schema()).unwrap();
        table.write(ids(vec![1, 2])).unwrap();

        assert_eq!(table.read().unwrap()[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn can_be_used_from_inside_an_async_runtime() {
        let (_dir, table) = new_table();
        table.create(&id_schema()).unwrap();
        assert_eq!(table.write(ids(vec![1])).unwrap(), 1);
    }
}
