//! A small Delta table built on top of [`delta_kernel`].
//!
//! The kernel owns the protocol logic (log replay, checkpoints, scans, commits);
//! this crate supplies the [`LocalEngine`] it calls for I/O and exposes a tiny
//! table API.
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use delta_kernel::{
    Snapshot,
    committer::FileSystemCommitter,
    engine::arrow_conversion::TryFromArrow,
    engine::arrow_data::ArrowEngineData,
    schema::StructType,
    transaction::{CommitResult, UpdateTableOperation, create_table::create_table},
};
use url::Url;

mod engine;
pub use engine::LocalEngine;

const ENGINE_INFO: &str = concat!("delta-force/", env!("CARGO_PKG_VERSION"));

/// A Delta table: a directory of data files plus a `_delta_log/` of commits.
///
/// All I/O goes through our own [`LocalEngine`] (local filesystem only).
pub struct Table {
    engine: Arc<LocalEngine>,
    url: Url,
}

impl Table {
    /// Open the table rooted at the local directory `path`; the directory is
    /// created if missing. Nothing is read until you call a method.
    pub fn new(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path.as_ref())?;
        let url = Url::from_directory_path(std::fs::canonicalize(path)?)
            .map_err(|()| anyhow::anyhow!("invalid table path"))?;
        Ok(Self {
            engine: Arc::new(LocalEngine::new()),
            url,
        })
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

    /// Create the table with the given schema. Returns the new version (0).
    /// Fails if the table already exists.
    pub fn create(&self, schema: &Schema) -> anyhow::Result<u64> {
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
        delta_kernel::KernelError::EmptyLog | delta_kernel::KernelError::FileNotFound(_)
    )
}
