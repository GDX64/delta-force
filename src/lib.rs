//! A small Delta table built on top of [`delta_kernel`].
//!
//! The kernel owns the protocol logic (log replay, checkpoints, scans, commits);
//! this crate wires it to an [`ObjectStore`] and exposes a tiny table API.
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
use delta_kernel_default_engine::{
    DefaultEngine, executor::tokio::TokioBackgroundExecutor,
};
use object_store::ObjectStore;
use url::Url;

const ENGINE_INFO: &str = concat!("delta-force/", env!("CARGO_PKG_VERSION"));

type Engine = DefaultEngine<TokioBackgroundExecutor>;

/// A Delta table: a directory of data files plus a `_delta_log/` of commits.
///
/// All I/O goes through an [`ObjectStore`], so the table works the same on
/// local disk, S3, or in memory.
pub struct Table {
    engine: Arc<Engine>,
    url: Url,
}

impl Table {
    /// Bind a table to the given store. `url` is the table root as the kernel
    /// sees it (e.g. `memory:///` for an [`InMemory`](object_store::memory::InMemory)
    /// store). Nothing is read yet.
    pub fn new(store: Arc<dyn ObjectStore>, mut url: Url) -> Self {
        // The kernel treats the root as a directory.
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Self {
            engine: Arc::new(DefaultEngine::builder(store).build()),
            url,
        }
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
    pub async fn write(&self, batch: RecordBatch) -> anyhow::Result<u64> {
        let engine = self.engine.as_ref();
        let mut txn = self
            .snapshot()?
            .update_table(UpdateTableOperation::Write, ENGINE_INFO, true)
            .build(engine, Box::new(FileSystemCommitter::new()))?;

        let context = txn.write_state()?.write_context_builder().build()?;
        let file = engine
            .write_parquet(&ArrowEngineData::new(batch), &context)
            .await?;
        txn.add_files(file);

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
