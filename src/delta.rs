//! The internal Arrow-level table on top of [`delta_kernel`].
//!
//! The kernel owns the protocol logic (log replay, checkpoints, scans, commits);
//! this module drives it through our [`ObjectStoreEngine`].
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::{
    Engine, FileMeta, Snapshot,
    actions::deletion_vector_writer::{KernelDeletionVector, StreamingDeletionVectorWriter},
    committer::FileSystemCommitter,
    engine::arrow_conversion::{TryFromArrow, TryIntoArrow},
    engine::arrow_data::ArrowEngineData,
    scan::{ScanMetadata, state::{ScanFile, transform_to_logical}},
    schema::StructType,
    transaction::{
        BoundWriteContext, CommitResult, Transaction, UpdateTableOperation,
        create_table::create_table,
    },
};
use url::Url;

use crate::compression::{self, Compression};
use crate::engine::ObjectStoreEngine;

const ENGINE_INFO: &str = concat!("delta-force/", env!("CARGO_PKG_VERSION"));
/// Table property: rows per data file when ingesting.
const ROWS_PER_FILE: &str = "delta-force.rows-per-file";
pub(crate) const DEFAULT_ROWS_PER_FILE: usize = 4_000_000;

/// Our settings, stored as table properties when a table is created.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    pub compression: Compression,
    pub rows_per_file: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self { compression: Compression::default(), rows_per_file: DEFAULT_ROWS_PER_FILE }
    }
}

/// A data file's rows that are still live, as read for a merge.
pub(crate) struct LiveFile {
    pub path: String,
    /// Row position in the file of each live row, in order.
    pub positions: Vec<u64>,
    /// The live rows, in the same order as `positions`.
    pub data: Vec<RecordBatch>,
    /// Positions already deleted by the file's current deletion vector.
    deleted: Vec<u64>,
}

/// What a merge does: delete rows by position, append new rows.
#[derive(Default)]
pub(crate) struct MergePlan {
    /// Data file path -> row positions to delete.
    pub deletes: HashMap<String, Vec<u64>>,
    pub append: Option<RecordBatch>,
}

/// An append in progress: one snapshot of the table and one transaction.
/// Data files written with [`Append::write_file`] stay invisible until
/// [`Append::commit`] adds them all in one commit.
pub(crate) struct Append {
    engine: Arc<ObjectStoreEngine>,
    txn: Transaction,
    context: BoundWriteContext,
    compression: Compression,
    schema: Schema,
}

impl Append {
    /// The table's schema at the snapshot this append writes to.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Write `batch` as a data file of this append, uncommitted.
    #[tracing::instrument(skip_all, fields(rows = batch.num_rows()))]
    pub fn write_file(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        let add = self.engine.write_data_file(batch, &self.context, self.compression)?;
        self.txn.add_files(add);
        Ok(())
    }

    /// Commit every data file written so far. Returns the version.
    #[tracing::instrument(skip_all)]
    pub fn commit(self) -> anyhow::Result<u64> {
        committed_version(self.txn.commit(self.engine.as_ref())?)
    }
}

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

    #[tracing::instrument(skip_all)]
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

    /// The codec new data files use: the table's stored setting, or the
    /// default for tables without one.
    pub fn compression(&self) -> anyhow::Result<Compression> {
        let snapshot = self.snapshot()?;
        Ok(compression_of(&snapshot))
    }

    /// Rows per data file when ingesting: the stored setting, or the default.
    pub fn rows_per_file(&self) -> anyhow::Result<usize> {
        let snapshot = self.snapshot()?;
        Ok(snapshot
            .table_properties()
            .unknown_properties
            .get(ROWS_PER_FILE)
            .and_then(|n| n.parse().ok())
            .unwrap_or(DEFAULT_ROWS_PER_FILE))
    }

    /// The table's schema at its latest version.
    pub fn arrow_schema(&self) -> anyhow::Result<Schema> {
        Ok(self.snapshot()?.schema().as_ref().try_into_arrow()?)
    }

    /// Create the table with the given schema. Returns the new version (0).
    /// Fails if the table already exists.
    #[tracing::instrument(name = "create_table", skip_all)]
    pub fn create(&self, schema: &Schema, settings: Settings) -> anyhow::Result<u64> {
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
            // Updates and deletes are recorded as deletion vectors (merge-on-read).
            .with_table_properties([
                ("delta.enableDeletionVectors", "true"),
                settings.compression.property(),
                (ROWS_PER_FILE, &settings.rows_per_file.to_string()),
            ])
            .build(self.engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(self.engine.as_ref())?;
        committed_version(result)
    }

    /// Append a batch as a new Parquet file in one commit. Returns the version.
    #[cfg(test)]
    pub fn write(&self, batch: RecordBatch) -> anyhow::Result<u64> {
        let mut append = self.append()?;
        append.write_file(batch)?;
        append.commit()
    }

    /// Start an append: the log is read once, here, for every data file
    /// written and for the commit.
    #[tracing::instrument(name = "start_append", skip_all)]
    pub fn append(&self) -> anyhow::Result<Append> {
        let snapshot = self.snapshot()?;
        let compression = compression_of(&snapshot);
        let schema = snapshot.schema().as_ref().try_into_arrow()?;
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, ENGINE_INFO, true)
            .build(self.engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        let context = txn.write_state()?.write_context_builder().build()?;
        Ok(Append { engine: self.engine.clone(), txn, context, compression, schema })
    }

    /// Merge-on-read update in one commit. `f` gets every data file's live
    /// rows (with their row positions in the file) and returns which positions
    /// to delete and which new rows to append. Deletes are recorded as deletion
    /// vectors; no existing data file is rewritten. Returns the new version, or
    /// `None` if the plan was empty and nothing was committed.
    #[tracing::instrument(skip_all)]
    pub fn merge<E: From<anyhow::Error>>(
        &self,
        f: impl FnOnce(&[LiveFile]) -> Result<MergePlan, E>,
    ) -> Result<Option<u64>, E> {
        let (snapshot, metadata, files) = self.live_files()?;
        let plan = f(&files)?;
        if plan.deletes.is_empty() && plan.append.is_none() {
            return Ok(None);
        }
        let compression = compression_of(&snapshot);
        let commit = || -> anyhow::Result<u64> {
            let engine = self.engine.as_ref();
            let mut txn = snapshot
                .update_table(UpdateTableOperation::Merge, ENGINE_INFO, true)
                .build(engine, Box::new(FileSystemCommitter::new()))?;
            let context = txn.write_state()?.write_context_builder().build()?;

            if !plan.deletes.is_empty() {
                let _span = tracing::info_span!("write_deletion_vectors", files = plan.deletes.len())
                    .entered();
                let dv_path = context.new_deletion_vector_path("dv".into());
                let mut buffer = Vec::new();
                let mut descriptors = HashMap::new();
                let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
                for (path, positions) in plan.deletes {
                    let file = files
                        .iter()
                        .find(|f| f.path == path)
                        .ok_or_else(|| anyhow::anyhow!("unknown data file {path}"))?;
                    let mut dv = KernelDeletionVector::new();
                    dv.add_deleted_row_indexes(file.deleted.iter().copied().chain(positions));
                    let written = writer.write_deletion_vector(dv)?;
                    descriptors.insert(path, written.to_descriptor(&dv_path));
                }
                writer.finalize()?;
                engine
                    .storage_handler()
                    .put(&dv_path.absolute_path()?, buffer.into(), false)?;
                txn.update_deletion_vectors(
                    descriptors,
                    metadata.into_iter().map(|m| Ok(m.scan_files)),
                )?;
            }
            if let Some(batch) = plan.append {
                txn.add_files(engine.write_data_file(batch, &context, compression)?);
            }
            let _span = tracing::info_span!("commit").entered();
            committed_version(txn.commit(engine)?)
        };
        Ok(Some(commit()?))
    }

    /// Every data file at the latest version with its live (not deleted) rows.
    #[tracing::instrument(skip_all, fields(files))]
    fn live_files(
        &self,
    ) -> anyhow::Result<(Arc<Snapshot>, Vec<ScanMetadata>, Vec<LiveFile>)> {
        let engine = self.engine.as_ref();
        let snapshot = self.snapshot()?;
        let scan = snapshot.clone().scan_builder().build()?;
        let metadata = tracing::info_span!("scan_metadata")
            .in_scope(|| scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>())?;
        tracing::Span::current().record("files", metadata.len());
        let mut scan_files = Vec::new();
        for m in &metadata {
            scan_files = m.visit_scan_files(scan_files, |acc: &mut Vec<ScanFile>, f| acc.push(f))?;
        }

        let physical = scan.physical_schema().clone();
        let logical = scan.logical_schema().clone();
        let mut files = Vec::with_capacity(scan_files.len());
        for file in scan_files {
            let _span = tracing::info_span!("read_live_file", path = %file.path).entered();
            let deleted: Vec<u64> = file
                .dv_info
                .get_row_indexes(engine, &self.url)?
                .unwrap_or_default();
            let deleted_set: HashSet<u64> = deleted.iter().copied().collect();
            let meta = FileMeta {
                location: self.url.join(&file.path)?,
                last_modified: file.modification_time,
                size: file.size as u64,
            };
            let mut positions = Vec::new();
            let mut data = Vec::new();
            let mut next = 0u64;
            for chunk in engine
                .parquet_handler()
                .read_parquet_files(&[meta], physical.clone(), None)?
            {
                let chunk = transform_to_logical(
                    engine,
                    chunk?,
                    &physical,
                    &logical,
                    file.transform.clone(),
                )?;
                let batch = RecordBatch::from(ArrowEngineData::try_from_engine_data(chunk)?);
                let start = next;
                next += batch.num_rows() as u64;
                let keep: BooleanArray =
                    (start..next).map(|p| Some(!deleted_set.contains(&p))).collect();
                positions.extend((start..next).filter(|p| !deleted_set.contains(p)));
                data.push(filter_record_batch(&batch, &keep)?);
            }
            files.push(LiveFile { path: file.path, positions, data, deleted });
        }
        Ok((snapshot, metadata, files))
    }

    /// Scan the latest version one batch at a time, handing each to `f`.
    /// Stops early when `f` returns `false`.
    #[tracing::instrument(skip_all)]
    pub fn scan(&self, mut f: impl FnMut(RecordBatch) -> bool) -> anyhow::Result<()> {
        let scan = self.snapshot()?.scan_builder().build()?;
        let mut batches = scan.execute(self.engine.clone())?;
        while let Some(data) = tracing::info_span!("read_batch").in_scope(|| batches.next()) {
            let batch = RecordBatch::from(ArrowEngineData::try_from_engine_data(data?)?);
            let _span = tracing::info_span!("hand_off_batch", rows = batch.num_rows()).entered();
            if !f(batch) {
                break;
            }
        }
        Ok(())
    }

    /// Row count of each data file at the latest version.
    #[cfg(test)]
    pub fn file_rows(&self) -> anyhow::Result<Vec<usize>> {
        let (_, _, files) = self.live_files()?;
        Ok(files.iter().map(|f| f.positions.len()).collect())
    }

    /// Read the whole table at its latest version.
    #[cfg(test)]
    pub fn read(&self) -> anyhow::Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        self.scan(|batch| {
            batches.push(batch);
            true
        })?;
        Ok(batches)
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

/// The compression stored in the snapshot's table properties.
fn compression_of(snapshot: &Snapshot) -> Compression {
    snapshot
        .table_properties()
        .unknown_properties
        .get(compression::PROPERTY)
        .and_then(|name| Compression::from_name(name))
        .unwrap_or_default()
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
        assert_eq!(table.create(&id_schema(), Settings::default()).unwrap(), 0);

        assert!(table.create(&id_schema(), Settings::default()).is_err());
        assert_eq!(table.version().unwrap(), Some(0));
    }

    #[test]
    fn write_commits_new_versions_and_read_returns_them() {
        let (_dir, table) = new_table();
        table.create(&id_schema(), Settings::default()).unwrap();

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
        table.create(&id_schema(), Settings::default()).unwrap();
        table.write(ids(vec![7])).unwrap();

        let reopened = local(&path);
        assert_eq!(reopened.version().unwrap(), Some(1));
        assert_eq!(reopened.arrow_schema().unwrap().field(0).name(), "id");
    }

    #[test]
    fn works_on_an_in_memory_store() {
        let table = table_at(Arc::new(InMemory::new()), Url::parse("memory:///").unwrap());
        table.create(&id_schema(), Settings::default()).unwrap();
        table.write(ids(vec![1, 2])).unwrap();

        assert_eq!(table.read().unwrap()[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn can_be_used_from_inside_an_async_runtime() {
        let (_dir, table) = new_table();
        table.create(&id_schema(), Settings::default()).unwrap();
        assert_eq!(table.write(ids(vec![1])).unwrap(), 1);
    }

    /// The codec of the first column chunk of every data file in `dir`.
    fn codecs_in(dir: &std::path::Path) -> Vec<delta_kernel::parquet::basic::Compression> {
        use delta_kernel::parquet::file::reader::{FileReader, SerializedFileReader};
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .map(|p| {
                let reader = SerializedFileReader::new(std::fs::File::open(p).unwrap()).unwrap();
                reader.metadata().row_group(0).column(0).compression()
            })
            .collect()
    }

    #[test]
    fn data_files_use_the_table_compression() {
        let dir = TempDir::new().unwrap();
        for compression in [Compression::Snappy, Compression::Zstd] {
            let path = dir.path().join(compression.to_string());
            let table = local(&path);
            table.create(&id_schema(), Settings { compression, ..Default::default() }).unwrap();
            table.write(ids(vec![1, 2, 3])).unwrap();

            assert_eq!(table.compression().unwrap(), compression);
            assert_eq!(codecs_in(&path), [compression.codec()]);
        }
    }
}
