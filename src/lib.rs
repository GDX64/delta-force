#![feature(iterator_try_collect)]
use std::collections::HashSet;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::{AsyncBufRead, AsyncBufReadExt, Stream, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, path::Path};
use parquet::arrow::ArrowWriter;
mod actions;
pub use actions::{Action, Add, Metadata, Protocol, Remove};

/// A Delta table: a directory of data files plus a `_delta_log/` of JSON
/// commits describing which files make up the table at each version.
///
/// All I/O goes through an [`ObjectStore`], so the table works the same on
/// local disk, S3, or in memory.
pub struct Table {
    store: Box<dyn ObjectStore>,
}

impl Table {
    /// Open a table rooted at the given store.
    ///
    /// Nothing is read yet; this only binds the table to its storage.
    pub fn new(store: Box<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn commit_list<'a>(&'a self) -> BoxStream<'a, ObjectMeta> {
        let path = Path::from("_delta_log");
        let s = self
            .store
            .list(Some(&path))
            .filter_map(async |meta| meta.ok());
        return Box::pin(s);
    }

    /// The latest committed version, or `None` if the table has no commits.
    ///
    /// Only files named `<20-digit version>.json` count as commits;
    /// checkpoints, `_last_checkpoint`, crc files etc. are ignored.
    pub async fn version(&self) -> anyhow::Result<Option<u64>> {
        let path = Path::from("_delta_log");
        self.store
            .list(Some(&path))
            .try_fold(None, async |latest: Option<u64>, meta| {
                Ok(match commit_version(&meta.location) {
                    Some(v) => latest.max(Some(v)),
                    None => latest,
                })
            })
            .await
            .map_err(Into::into)
    }

    pub async fn create(&self, schema: &Schema) -> anyhow::Result<u64> {
        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let protocol = Action::Protocol(protocol);
        let meta_data = Metadata {
            id: uuid::Uuid::new_v4().to_string(),
            schema_string: Metadata::schema_string(schema),
        };
        let meta_data = Action::Metadata(meta_data);
        let r = self.commit([protocol, meta_data]).await?;
        return Ok(r);
    }

    fn file_name(version: u64) -> String {
        return format!("{:0>20}.json", version.to_string());
    }

    pub async fn read_commit(&self, i: usize) -> anyhow::Result<Vec<Action>> {
        let path = Path::from("_delta_log");
        let path = path.join(Self::file_name(i as u64));
        let lines: Vec<Action> = self.read_commit_file(path).await?.collect().await;
        Ok(lines)
    }

    pub async fn commit(&self, action: impl IntoIterator<Item = Action>) -> anyhow::Result<u64> {
        let current_version = self.version().await?;
        let file = action
            .into_iter()
            .map(|v| {
                return serde_json::to_string(&v);
            })
            .try_collect::<Vec<String>>()?
            .join("\n");
        let next_version = current_version.map(|v| v + 1).unwrap_or(0);
        let file_name = Self::file_name(next_version);
        let path = Path::from("_delta_log").join(file_name);
        let _result = self
            .store
            .put_opts(&path, file.into(), PutMode::Create.into())
            .await?;
        return Ok(next_version);
    }

    pub async fn write(&self, batch: RecordBatch) -> anyhow::Result<u64> {
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }
        let parquet_name = format!("{}.parquet", uuid::Uuid::new_v4().to_string());
        let add = Add {
            data_change: true,
            path: parquet_name,
            size: buffer.len() as u64,
        };

        let path = Path::from(add.path.clone());
        self.store
            .put_opts(&path, buffer.into(), PutMode::Create.into())
            .await?;
        let v = self.commit([Action::Add(add)]).await?;
        return Ok(v);
    }

    pub async fn files(&self) -> anyhow::Result<Vec<String>> {
        let mut files = HashSet::<String>::new();
        let mut commits = self.commit_list();
        while let Some(commit) = commits.next().await {
            let mut actions = Box::pin(self.read_commit_file(commit.location).await?);
            while let Some(action) = actions.next().await {
                match action {
                    Action::Add(add) => {
                        files.insert(add.path);
                    }
                    Action::Remove(remove) => {
                        files.remove(&remove.path);
                    }
                    _ => {}
                }
            }
        }

        Ok(files.into_iter().collect())
    }

    async fn read_commit_file(
        &self,
        path: Path,
    ) -> anyhow::Result<impl Stream<Item = Action> + use<>> {
        let r = self.store.get(&path).await?;
        let s = into_buf_reader(r).lines().filter_map(async |s| {
            let Ok(s) = s else { return None };
            serde_json::from_str::<Action>(&s).ok()
        });
        Ok(s)
    }
}

/// Adapt an object-store download into an async, buffered reader.
///
/// Chunks are pulled from the store as the reader is polled, so the whole
/// object is never held in memory. Call `.lines()` (from `AsyncBufReadExt`)
/// on the result to get a stream of lines, even when a line spans chunks.
fn into_buf_reader(result: GetResult) -> impl AsyncBufRead + Unpin {
    result
        .into_stream()
        .map_err(std::io::Error::other)
        .into_async_read()
}

/// Parse `<version>.json` from a log file path. `None` for anything else.
fn commit_version(path: &Path) -> Option<u64> {
    path.filename()?.strip_suffix(".json")?.parse().ok()
}
