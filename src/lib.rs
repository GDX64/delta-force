use std::collections::HashSet;

use futures::{AsyncBufRead, AsyncBufReadExt, Stream, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use serde::{Deserialize, Serialize};

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

    pub async fn read_commit(&self, i: usize) -> anyhow::Result<Vec<Action>> {
        let path = Path::from("_delta_log");
        let path = path.join(format!("{:0>20}.json", i.to_string()));
        let lines: Vec<Action> = self.read_commit_file(path).await?.collect().await;
        Ok(lines)
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

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "remove")]
    Remove(Remove),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub min_reader_version: u64,
    pub min_writer_version: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    pub path: String,
    pub size: u64,
    pub data_change: bool,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    pub path: String,
    pub data_change: bool,
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
