use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
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
        let file = self.store.get(&path).await?;
        let bytes = file.bytes().await?;
        let lines = bytes
            .lines()
            .filter_map(async |s| {
                let Ok(s) = s else { return None };
                let Ok(action) = serde_json::from_str::<Action>(&s) else {
                    return None;
                };
                return Some(action);
            })
            .collect()
            .await;

        return Ok(lines);
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

/// Parse `<version>.json` from a log file path. `None` for anything else.
fn commit_version(path: &Path) -> Option<u64> {
    path.filename()?.strip_suffix(".json")?.parse().ok()
}
