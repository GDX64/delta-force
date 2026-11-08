use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};

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
}

/// Parse `<version>.json` from a log file path. `None` for anything else.
fn commit_version(path: &Path) -> Option<u64> {
    path.filename()?.strip_suffix(".json")?.parse().ok()
}
