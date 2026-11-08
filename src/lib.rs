use anyhow::anyhow;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path};

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

    pub async fn version(&self) -> anyhow::Result<Option<u32>> {
        let path = Path::from("/_delta_log");
        let last: Option<ObjectMeta> = self
            .store
            .list(Some(&path))
            .filter_map(async |v| {
                return v.ok();
            })
            .fold(None, async |acc: Option<ObjectMeta>, v| {
                let Some(acc) = acc else {
                    return Some(v);
                };
                if acc.location.filename() < v.location.filename() {
                    return Some(v);
                }
                return Some(acc);
            })
            .await;

        let Some(last) = last else { return Ok(None) };
        let (version, _) = last
            .location
            .filename()
            .ok_or(anyhow!("file must have a name"))?
            .split_once(".")
            .ok_or(anyhow!("file must have extension"))?;
        let version = version.parse::<u32>()?;

        return Ok(Some(version));
    }
}
