//! [`LakeFile`]: a file stored in the lake, such as a landing CSV.
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::{Error, Result};

/// A file in the lake, addressed by its path relative to the lake root.
/// Get one with [`DeltaForce::file`](crate::DeltaForce::file).
#[derive(Clone)]
pub struct LakeFile {
    store: Arc<dyn ObjectStore>,
    location: Path,
    path: String,
}

/// The file formats [`RowBatch::push_file`](crate::RowBatch::push_file) reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Csv,
}

impl LakeFile {
    /// `path` is relative to `root`; `..` and empty segments can't escape it.
    pub(crate) fn new(store: Arc<dyn ObjectStore>, root: &Path, path: &str) -> Self {
        let parts = root
            .parts()
            .chain(path.split('/').filter(|p| !p.is_empty()).map(Into::into));
        Self { store, location: Path::from_iter(parts), path: path.to_owned() }
    }

    /// The path relative to the lake root, as given.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn format(&self) -> Result<Format> {
        let extension = self.path.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
        match extension.as_deref() {
            Some("csv") => Ok(Format::Csv),
            _ => Err(Error::UnsupportedFormat(self.path.clone())),
        }
    }

    pub(crate) async fn read(&self) -> Result<Bytes> {
        let object = self.store.get(&self.location).await.map_err(|e| self.error(e))?;
        object.bytes().await.map_err(|e| self.error(e))
    }

    pub(crate) async fn put(&self, contents: Vec<u8>) -> Result<()> {
        self.store
            .put(&self.location, PutPayload::from(contents))
            .await
            .map(|_| ())
            .map_err(|e| self.error(e))
    }

    fn error(&self, e: object_store::Error) -> Error {
        match e {
            object_store::Error::NotFound { .. } => Error::FileNotFound(self.path.clone()),
            other => Error::Storage(other.into()),
        }
    }
}

impl fmt::Debug for LakeFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LakeFile").field(&self.path).finish()
    }
}
