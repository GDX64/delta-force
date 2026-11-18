//! [`LakeFile`]: a file stored in the lake, such as a landing CSV.
use std::fmt;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::stream::{self, BoxStream, Stream, StreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::{Error, Result};

/// Chunks of a file are joined up to at least this many bytes before they
/// are handed on, however small the store's own pieces are.
const CHUNK_SIZE: usize = 1 << 23;

/// A file in the lake, addressed by its path relative to the lake root.
/// Get one with [`Lake::file`](crate::Lake::file).
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
    Parquet,
}

impl LakeFile {
    /// `path` is relative to `root`; `..` and empty segments can't escape it.
    pub(crate) fn new(store: Arc<dyn ObjectStore>, root: &Path, path: &str) -> Self {
        let parts = root
            .parts()
            .chain(path.split('/').filter(|p| !p.is_empty()).map(Into::into));
        Self {
            store,
            location: Path::from_iter(parts),
            path: path.to_owned(),
        }
    }

    /// The path relative to the lake root, as given.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn format(&self) -> Result<Format> {
        let extension = self
            .path
            .rsplit_once('.')
            .map(|(_, e)| e.to_ascii_lowercase());
        match extension.as_deref() {
            Some("csv") => Ok(Format::Csv),
            Some("parquet") => Ok(Format::Parquet),
            _ => Err(Error::UnsupportedFormat(self.path.clone())),
        }
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub(crate) fn location(&self) -> &Path {
        &self.location
    }

    pub(crate) async fn size(&self) -> Result<u64> {
        let meta = self
            .store
            .head(&self.location)
            .await
            .map_err(|e| self.error(e))?;
        Ok(meta.size)
    }

    /// The file's contents as a stream of chunks of at least 1 MiB (the
    /// last may be smaller), fetched as they are read.
    pub(crate) async fn chunks(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        let object = self
            .store
            .get(&self.location)
            .await
            .map_err(|e| self.error(e))?;
        let this = self.clone();
        let pieces = object
            .into_stream()
            .map(move |piece| piece.map_err(|e| this.error(e)));
        Ok(join_pieces(pieces, CHUNK_SIZE).boxed())
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

/// Join `pieces` into chunks of at least `size` bytes, or until the stream
/// ends. A piece that is already big enough is passed on as it is.
fn join_pieces(
    pieces: impl Stream<Item = Result<Bytes>> + Send + 'static,
    size: usize,
) -> impl Stream<Item = Result<Bytes>> {
    // Fused: after the last chunk the stream is polled once more.
    let pieces = pieces.boxed().fuse();
    stream::try_unfold(pieces, move |mut pieces| async move {
        let mut chunk = BytesMut::new();
        while chunk.len() < size {
            let Some(piece) = pieces.next().await else {
                break;
            };
            let piece = piece?;
            if chunk.is_empty() && piece.len() >= size {
                return Ok(Some((piece, pieces)));
            }
            chunk.extend_from_slice(&piece);
        }
        Ok((!chunk.is_empty()).then(|| (chunk.freeze(), pieces)))
    })
}

impl fmt::Debug for LakeFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LakeFile").field(&self.path).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn join(pieces: &[&'static [u8]], size: usize) -> Vec<Vec<u8>> {
        let pieces: Vec<_> = pieces.iter().map(|p| Ok(Bytes::from_static(p))).collect();
        let pieces = stream::iter(pieces);
        join_pieces(pieces, size)
            .map(|chunk| chunk.unwrap().to_vec())
            .collect()
            .await
    }

    #[tokio::test]
    async fn small_pieces_are_joined_up_to_the_chunk_size() {
        let chunks = join(&[b"ab", b"cd", b"ef", b"g"], 4).await;
        assert_eq!(chunks, [b"abcd".to_vec(), b"efg".to_vec()]);
    }

    #[tokio::test]
    async fn big_pieces_pass_through_whole() {
        let chunks = join(&[b"a", b"bcdefgh", b"ijklmno"], 4).await;
        assert_eq!(chunks, [b"abcdefgh".to_vec(), b"ijklmno".to_vec()]);
    }

    #[tokio::test]
    async fn empty_files_have_no_chunks() {
        assert!(join(&[], 4).await.is_empty());
        assert!(join(&[b""], 4).await.is_empty());
    }

    #[tokio::test]
    async fn errors_end_the_stream() {
        let pieces = stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Err(Error::FileNotFound("x".into())),
            Ok(Bytes::from_static(b"cd")),
        ]);
        let chunks: Vec<_> = join_pieces(pieces, 4).collect().await;
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_err());
    }
}
