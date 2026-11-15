//! Test helpers, behind the `test-util` feature: a [`TestStore`] that can
//! serve virtual files, generated on the fly, and counts what is read, for
//! benchmarks.
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

/// Virtual files are generated at most this many bytes at a time.
const CHUNK: u64 = 64 * 1024;

type Generate = Arc<dyn Fn(Range<u64>) -> Bytes + Send + Sync>;

#[derive(Clone)]
struct VirtualFile {
    size: u64,
    generate: Generate,
}

/// An object store that serves virtual files, whose bytes are generated on
/// read, and sends every other path to an inner store. Virtual files are
/// read-only and don't show up in listings. Reads and writes are counted,
/// see [`TestStore::stats`]. Clones share the same files and counts.
#[derive(Clone)]
pub struct TestStore {
    inner: Arc<dyn ObjectStore>,
    files: Arc<RwLock<HashMap<Path, VirtualFile>>>,
    counts: Arc<Mutex<BTreeMap<String, FileStats>>>,
}

/// What went through a [`TestStore`] so far, from [`TestStore::stats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Reads of every file together.
    pub reads: Reads,
    /// Writes of every file together.
    pub writes: Writes,
    /// Each file read or written, by path.
    pub files: BTreeMap<String, FileStats>,
}

/// Reads and writes of one file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileStats {
    pub reads: Reads,
    pub writes: Writes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reads {
    /// Gets of the file's data, whole or a range; `head` isn't counted.
    pub gets: u64,
    /// Bytes the readers pulled, which can be fewer than they asked for.
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Writes {
    /// Puts; a multipart upload is one put, whatever its number of parts.
    pub puts: u64,
    /// Bytes sent.
    pub bytes: u64,
}

impl TestStore {
    pub fn new(inner: impl ObjectStore) -> Self {
        Self {
            inner: Arc::new(inner),
            files: Default::default(),
            counts: Default::default(),
        }
    }

    /// Everything read and written through this store so far.
    pub fn stats(&self) -> Stats {
        let files = self.counts.lock().unwrap().clone();
        let mut stats = Stats::default();
        for file in files.values() {
            stats.reads.gets += file.reads.gets;
            stats.reads.bytes += file.reads.bytes;
            stats.writes.puts += file.writes.puts;
            stats.writes.bytes += file.writes.bytes;
        }
        Stats { files, ..stats }
    }

    fn count(&self, location: &Path, update: impl FnOnce(&mut FileStats)) {
        let mut files = self.counts.lock().unwrap();
        update(files.entry(location.to_string()).or_default());
    }

    /// Count the get, and the bytes of `result` as they are pulled.
    fn counted(&self, location: &Path, mut result: GetResult) -> GetResult {
        self.count(location, |f| f.reads.gets += 1);
        let (store, location) = (self.clone(), location.clone());
        let (meta, range) = (result.meta.clone(), result.range.clone());
        let attributes = std::mem::take(&mut result.attributes);
        let extensions = std::mem::take(&mut result.extensions);
        let payload = result.into_stream().map(move |chunk| {
            if let Ok(bytes) = &chunk {
                store.count(&location, |f| f.reads.bytes += bytes.len() as u64);
            }
            chunk
        });
        GetResult {
            payload: GetResultPayload::Stream(payload.boxed()),
            meta,
            range,
            attributes,
            extensions,
        }
    }

    /// A store whose real files live under the local directory `dir`,
    /// created if missing.
    pub fn on_disk(dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let local = object_store::local::LocalFileSystem::new_with_prefix(dir)
            .map_err(std::io::Error::other)?;
        Ok(Self::new(local))
    }

    /// Serve `size` bytes at `path`. `generate` returns the bytes of any
    /// range of the file, at most 64 KB at a time, and must always return
    /// the same bytes for the same range.
    pub fn create_virtual_file(
        &self,
        path: impl Into<String>,
        size: u64,
        generate: impl Fn(Range<u64>) -> Bytes + Send + Sync + 'static,
    ) {
        let file = VirtualFile {
            size,
            generate: Arc::new(generate),
        };
        self.files
            .write()
            .unwrap()
            .insert(Path::from(path.into()), file);
    }

    fn virtual_file(&self, path: &Path) -> Option<VirtualFile> {
        self.files.read().unwrap().get(path).cloned()
    }

    fn read_only(&self, path: &Path) -> Result<()> {
        match self.virtual_file(path) {
            Some(_) => Err(object_store::Error::NotSupported {
                source: format!("virtual file {path} is read-only").into(),
            }),
            None => Ok(()),
        }
    }
}

fn serve(location: &Path, file: VirtualFile, options: GetOptions) -> Result<GetResult> {
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: Default::default(),
        size: file.size,
        e_tag: None,
        version: None,
    };
    options.check_preconditions(&meta)?;
    let range = match &options.range {
        Some(range) => range
            .as_range(file.size)
            .map_err(|e| object_store::Error::Generic {
                store: "TestStore",
                source: Box::new(e),
            })?,
        None => 0..file.size,
    };
    let chunks = if options.head {
        range.end..range.end
    } else {
        range.clone()
    };
    let payload = stream::iter(chunks.step_by(CHUNK as usize))
        .map(move |start| Ok((file.generate)(start..(start + CHUNK).min(range.end))));
    Ok(GetResult {
        payload: GetResultPayload::Stream(payload.boxed()),
        meta,
        range,
        attributes: Default::default(),
        extensions: Default::default(),
    })
}

#[async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.read_only(location)?;
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.count(location, |f| {
            f.writes.puts += 1;
            f.writes.bytes += bytes;
        });
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.read_only(location)?;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        self.count(location, |f| f.writes.puts += 1);
        Ok(Box::new(CountedUpload { inner, store: self.clone(), location: location.clone() }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let result = match self.virtual_file(location) {
            Some(file) => serve(location, file, options)?,
            None => self.inner.get_opts(location, options).await?,
        };
        Ok(if head {
            result
        } else {
            self.counted(location, result)
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.read_only(from)?;
        self.read_only(to)?;
        self.inner.copy_opts(from, to, options).await
    }
}

/// A multipart upload whose parts are counted as writes of `location`.
#[derive(Debug)]
struct CountedUpload {
    inner: Box<dyn MultipartUpload>,
    store: TestStore,
    location: Path,
}

#[async_trait]
impl MultipartUpload for CountedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let part = self.inner.put_part(data);
        let (store, location) = (self.store.clone(), self.location.clone());
        async move {
            part.await?;
            store.count(&location, |f| f.writes.bytes += bytes);
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore({})", self.inner)
    }
}

impl fmt::Debug for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let files = self.files.read().unwrap();
        f.debug_struct("TestStore")
            .field("inner", &self.inner)
            .field("virtual_files", &files.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    /// The alphabet, repeated: the byte at offset `i` is `a + i % 26`.
    fn alphabet(range: Range<u64>) -> Bytes {
        range
            .map(|i| b'a' + (i % 26) as u8)
            .collect::<Vec<u8>>()
            .into()
    }

    #[tokio::test]
    async fn virtual_files_are_generated_on_read() {
        let store = TestStore::new(InMemory::new());
        store.create_virtual_file("letters.txt", 30, alphabet);
        let path = Path::from("letters.txt");

        assert_eq!(store.head(&path).await.unwrap().size, 30);
        let all = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(all, "abcdefghijklmnopqrstuvwxyzabcd");
        assert_eq!(store.get_range(&path, 24..28).await.unwrap(), "yzab");
    }

    #[tokio::test]
    async fn large_files_are_generated_in_small_chunks() {
        let largest = Arc::new(AtomicU64::new(0));
        let store = TestStore::new(InMemory::new());
        store.create_virtual_file("big.txt", 10 * 1024 * 1024, {
            let largest = largest.clone();
            move |range: Range<u64>| {
                largest.fetch_max(range.end - range.start, Ordering::Relaxed);
                alphabet(range)
            }
        });

        let all = store
            .get(&Path::from("big.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(all.len(), 10 * 1024 * 1024);
        assert!(largest.load(Ordering::Relaxed) <= 64 * 1024);
    }

    #[tokio::test]
    async fn other_paths_use_the_inner_store() {
        let store = TestStore::new(InMemory::new());
        let path = Path::from("tables/events/part-0.parquet");

        store.put(&path, PutPayload::from("data")).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "data"
        );
    }

    #[tokio::test]
    async fn reads_count_gets_and_the_bytes_streamed() {
        let store = TestStore::new(InMemory::new());
        store.create_virtual_file("letters.txt", 30, alphabet);
        let letters = Path::from("letters.txt");
        let other = Path::from("other.txt");
        store.put(&other, PutPayload::from("data")).await.unwrap();

        store.get(&letters).await.unwrap().bytes().await.unwrap();
        store.get_range(&letters, 24..28).await.unwrap();
        store.head(&letters).await.unwrap(); // metadata only: not a get
        store.get(&other).await.unwrap().bytes().await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.reads, Reads { gets: 3, bytes: 38 });
        assert_eq!(
            stats.files["letters.txt"].reads,
            Reads { gets: 2, bytes: 34 }
        );
        assert_eq!(stats.files["other.txt"].reads, Reads { gets: 1, bytes: 4 });
        assert_eq!(stats.files.len(), 2);
    }

    #[tokio::test]
    async fn only_bytes_actually_pulled_are_counted() {
        let store = TestStore::new(InMemory::new());
        store.create_virtual_file("big.txt", 10 * 1024 * 1024, alphabet);
        let path = Path::from("big.txt");

        let mut chunks = store.get(&path).await.unwrap().into_stream();
        let first = chunks.next().await.unwrap().unwrap();
        drop(chunks);

        assert_eq!(
            store.stats().reads,
            Reads {
                gets: 1,
                bytes: first.len() as u64
            }
        );
    }

    #[tokio::test]
    async fn writes_count_puts_and_their_bytes() {
        let store = TestStore::new(InMemory::new());
        let small = Path::from("small.txt");
        let large = Path::from("large.txt");

        store.put(&small, PutPayload::from("data")).await.unwrap();
        store
            .put(&small, PutPayload::from("more data"))
            .await
            .unwrap();
        // A multipart upload is one put, whatever the number of parts.
        let mut upload = store.put_multipart(&large).await.unwrap();
        upload.put_part(PutPayload::from("abc")).await.unwrap();
        upload.put_part(PutPayload::from("defg")).await.unwrap();
        upload.complete().await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.writes, Writes { puts: 3, bytes: 20 });
        assert_eq!(
            stats.files["small.txt"].writes,
            Writes { puts: 2, bytes: 13 }
        );
        assert_eq!(
            stats.files["large.txt"].writes,
            Writes { puts: 1, bytes: 7 }
        );
        assert_eq!(stats.files["small.txt"].reads, Reads::default());
        assert_eq!(stats.reads, Reads::default());
    }

    #[tokio::test]
    async fn virtual_files_are_read_only() {
        let store = TestStore::new(InMemory::new());
        store.create_virtual_file("letters.txt", 30, alphabet);

        let path = Path::from("letters.txt");
        assert!(store.put(&path, PutPayload::from("x")).await.is_err());
    }
}
