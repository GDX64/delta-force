//! [`SyncObjectStore`]: a blocking facade over an async [`ObjectStore`].
//!
//! The kernel's `Engine` traits are synchronous, but object stores are async
//! (and cloud stores need a tokio reactor to make progress). This wrapper owns a
//! private tokio runtime, runs each call on it, and blocks the calling thread
//! until the result is ready. Because the work runs on its own runtime, it is
//! also safe to call from inside another async runtime.
//!
//! It knows nothing about Delta: methods mirror [`ObjectStore`] but take no
//! `.await`.
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{
    ListResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, Result,
};
use tracing::Instrument;

#[derive(Clone)]
pub struct SyncObjectStore {
    store: Arc<dyn ObjectStore>,
    runtime: Arc<Runtime>,
}

/// A tokio runtime that is torn down without blocking, so dropping the store
/// from async code doesn't panic.
struct Runtime(Option<tokio::runtime::Runtime>);

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

impl SyncObjectStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to start the object store's tokio runtime");
        Self {
            store,
            runtime: Arc::new(Runtime(Some(runtime))),
        }
    }

    /// Run the future built by `f` on the private runtime, blocking until done.
    /// The future runs inside the caller's span, so traces show it there.
    fn run<T, F, Fut>(&self, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn ObjectStore>) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let fut = f(self.store.clone());
        let rt = self.runtime.0.as_ref().expect("runtime is only taken on drop");
        rt.spawn(
            async move {
                let _ = tx.send(fut.await);
            }
            .in_current_span(),
        );
        rx.recv().expect("object store runtime stopped unexpectedly")
    }

    #[tracing::instrument(name = "store_get", skip_all, fields(%path))]
    pub fn get(&self, path: &Path) -> Result<Bytes> {
        let path = path.clone();
        self.run(|s| async move { s.get(&path).await?.bytes().await })
    }

    /// Write `data`. With `overwrite = false` this is an atomic "create if
    /// absent" and fails with `Error::AlreadyExists` if the object exists.
    #[tracing::instrument(name = "store_put", skip_all, fields(%path, bytes = data.len()))]
    pub fn put(&self, path: &Path, data: Bytes, overwrite: bool) -> Result<()> {
        let path = path.clone();
        let mode = if overwrite { PutMode::Overwrite } else { PutMode::Create };
        let opts = PutOptions { mode, ..Default::default() };
        self.run(|s| async move { s.put_opts(&path, data.into(), opts).await })
            .map(|_| ())
    }

    #[tracing::instrument(name = "store_head", skip_all, fields(%path))]
    pub fn head(&self, path: &Path) -> Result<ObjectMeta> {
        let path = path.clone();
        self.run(|s| async move { s.head(&path).await })
    }

    #[tracing::instrument(name = "store_delete", skip_all, fields(%path))]
    pub fn delete(&self, path: &Path) -> Result<()> {
        let path = path.clone();
        self.run(|s| async move { s.delete(&path).await })
    }

    /// Objects and sub-directories directly under `prefix` (non-recursive).
    #[tracing::instrument(name = "store_list", skip_all, fields(%prefix))]
    pub fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let prefix = prefix.clone();
        self.run(|s| async move { s.list_with_delimiter(Some(&prefix)).await })
    }

    /// Atomic copy that fails with `Error::AlreadyExists` if `to` exists.
    #[tracing::instrument(name = "store_copy", skip_all, fields(%from, %to))]
    pub fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(|s| async move { s.copy_if_not_exists(&from, &to).await })
    }
}
