use object_store::ObjectStore;

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
}
