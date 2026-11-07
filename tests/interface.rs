use delta_force::Table;
use object_store::{memory::InMemory, path::Path};

/// Put an (empty) commit file in the log. Contents don't matter for
/// `version()`, only the file name does.
async fn put_commit(store: &InMemory, version: u64) {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    store.put(&path, "{}".into()).await.unwrap();
}

#[tokio::test]
async fn version_of_empty_table_is_none() {
    let table = Table::new(Box::new(InMemory::new()));

    assert_eq!(table.version().await.unwrap(), None);
}

#[tokio::test]
async fn version_is_the_latest_commit() {
    let storage = InMemory::new();
    put_commit(&storage, 0).await;
    put_commit(&storage, 1).await;
    put_commit(&storage, 2).await;

    let table = Table::new(Box::new(storage));

    assert_eq!(table.version().await.unwrap(), Some(2));
}
