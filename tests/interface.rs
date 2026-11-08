use delta_force::{Action, Protocol, Table};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

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

#[tokio::test]
async fn read_commit_parses_protocol_and_metadata() {
    let storage = InMemory::new();
    let commit = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"metaData":{"id":"table-id","format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}"#,
    ]
    .join("\n");
    storage
        .put(
            &Path::from("_delta_log/00000000000000000000.json"),
            commit.into(),
        )
        .await
        .unwrap();

    let table = Table::new(Box::new(storage));
    let actions = table.read_commit(0).await.unwrap();

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        actions[0],
        Action::Protocol(Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
            ..
        })
    ));
    assert!(matches!(&actions[1], Action::Metadata(m) if m.id == "table-id"));
}

#[tokio::test]
async fn version_ignores_files_that_are_not_commits() {
    let storage = InMemory::new();
    put_commit(&storage, 0).await;
    // Data files and non-commit log files live alongside the commits.
    for name in [
        "part-0000.parquet",
        "_delta_log/_last_checkpoint",
        "_delta_log/00000000000000000010.checkpoint.parquet",
        "_delta_log/00000000000000000000.crc",
    ] {
        storage.put(&Path::from(name), "".into()).await.unwrap();
    }

    let table = Table::new(Box::new(storage));

    assert_eq!(table.version().await.unwrap(), Some(0));
}
