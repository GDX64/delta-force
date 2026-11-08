use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use delta_force::{Action, Add, Protocol, Remove, Table};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
async fn read_commit_parses_add_and_remove() {
    let storage = InMemory::new();
    let commit = [
        r#"{"add":{"path":"part-0000.parquet","partitionValues":{},"size":123,"modificationTime":1700000000000,"dataChange":true}}"#,
        r#"{"remove":{"path":"part-0001.parquet","deletionTimestamp":1700000001000,"dataChange":true}}"#,
    ]
    .join("\n");
    storage
        .put(
            &Path::from("_delta_log/00000000000000000001.json"),
            commit.into(),
        )
        .await
        .unwrap();

    let table = Table::new(Box::new(storage));
    let actions = table.read_commit(1).await.unwrap();

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        Action::Add(a) if a.path == "part-0000.parquet" && a.size == 123 && a.data_change
    ));
    assert!(matches!(
        &actions[1],
        Action::Remove(r) if r.path == "part-0001.parquet" && r.data_change
    ));
}

/// Write a commit made of raw JSON action lines.
async fn put_actions(store: &InMemory, version: u64, lines: &[&str]) {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    store.put(&path, lines.join("\n").into()).await.unwrap();
}

fn add(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":0,"dataChange":true}}}}"#
    )
}

fn remove(path: &str) -> String {
    format!(r#"{{"remove":{{"path":"{path}","deletionTimestamp":0,"dataChange":true}}}}"#)
}

#[tokio::test]
async fn files_replays_adds_and_removes() {
    let storage = InMemory::new();
    put_actions(&storage, 0, &[&add("a.parquet"), &add("b.parquet")]).await;
    // Version 1 replaces a.parquet with c.parquet (e.g. an update/compaction).
    put_actions(&storage, 1, &[&remove("a.parquet"), &add("c.parquet")]).await;

    let table = Table::new(Box::new(storage));
    let mut files = table.files().await.unwrap();
    files.sort();

    assert_eq!(files, vec!["b.parquet", "c.parquet"]);
}

fn add_action(path: &str) -> Action {
    Action::Add(Add {
        path: path.into(),
        size: 1,
        data_change: true,
    })
}

fn remove_action(path: &str) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change: true,
    })
}

#[tokio::test]
async fn commit_writes_the_next_version() {
    let table = Table::new(Box::new(InMemory::new()));

    // First commit on an empty log is version 0.
    let v0 = table.commit(vec![add_action("a.parquet")]).await.unwrap();
    let v1 = table
        .commit(vec![remove_action("a.parquet"), add_action("b.parquet")])
        .await
        .unwrap();

    assert_eq!((v0, v1), (0, 1));
    assert_eq!(table.version().await.unwrap(), Some(1));
    assert_eq!(table.files().await.unwrap(), vec!["b.parquet"]);
}

#[tokio::test]
async fn write_stores_a_parquet_file_and_commits_it() {
    // Keep a handle on the store so the test can look inside it afterwards.
    let store = Arc::new(InMemory::new());
    let table = Table::new(Box::new(store.clone()));
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef)])
            .unwrap();

    let version = table.write(batch.clone()).await.unwrap();

    // The write is a commit: first one on an empty log is version 0...
    assert_eq!(version, 0);
    // ...which adds exactly one data file to the table.
    let files = table.files().await.unwrap();
    assert_eq!(files.len(), 1);

    // That file is real Parquet that reads back to what we wrote.
    let bytes = store
        .get(&Path::from(files[0].as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
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
