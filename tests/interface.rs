use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use delta_force::Table;
use object_store::memory::InMemory;
use url::Url;

fn new_table() -> Table {
    Table::new(Arc::new(InMemory::new()), Url::parse("memory:///").unwrap())
}

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn ids(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(values)) as ArrayRef)]).unwrap()
}

#[tokio::test]
async fn version_of_missing_table_is_none() {
    assert_eq!(new_table().version().unwrap(), None);
}

#[tokio::test]
async fn create_makes_version_zero() {
    let table = new_table();

    assert_eq!(table.create(&id_schema()).unwrap(), 0);
    assert_eq!(table.version().unwrap(), Some(0));
}

#[tokio::test]
async fn create_fails_if_the_table_already_exists() {
    let table = new_table();
    table.create(&id_schema()).unwrap();

    assert!(table.create(&id_schema()).is_err());
    assert_eq!(table.version().unwrap(), Some(0));
}

#[tokio::test]
async fn write_commits_a_new_version() {
    let table = new_table();
    table.create(&id_schema()).unwrap();

    assert_eq!(table.write(ids(vec![1, 2, 3])).await.unwrap(), 1);
    assert_eq!(table.write(ids(vec![4])).await.unwrap(), 2);
    assert_eq!(table.version().unwrap(), Some(2));
}

#[tokio::test]
async fn read_returns_what_was_written() {
    let table = new_table();
    table.create(&id_schema()).unwrap();
    table.write(ids(vec![1, 2, 3])).await.unwrap();
    table.write(ids(vec![4, 5])).await.unwrap();

    let mut values: Vec<i32> = table
        .read()
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    values.sort();

    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}
