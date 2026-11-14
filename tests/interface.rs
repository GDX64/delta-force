use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use delta_force::Table;
use tempfile::TempDir;

fn new_table() -> (TempDir, Table) {
    let dir = TempDir::new().unwrap();
    let table = Table::new(dir.path().join("table")).unwrap();
    (dir, table)
}

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn ids(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(values)) as ArrayRef)]).unwrap()
}

#[test]
fn version_of_missing_table_is_none() {
    let (_dir, table) = new_table();
    assert_eq!(table.version().unwrap(), None);
}

#[test]
fn create_makes_version_zero() {
    let (_dir, table) = new_table();

    assert_eq!(table.create(&id_schema()).unwrap(), 0);
    assert_eq!(table.version().unwrap(), Some(0));
}

#[test]
fn create_fails_if_the_table_already_exists() {
    let (_dir, table) = new_table();
    table.create(&id_schema()).unwrap();

    assert!(table.create(&id_schema()).is_err());
    assert_eq!(table.version().unwrap(), Some(0));
}

#[test]
fn write_commits_a_new_version() {
    let (_dir, table) = new_table();
    table.create(&id_schema()).unwrap();

    assert_eq!(table.write(ids(vec![1, 2, 3])).unwrap(), 1);
    assert_eq!(table.write(ids(vec![4])).unwrap(), 2);
    assert_eq!(table.version().unwrap(), Some(2));
}

#[test]
fn read_returns_what_was_written() {
    let (_dir, table) = new_table();
    table.create(&id_schema()).unwrap();
    table.write(ids(vec![1, 2, 3])).unwrap();
    table.write(ids(vec![4, 5])).unwrap();

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

#[test]
fn reopening_the_table_sees_earlier_commits() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("table");
    let table = Table::new(&path).unwrap();
    table.create(&id_schema()).unwrap();
    table.write(ids(vec![7])).unwrap();

    let reopened = Table::new(&path).unwrap();

    assert_eq!(reopened.version().unwrap(), Some(1));
    assert_eq!(reopened.read().unwrap()[0].num_rows(), 1);
}
