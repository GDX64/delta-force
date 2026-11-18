use delta_force::object_store::memory::InMemory;
use delta_force::{
    Change, ChangesBatch, ChangesConfig, DataType, Lake, RowBatch, TableConfig, row,
};

#[tokio::test]
async fn apply_inserts_updates_and_deletes() -> anyhow::Result<()> {
    let lake = Lake::new(InMemory::new());
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;
    let mut batch = RowBatch::new(events.schema());
    batch.push(row! { "id" => 1, "score" => 0.9 })?;
    batch.push(row! { "id" => 2, "score" => 0.5 })?;
    events.write(batch).await?;

    // Changes are checked against the table's schema as they are pushed,
    // and applied by key in one commit.
    let config = ChangesConfig::new(events.schema()).key("id");
    let mut changes = ChangesBatch::new(config);
    changes.push(Change::insert(row! { "id" => 3, "score" => 0.1 }))?;
    changes.push(Change::update(row! { "id" => 1, "score" => 0.7 }))?;
    changes.push(Change::delete(row! { "id" => 2 }))?;
    events.apply(changes).await?;

    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1, "score" => 0.7 },
            row! { "id" => 3, "score" => 0.1 },
        ]
    );
    Ok(())
}
