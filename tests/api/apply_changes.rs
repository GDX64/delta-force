use delta_force::{Change, Changes, ChangesConfig, DataType, DeltaForce, TableConfig, row};

#[tokio::test]
async fn apply_inserts_updates_and_deletes() -> anyhow::Result<()> {
    let lake = DeltaForce::in_memory();
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;
    let mut batch = events.batch();
    batch.push(row! { "id" => 1, "score" => 0.9 })?;
    batch.push(row! { "id" => 2, "score" => 0.5 })?;
    events.write(batch).await?;

    // One batch is applied in one commit.
    let mut batch = events.change_batch();
    batch.push(Change::insert(row! { "id" => 3, "score" => 0.1 }))?;
    batch.push(Change::update(row! { "id" => 1, "score" => 0.7 }))?;
    batch.push(Change::delete(row! { "id" => 2 }))?;

    let mut changes = Changes::new(ChangesConfig::new().key("id"));
    changes.push(batch);
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
