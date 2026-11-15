use delta_force::{Change, Changes, ChangesConfig, DataType, DeltaForce, TableConfig, row};

#[tokio::test]
async fn apply_a_stream_of_inserts_updates_and_deletes() -> anyhow::Result<()> {
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

    let mut changes = events.change_batch();
    changes.push(Change::insert(row! { "id" => 3, "score" => 0.1 }))?;
    changes.push(Change::update(row! { "id" => 1, "score" => 0.7 }))?;
    changes.push(Change::delete(row! { "id" => 2 }))?;

    let stream = Changes::in_memory(ChangesConfig::new().key("id"), [changes]);
    events.apply(stream).await?;

    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1, "score" => 0.7 },
            row! { "id" => 3, "score" => 0.1 },
        ]
    );
    Ok(())
}
