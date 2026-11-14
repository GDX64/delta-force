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
    events
        .write([
            row! { "id" => 1, "score" => 0.9 },
            row! { "id" => 2, "score" => 0.5 },
        ])
        .await?;

    let changes = Changes::in_memory(
        ChangesConfig::new().key("id"),
        [
            Change::insert(row! { "id" => 3, "score" => 0.1 }),
            Change::update(row! { "id" => 1, "score" => 0.7 }),
            Change::delete(row! { "id" => 2 }),
        ],
    );
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
