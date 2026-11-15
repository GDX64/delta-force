use delta_force::{Compression, DataType, DeltaForce, TableConfig, row};

#[tokio::test]
async fn choose_how_a_table_compresses_its_files() -> anyhow::Result<()> {
    let lake = DeltaForce::in_memory();
    lake.create(
        TableConfig::new("events")
            .column("id", DataType::Long)
            .compression(Compression::Snappy),
    )
    .await?;

    // The setting is stored with the table, so every writer uses it.
    let events = lake.table("events").await?;
    assert_eq!(events.compression(), Compression::Snappy);

    let mut batch = events.batch();
    batch.push(row! { "id" => 1 })?;
    events.write(batch).await?;

    assert_eq!(events.rows().await?, [row! { "id" => 1 }]);
    Ok(())
}
