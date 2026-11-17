use delta_force::object_store::memory::InMemory;
use delta_force::{Compression, DataType, DeltaForce, RowBatch, TableConfig, row};

#[tokio::test]
async fn choose_how_a_table_compresses_its_files() -> anyhow::Result<()> {
    let lake = DeltaForce::lake(InMemory::new());
    lake.create(
        TableConfig::new("events")
            .column("id", DataType::Long)
            .compression(Compression::Snappy),
    )
    .await?;

    // The setting is stored with the table, so every writer uses it.
    let events = lake.table("events").await?;
    assert_eq!(events.compression(), Compression::Snappy);

    let mut batch = RowBatch::new(events.schema());
    batch.push(row! { "id" => 1 })?;
    events.write(batch).await?;

    assert_eq!(events.rows().await?, [row! { "id" => 1 }]);
    Ok(())
}
