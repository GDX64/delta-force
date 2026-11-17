use delta_force::object_store::memory::InMemory;
use delta_force::{DataType, DeltaForce, RowBatch, TableConfig, row};

#[tokio::test]
async fn read_a_table_as_a_stream_of_row_batches() -> anyhow::Result<()> {
    let lake = DeltaForce::lake(InMemory::new());
    let events = lake
        .create(TableConfig::new("events").column("id", DataType::Long))
        .await?;
    for ids in [[1, 2], [3, 4]] {
        let mut batch = RowBatch::new(events.schema());
        for id in ids {
            batch.push(row! { "id" => id })?;
        }
        events.write(batch).await?;
    }

    // Only one batch of rows is held in memory at a time.
    let mut stream = events.row_stream();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(rows) = stream.next().await? {
        for row in &rows {
            ids.push(row.get("id")?);
        }
    }

    ids.sort();
    assert_eq!(ids, [1, 2, 3, 4]);
    Ok(())
}
