use delta_force::object_store::local::LocalFileSystem;
use delta_force::{DataType, DeltaForce, RowBatch, TableConfig, row};
use tempfile::TempDir;

#[tokio::test]
async fn create_a_table_write_rows_and_read_them_back() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let lake = DeltaForce::lake(LocalFileSystem::new_with_prefix(dir.path())?);

    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;

    let mut batch = RowBatch::new(events.schema());
    batch.push(row! { "id" => 1, "score" => 0.9 })?;
    batch.push(row! { "id" => 2 })?; // a nullable column can be left out
    events.write(batch).await?;

    let events = lake.table("events").await?;
    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1, "score" => 0.9 },
            row! { "id" => 2, "score" => null },
        ]
    );
    Ok(())
}
