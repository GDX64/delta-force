use delta_force::{DataType, DeltaForce, TableConfig, row};
use tempfile::TempDir;

#[tokio::test]
async fn create_a_table_write_rows_and_read_them_back() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let lake = DeltaForce::open(dir.path()).await?;

    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;

    let mut batch = events.batch();
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
