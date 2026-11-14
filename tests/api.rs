use delta_force::{DataType, DeltaForce, TableConfig, row};
use tempfile::TempDir;

#[tokio::test]
async fn create_write_and_read_rows() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let lake = DeltaForce::open(dir.path()).await?;

    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;

    let version = events
        .write([
            row! { "id" => 1, "score" => 0.9 },
            row! { "id" => 2 }, // missing nullable column -> null
        ])
        .await?;
    assert_eq!(version, 1);

    // Reopen through the lake to read back what was committed.
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
