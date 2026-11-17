use std::path::PathBuf;

use delta_force::object_store::local::LocalFileSystem;
use delta_force::{DataType, DeltaForce, RowBatch, TableConfig, row};

/// Kept after the run so the files can be inspected:
/// `target/api-tests/ingest_1000_rows_to_the_filesystem/events`.
fn output_dir(test: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/api-tests")
        .join(test);
    let _ = std::fs::remove_dir_all(&dir); // start fresh on every run
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn ingest_1000_rows_to_the_filesystem() -> anyhow::Result<()> {
    // The lake lives wherever its store points: here, a local directory.
    let store = LocalFileSystem::new_with_prefix(output_dir("ingest_1000_rows_to_the_filesystem"))?;
    let lake = DeltaForce::lake(store);
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .column("name", DataType::String)
                .nullable("score", DataType::Double)
                .compression(delta_force::Compression::Zstd),
        )
        .await?;

    let mut batch = RowBatch::new(events.schema());
    for id in 0..1_000_i64 {
        batch.push(row! {
            "id" => id,
            "name" => format!("event-{id}"),
            "score" => id as f64 / 10.0,
        })?;
    }
    events.write(batch).await?;

    assert_eq!(events.rows().await?.len(), 1_000);
    Ok(())
}
