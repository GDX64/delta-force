use delta_force::{DataType, DeltaForce, TableConfig, row};

#[tokio::test]
async fn append_a_parquet_file_from_the_lake() -> anyhow::Result<()> {
    let lake = DeltaForce::in_memory();
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .column("name", DataType::String)
                .nullable("score", DataType::Double),
        )
        .await?;

    // Columns: name (string), id (int32), score (double, one null).
    // Matched by name; int32 fits into the table's long column.
    lake.put_file(
        "landing/events.parquet",
        include_bytes!("../fixtures/events.parquet"),
    )
    .await?;

    let mut batch = events.batch();
    batch.push_file(&lake.file("landing/events.parquet")).await?;
    events.write(batch).await?;

    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1, "name" => "signup", "score" => 0.9 },
            row! { "id" => 2, "name" => "login", "score" => null },
        ]
    );
    Ok(())
}
