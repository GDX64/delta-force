use delta_force::object_store::memory::InMemory;
use delta_force::{DataType, Lake, RowBatch, TableConfig, row};

#[tokio::test]
async fn append_a_csv_file_from_the_lake() -> anyhow::Result<()> {
    let lake = Lake::new(InMemory::new());
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .column("name", DataType::String)
                .nullable("score", DataType::Double),
        )
        .await?;

    // A file landing in the lake; columns are matched by header name.
    lake.put_file(
        "landing/events.csv",
        "name,id,score\n\
         signup,1,0.9\n\
         login,2,\n",
    )
    .await?;

    let mut batch = RowBatch::new(events.schema());
    batch.push_file(&lake.file("landing/events.csv"));
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
