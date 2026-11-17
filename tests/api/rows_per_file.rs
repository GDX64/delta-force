use delta_force::{DataType, DeltaForce, RowBatch, TableConfig, row};

#[tokio::test]
async fn large_files_are_written_in_parts_but_committed_once() -> anyhow::Result<()> {
    let lake = DeltaForce::in_memory();
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                // Defaults to 4M. While a batch is written, every 2 rows are
                // written out as a data file, so memory holds at most 2.
                .rows_per_file(2),
        )
        .await?;
    lake.put_file("landing/ids.csv", "id\n1\n2\n3\n4\n5\n")
        .await?;

    // Pushing a file only records it; write streams it into 3 data files
    // and commits them together.
    let mut batch = RowBatch::new(events.schema());
    batch.push_file(&lake.file("landing/ids.csv"));
    assert_eq!(events.write(batch).await?, 1);
    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1 },
            row! { "id" => 2 },
            row! { "id" => 3 },
            row! { "id" => 4 },
            row! { "id" => 5 },
        ]
    );
    Ok(())
}
