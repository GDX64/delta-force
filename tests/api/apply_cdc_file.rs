use delta_force::{ChangesBatch, ChangesConfig, DataType, DeltaForce, RowBatch, TableConfig, row};

#[tokio::test]
async fn apply_a_cdc_file_from_the_lake() -> anyhow::Result<()> {
    let lake = DeltaForce::in_memory();
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .nullable("score", DataType::Double),
        )
        .await?;
    let mut batch = RowBatch::new(events.schema());
    batch.push(row! { "id" => 1, "score" => 0.9 })?;
    batch.push(row! { "id" => 2, "score" => 0.5 })?;
    events.write(batch).await?;

    // A change feed landing in the lake: the `op` column says what each row
    // does (I, U, D or insert, update, delete). A delete only needs the key.
    lake.put_file(
        "landing/cdc.csv",
        "op,id,score\n\
         I,3,0.1\n\
         U,1,0.7\n\
         D,2,\n",
    )
    .await?;

    // The file is read as it is applied, in one commit, never held whole.
    let config = ChangesConfig::new(events.schema()).key("id").operation("op");
    let mut changes = ChangesBatch::new(config);
    changes.push_file(&lake.file("landing/cdc.csv"));
    events.apply(changes).await?;

    assert_eq!(
        events.rows().await?.sorted_by("id"),
        [
            row! { "id" => 1, "score" => 0.7 },
            row! { "id" => 3, "score" => 0.1 },
        ]
    );
    Ok(())
}
