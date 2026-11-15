# delta-force

A friendly, async Rust library for **ingesting** data into Delta Lake tables.
Users work with dynamic rows; Arrow, Parquet and object_store stay internal
and must not appear in the public API.

## Using it

```rust
let lake = DeltaForce::open("data").await?;            // or DeltaForce::in_memory()
let events = lake
    .create(TableConfig::new("events").column("id", DataType::Long))
    .await?;                                            // lake.table("events") to reopen

let mut batch = events.batch();                         // columnar, checked on push
batch.push(row! { "id" => 1 })?;
events.write(batch).await?;

let mut changes = events.change_batch();                // keyed insert/update/delete
changes.push(Change::delete(row! { "id" => 1 }))?;
events.apply(Changes::in_memory(ChangesConfig::new().key("id"), [changes])).await?;

let mut stream = events.row_stream();                   // one batch in memory at a time
while let Some(rows) = stream.next().await? { /* row.get::<i64>("id")? */ }
```

## Layout

| File | What it holds |
|---|---|
| `src/lake.rs` | Public API: `DeltaForce`, `TableConfig`, `Table`, `DataType` |
| `src/batch.rs` | `RowBatch` / `ChangeBatch`: rows stored as Arrow builders |
| `src/changes.rs` | `Change`, `Changes` (stream of batches), merge planning by key |
| `src/stream.rs` | `RowStream`: batch-at-a-time reads on a blocking thread |
| `src/row.rs` | `Value`, `Row`, `Rows`, the `row!` macro |
| `src/compression.rs` | Per-table codec, stored as the `delta-force.compression` property |
| `src/delta.rs` | Internal Arrow-level table on `delta_kernel` (create, write, merge, scan) |
| `src/engine.rs`, `src/sync_store.rs` | Our kernel `Engine` over any `ObjectStore` |

## How it works

- `delta_kernel` is synchronous; public async methods run it with
  `spawn_blocking`.
- `apply` is merge-on-read: replaced/deleted rows go into deletion vectors,
  new values are appended. One commit per `ChangeBatch`. Strict mode only.
- One Parquet file per `write`/batch; Zstd by default.

## Conventions

- **Ingestion first.** Maintenance (compaction, vacuum) is out of scope for now.
- **Workflow:** agree on the API, write one test, then implement.
- **`tests/api/`**: one happy-path test per feature, written to read like
  documentation (register it in `tests/api/main.rs`). Edge cases belong in
  unit tests next to the code.
- Run `cargo test` and `cargo clippy --all-targets` (no warnings) before committing.

## Dev commands

`./dev <command>` (works in bash and PowerShell; `./dev --help` lists them):

| Command | What it does |
|---|---|
| `./dev benchmark [size]` | Ingests a generated CSV (default `1GB`, e.g. `100MB`) and reports wall time, CPU and peak memory. Source: `examples/ingest_1gb.rs`, which uses `TestStore` virtual files (`test-util` feature). |

Add new commands to `dev.py`.
