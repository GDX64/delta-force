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

let mut cdc = events.change_batch();                    // a CDC file, op column I/U/D
cdc.push_file(&lake.file("landing/cdc.csv"));
events.apply(Changes::in_memory(ChangesConfig::new().key("id").operation("op"), [cdc])).await?;

let mut stream = events.row_stream();                   // one batch in memory at a time
while let Some(rows) = stream.next().await? { /* row.get::<i64>("id")? */ }
```

## Layout

| File | What it holds |
|---|---|
| `src/lake.rs` | Public API: `DeltaForce`, `TableConfig`, `Table`, `DataType` |
| `src/batch.rs` | `RowBatch` / `ChangeBatch`: pushed rows as Arrow builders, pushed files read lazily |
| `src/batch/{csv,parquet,columns}.rs` | File readers: batches of the table columns a file has (`complete` fills the rest) |
| `src/changes.rs` | `Change`, `Changes` (stream of batches), `Planner`: merge planning by key |
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
- Lazy: building a batch does no I/O. `push_file` only records the file;
  `RowBatch::into_stream` reads files as they are pulled. `write` pulls that
  stream, writes a data file every `rows_per_file` rows (default 4M), and
  commits them all in one commit. One snapshot (`DeltaTable::append`) serves
  the whole write, so the log is read once. Zstd by default.
- `ChangeBatch` keeps pushed changes and CDC files in push order. `apply`
  pulls the batch's change stream from inside the merge's blocking thread
  (`Handle::block_on`) and folds each change into the `Planner`, so a CDC
  file is never held whole as changes. File columns are read as nullable
  (deletes only carry the key) and each row is checked like `push`.

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
| `./dev profile [size]` | Records a CPU profile of the same benchmark with [samply](https://github.com/mstange/samply) (`cargo install --locked samply`; on Windows it also needs xperf from the Windows Performance Toolkit, and an administrator terminal) and opens it in the Firefox Profiler. Saved to `target/bench/profile.json.gz`; builds with the `profiling` cargo profile (release + debug symbols). |

Add new commands to `dev.py`.
