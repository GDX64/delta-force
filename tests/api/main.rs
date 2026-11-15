//! Happy-path tests, one per feature, written to read like documentation.
//!
//! No edge cases here: each test shows the most readable way to use a
//! feature. Every feature gets one.
mod append_csv;
mod append_parquet;
mod apply_changes;
mod compression;
mod create_and_write;
mod ingest_to_disk;
mod stream_rows;
