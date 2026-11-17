//! A friendly Delta Lake library: dynamic rows in, dynamic rows out.
//!
//! ```no_run
//! # async fn demo() -> delta_force::Result<()> {
//! use delta_force::object_store::local::LocalFileSystem;
//! use delta_force::{DataType, DeltaForce, RowBatch, TableConfig, row};
//!
//! let lake = DeltaForce::lake(LocalFileSystem::new_with_prefix("data")?);
//! let events = lake
//!     .create(TableConfig::new("events").column("id", DataType::Long))
//!     .await?;
//! let mut batch = RowBatch::new(events.schema());
//! batch.push(row! { "id" => 1 })?;
//! events.write(batch).await?;
//! for row in events.rows().await? {
//!     let id: i64 = row.get("id")?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Storage is any [`object_store`] backend, re-exported so versions match.
//! Parquet and Arrow stay internal.
/// The storage backends a lake can live in: pass one to [`DeltaForce::lake`].
pub use object_store;

mod batch;
mod changes;
mod compression;
mod delta;
mod engine;
mod error;
mod file;
mod lake;
mod row;
mod stream;
mod sync_store;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

pub use batch::{ChangesBatch, RowBatch};
pub use changes::{ApplyMode, Change, ChangesConfig};
pub use compression::Compression;
pub use error::{Error, Result};
pub use file::LakeFile;
pub use lake::{Applied, DataType, DeltaForce, Schema, Table, TableConfig};
pub use row::{FromValue, Row, Rows, Value};
pub use stream::RowStream;
