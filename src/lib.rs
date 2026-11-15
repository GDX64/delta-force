//! A friendly Delta Lake library: dynamic rows in, dynamic rows out.
//!
//! ```no_run
//! # async fn demo() -> delta_force::Result<()> {
//! use delta_force::{DataType, DeltaForce, TableConfig, row};
//!
//! let lake = DeltaForce::open("data").await?;
//! let events = lake
//!     .create(TableConfig::new("events").column("id", DataType::Long))
//!     .await?;
//! let mut batch = events.batch();
//! batch.push(row! { "id" => 1 })?;
//! events.write(batch).await?;
//! for row in events.rows().await? {
//!     let id: i64 = row.get("id")?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Parquet, Arrow and the storage layer stay internal.
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

pub use batch::{ChangeBatch, RowBatch};
pub use changes::{ApplyMode, Change, Changes, ChangesConfig};
pub use compression::Compression;
pub use error::{Error, Result};
pub use file::LakeFile;
pub use lake::{Applied, DataType, DeltaForce, Table, TableConfig};
pub use row::{FromValue, Row, Rows, Value};
pub use stream::RowStream;
