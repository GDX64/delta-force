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
//! events.write([row! { "id" => 1 }]).await?;
//! for row in events.rows().await? {
//!     let id: i64 = row.get("id")?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Parquet, Arrow and the storage layer stay internal.
mod delta;
mod engine;
mod error;
mod lake;
mod row;
mod sync_store;

pub use error::{Error, Result};
pub use lake::{DataType, DeltaForce, Table, TableConfig};
pub use row::{FromValue, Row, Rows, Value};
