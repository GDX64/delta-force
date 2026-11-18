//! [`RowStream`]: read a table one batch of rows at a time.
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::stream::{self, BoxStream, Stream, StreamExt};

use crate::delta::{DeltaTable, ScanBatches};
use crate::lake::{batch_rows, blocking};
use crate::{Result, Rows};

/// The rows of a table, one batch at a time. Get one from
/// [`Table::row_stream`](crate::Table::row_stream).
///
/// Nothing is read until the first [`next`](RowStream::next), and each batch
/// is read only when it is asked for, so one batch is in memory at a time.
pub struct RowStream {
    rows: BoxStream<'static, Result<Rows>>,
}

/// Where a [`RowStream`] is in its scan.
enum Scan {
    NotStarted(DeltaTable),
    Reading(ScanBatches),
}

impl RowStream {
    pub(crate) fn new(table: DeltaTable) -> Self {
        // The kernel is synchronous: each batch is read on a blocking
        // thread, which takes the scan and hands it back with the rows.
        let rows = stream::try_unfold(Scan::NotStarted(table), |scan| {
            blocking(move || scan.next())
        });
        Self { rows: rows.boxed() }
    }

    /// The next batch of rows, or `None` once the table has been read.
    pub async fn next(&mut self) -> Result<Option<Rows>> {
        StreamExt::next(&mut self.rows).await.transpose()
    }
}

impl Scan {
    /// Read the next batch of rows, starting the scan if needed.
    fn next(self) -> Result<Option<(Rows, Scan)>> {
        let _span = tracing::info_span!("next_rows").entered();
        let mut batches = match self {
            Scan::NotStarted(table) => table.scan()?,
            Scan::Reading(batches) => batches,
        };
        let Some(batch) = batches.next() else {
            return Ok(None);
        };
        let rows = tracing::info_span!("to_rows").in_scope(|| batch_rows(&batch?))?;
        Ok(Some((Rows::from(rows), Scan::Reading(batches))))
    }
}

/// For use with stream combinators.
impl Stream for RowStream {
    type Item = Result<Rows>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rows.poll_next_unpin(cx)
    }
}
