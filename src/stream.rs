//! [`RowStream`]: read a table one batch of rows at a time.
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::delta::DeltaTable;
use crate::lake::batch_rows;
use crate::{Error, Result, Rows};

/// How many batches the reader may get ahead of the consumer.
const BUFFER: usize = 2;

/// The rows of a table, one batch at a time. Get one from
/// [`Table::row_stream`](crate::Table::row_stream).
///
/// Nothing is read until the first [`next`](RowStream::next). Only a couple
/// of batches are held in memory at once.
pub struct RowStream {
    state: State,
}

enum State {
    NotStarted(DeltaTable),
    Reading {
        rx: mpsc::Receiver<Result<Rows>>,
        reader: Option<JoinHandle<()>>,
    },
}

impl RowStream {
    pub(crate) fn new(table: DeltaTable) -> Self {
        Self { state: State::NotStarted(table) }
    }

    /// The next batch of rows, or `None` once the table has been read.
    pub async fn next(&mut self) -> Result<Option<Rows>> {
        self.start();
        let State::Reading { rx, reader } = &mut self.state else {
            unreachable!("started above")
        };
        match rx.recv().await {
            Some(rows) => rows.map(Some),
            // The reader finished: surface a panic instead of ending quietly.
            None => match reader.take() {
                Some(handle) => handle.await.map(|()| None).map_err(|e| Error::Storage(e.into())),
                None => Ok(None),
            },
        }
    }

    /// Start the kernel scan on a blocking thread, if not started yet.
    fn start(&mut self) {
        let State::NotStarted(table) = &self.state else {
            return;
        };
        let table = table.clone();
        let (tx, rx) = mpsc::channel(BUFFER);
        let span = tracing::info_span!("row_stream");
        let reader = tokio::task::spawn_blocking(move || {
            let _span = span.entered();
            let scanned = table.scan(|batch| {
                // `false` once the consumer is gone: stop reading.
                let rows = tracing::info_span!("to_rows").in_scope(|| batch_rows(&batch));
                tracing::trace_span!("wait_for_consumer")
                    .in_scope(|| tx.blocking_send(rows.map(Rows::from)).is_ok())
            });
            if let Err(e) = scanned {
                let _ = tx.blocking_send(Err(e.into()));
            }
        });
        self.state = State::Reading { rx, reader: Some(reader) };
    }
}

/// For use with stream combinators. Unlike [`RowStream::next`], a reader
/// panic just ends the stream.
impl Stream for RowStream {
    type Item = Result<Rows>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.start();
        match &mut self.state {
            State::Reading { rx, .. } => rx.poll_recv(cx),
            State::NotStarted(_) => unreachable!("started above"),
        }
    }
}
