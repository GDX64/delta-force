//! Reading CSV files into a table's columns.
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::{Buf, Bytes};
use arrow_schema::{Field, Schema};
use delta_kernel::arrow::csv::ReaderBuilder;
use delta_kernel::arrow::csv::reader::Format;
use futures_util::stream::{self, StreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

use super::Batches;
use super::columns::{assemble, match_columns};
use crate::file::LakeFile;
use crate::lake::Column;
use crate::{Error, Result};

/// Chunks in flight between the download and the parser.
const IN_FLIGHT: usize = 8;
/// Parsed batches waiting to be taken.
const BATCHES_IN_FLIGHT: usize = 2;

/// Read `file` as CSV with a header row, as a stream of batches in the
/// table's columns. The file is downloaded and parsed chunk by chunk, never
/// held whole.
#[tracing::instrument(name = "open_csv", skip_all, fields(path = file.path()))]
pub(super) async fn read(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Batches> {
    let mut download = file.chunks().await?;
    let (send_bytes, mut receive_bytes) = mpsc::channel::<Bytes>(IN_FLIGHT);
    let (send_batch, receive_batch) = mpsc::channel::<Result<RecordBatch>>(BATCHES_IN_FLIGHT);

    // Parsing is CPU work: run it on a blocking thread while we download.
    let (columns, path, parsed) = (columns.clone(), file.path().to_owned(), send_batch.clone());
    let span = tracing::info_span!("parse_csv", path = %path);
    tokio::task::spawn_blocking(move || {
        let _span = span.entered();
        let chunks = std::iter::from_fn(|| {
            tracing::trace_span!("wait_for_chunk").in_scope(|| receive_bytes.blocking_recv())
        });
        // A send fails when the reader stopped listening: stop parsing.
        let emit = |batch: RecordBatch| {
            tracing::trace_span!("wait_to_hand_off", rows = batch.num_rows())
                .in_scope(|| parsed.blocking_send(Ok(batch)).is_ok())
        };
        if let Err(message) = parse(chunks, &columns, emit) {
            let _ = parsed.blocking_send(Err(Error::InvalidFile { file: path, message }));
        }
    });
    let span = tracing::info_span!("download_csv", path = file.path());
    let download = async move {
        loop {
            let next = download.next().instrument(tracing::trace_span!("get_chunk")).await;
            let Some(chunk) = next else { break };
            let sent = match chunk {
                Ok(chunk) => send_bytes
                    .send(chunk)
                    .instrument(tracing::trace_span!("wait_for_parser"))
                    .await
                    .is_ok(),
                Err(e) => {
                    let _ = send_batch.send(Err(e)).await;
                    false
                }
            };
            // Stop on a download error, or once the parser has stopped.
            if !sent {
                return;
            }
        }
    };
    tokio::spawn(download.instrument(span));

    let batches = stream::unfold(receive_batch, |mut receive| async move {
        receive.recv().await.map(|batch| (batch, receive))
    });
    Ok(batches.boxed())
}

/// Parse CSV arriving in `chunks`, split anywhere, including mid-row.
/// Each batch goes to `emit`; parsing stops early if it returns `false`.
fn parse(
    chunks: impl IntoIterator<Item = Bytes>,
    columns: &[Column],
    mut emit: impl FnMut(RecordBatch) -> bool,
) -> Result<(), String> {
    let mut chunks = chunks.into_iter();
    let format = Format::default().with_header(true);

    // Buffer just enough to read the header line.
    let mut head = Vec::new();
    for chunk in chunks.by_ref() {
        head.extend_from_slice(&chunk);
        if chunk.contains(&b'\n') {
            break;
        }
    }
    let (header, _) = format
        .infer_schema(Cursor::new(&head), Some(0))
        .map_err(|e| e.to_string())?;
    let names = header.fields().iter().map(|f| f.name().as_str());
    let source = match_columns(names, columns)?;

    // Parse each CSV column straight into its table column's type.
    let mut fields = vec![None; header.fields().len()];
    for (column, source) in columns.iter().zip(&source) {
        if let Some(i) = *source {
            let name = header.field(i).name();
            fields[i] = Some(Field::new(name, column.data_type.to_arrow(), true));
        }
    }
    let schema = Schema::new(fields.into_iter().flatten().collect::<Vec<_>>());

    // The decoder skips the header line itself, so feed it from the start.
    let mut decoder = ReaderBuilder::new(Arc::new(schema)).with_format(format).build_decoder();
    let mut chunks = std::iter::once(Bytes::from(head)).chain(chunks.filter(|c| !c.is_empty()));
    let mut buffer = Bytes::new();
    loop {
        if buffer.is_empty() {
            // An empty buffer at the end tells the decoder the input is over.
            buffer = chunks.next().unwrap_or_default();
        }
        let decoded = tracing::trace_span!("decode", bytes = buffer.len())
            .in_scope(|| decoder.decode(&buffer))
            .map_err(|e| e.to_string())?;
        buffer.advance(decoded);
        // 0 means a full batch or the end of the input.
        if decoded == 0 || decoder.capacity() == 0 {
            let flushed = tracing::trace_span!("build_batch").in_scope(|| {
                let Some(batch) = decoder.flush()? else { return Ok(None) };
                let take = |i: usize| Ok(batch.column(i).clone());
                Ok(Some(assemble(columns, &source, take, batch.num_rows())))
            });
            match flushed.map_err(|e: delta_kernel::arrow::error::ArrowError| e.to_string())? {
                Some(batch) => {
                    if !emit(batch?) {
                        return Ok(());
                    }
                }
                None if buffer.is_empty() => break,
                None => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataType;
    use arrow_array::Int64Array;

    fn columns() -> Vec<Column> {
        vec![
            Column { name: "id".into(), data_type: DataType::Long, nullable: false },
            Column { name: "score".into(), data_type: DataType::Double, nullable: true },
        ]
    }

    /// Parse `csv` arriving as a single chunk.
    fn parse_one(csv: &'static str) -> Result<Vec<RecordBatch>, String> {
        parse_all([Bytes::from(csv)])
    }

    fn parse_all(chunks: impl IntoIterator<Item = Bytes>) -> Result<Vec<RecordBatch>, String> {
        let mut batches = Vec::new();
        parse(chunks, &columns(), |batch| {
            batches.push(batch);
            true
        })?;
        Ok(batches)
    }

    fn error(csv: &'static str) -> String {
        parse_one(csv).unwrap_err()
    }

    #[test]
    fn header_names_ignore_case_and_order() {
        let batches = parse_one("SCORE,Id\n0.5,7\n").unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[test]
    fn chunks_can_split_rows_anywhere() {
        // One byte per chunk, and no newline after the last row.
        let csv = "id,score\n1,0.5\n22,\n333,1.25";
        let bytes = csv.bytes().map(|b| Bytes::from(vec![b]));
        let batches = parse_all(bytes).unwrap();
        assert_eq!(batches.len(), 1);
        let id = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 22, 333]);
        assert_eq!(batches[0].column(1).null_count(), 1);
    }

    #[test]
    fn missing_nullable_column_is_null() {
        let batches = parse_one("id\n1\n2\n").unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        let full = super::super::columns::complete(&columns(), &batches[0]).unwrap();
        assert_eq!(full.column(1).null_count(), 2);
    }

    #[test]
    fn rejects_files_that_do_not_fit() {
        assert!(error("id,extra\n1,x\n").contains("\"extra\" is not in the table"));
        assert!(error("score\n0.5\n").contains("required column \"id\" is missing"));
        assert!(error("id,ID\n1,2\n").contains("appears twice"));
        assert!(error("id,score\n,0.5\n").contains("required column \"id\" has empty values"));
        assert!(error("id\nabc\n").contains("abc"));
    }
}
