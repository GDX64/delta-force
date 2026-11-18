//! Reading CSV files into a table's columns.
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use bytes::{Buf, Bytes};
use delta_kernel::arrow::csv::ReaderBuilder;
use delta_kernel::arrow::csv::reader::{Decoder, Format};
use futures_util::stream::{self, BoxStream, StreamExt};
use tracing::Instrument;

use super::Batches;
use super::columns::{assemble, match_columns};
use crate::file::LakeFile;
use crate::lake::Column;
use crate::{Error, Result};

/// Read `file` as CSV with a header row, as a stream of batches in the
/// table's columns. Each chunk is downloaded and parsed as the stream is
/// pulled, so the file is never held whole.
#[tracing::instrument(name = "open_csv", skip_all, fields(path = file.path()))]
pub(super) async fn read(file: &LakeFile, columns: &Arc<[Column]>) -> Result<Batches> {
    let state = Reading {
        download: file.chunks().await?,
        parser: Some(Parser::new(columns.clone())),
        parsed: VecDeque::new(),
        path: file.path().to_owned(),
    };
    // Stops after the first error.
    let batches = stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(batch) = state.parsed.pop_front() {
                return Ok(Some((batch, state)));
            }
            let Some(parser) = state.parser.as_mut() else {
                return Ok(None);
            };
            let chunk = state
                .download
                .next()
                .instrument(tracing::trace_span!("get_chunk"))
                .await;
            let parsed = match chunk {
                Some(chunk) => parser.push(chunk?, &mut state.parsed),
                None => {
                    let parsed = parser.finish(&mut state.parsed);
                    state.parser = None;
                    parsed
                }
            };
            parsed.map_err(|message| Error::InvalidFile {
                file: state.path.clone(),
                message,
            })?;
        }
    });
    Ok(batches.boxed())
}

/// The state of [`read`] between batches.
struct Reading {
    download: BoxStream<'static, Result<Bytes>>,
    /// `None` once the download has ended.
    parser: Option<Parser>,
    /// Batches parsed but not yet taken.
    parsed: VecDeque<RecordBatch>,
    path: String,
}

/// Parses CSV pushed in chunks, split anywhere, including mid-row.
struct Parser {
    columns: Arc<[Column]>,
    state: State,
}

enum State {
    /// Buffering just enough to read the header line.
    Header(Vec<u8>),
    Rows {
        decoder: Box<Decoder>,
        /// The source column of each table column.
        source: Vec<Option<usize>>,
    },
}

impl Parser {
    fn new(columns: Arc<[Column]>) -> Self {
        Self {
            columns,
            state: State::Header(Vec::new()),
        }
    }

    /// Parse `chunk`, adding each completed batch to `out`.
    fn push(&mut self, chunk: Bytes, out: &mut VecDeque<RecordBatch>) -> Result<(), String> {
        let chunk = match &mut self.state {
            State::Header(head) => {
                head.extend_from_slice(&chunk);
                if !chunk.contains(&b'\n') {
                    return Ok(());
                }
                // The decoder skips the header line itself: feed it from the start.
                let head = Bytes::from(std::mem::take(head));
                self.start(&head)?;
                head
            }
            State::Rows { .. } => chunk,
        };
        self.decode(chunk, out)
    }

    /// The input is over: parse what is left.
    fn finish(&mut self, out: &mut VecDeque<RecordBatch>) -> Result<(), String> {
        if let State::Header(head) = &mut self.state {
            let head = Bytes::from(std::mem::take(head));
            self.start(&head)?;
            self.decode(head, out)?;
        }
        // Empty input tells the decoder the input is over (the last row may
        // have no newline).
        self.decode(Bytes::new(), out)?;
        self.flush(out)
    }

    /// Read the header in `head` and set up the decoder.
    fn start(&mut self, head: &[u8]) -> Result<(), String> {
        let format = Format::default().with_header(true);
        let (header, _) = format
            .infer_schema(Cursor::new(head), Some(0))
            .map_err(|e| e.to_string())?;
        let names = header.fields().iter().map(|f| f.name().as_str());
        let source = match_columns(names, &self.columns)?;

        // Parse each CSV column straight into its table column's type.
        let mut fields = vec![None; header.fields().len()];
        for (column, source) in self.columns.iter().zip(&source) {
            if let Some(i) = *source {
                let name = header.field(i).name();
                fields[i] = Some(Field::new(name, column.data_type.to_arrow(), true));
            }
        }
        let schema = Schema::new(fields.into_iter().flatten().collect::<Vec<_>>());
        let decoder = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .build_decoder();
        self.state = State::Rows {
            decoder: Box::new(decoder),
            source,
        };
        Ok(())
    }

    /// Decode all of `chunk`, flushing each batch as it fills up. The
    /// decoder takes input while it has room, so each pass makes progress.
    #[tracing::instrument(skip_all, fields(bytes = chunk.len()))]
    fn decode(&mut self, mut chunk: Bytes, out: &mut VecDeque<RecordBatch>) -> Result<(), String> {
        loop {
            let State::Rows { decoder, .. } = &mut self.state else {
                unreachable!("decoding starts after the header")
            };
            let decoded = decoder.decode(&chunk).map_err(|e| e.to_string())?;
            chunk.advance(decoded);
            if decoder.capacity() == 0 {
                self.flush(out)?;
            }
            if chunk.is_empty() {
                return Ok(());
            }
        }
    }

    /// Turn the rows decoded so far into a batch in the table's columns.
    fn flush(&mut self, out: &mut VecDeque<RecordBatch>) -> Result<(), String> {
        let State::Rows { decoder, source } = &mut self.state else {
            unreachable!("decoding starts after the header")
        };
        if let Some(batch) = decoder.flush().map_err(|e| e.to_string())? {
            let take = |i: usize| Ok(batch.column(i).clone());
            out.push_back(assemble(&self.columns, source, take, batch.num_rows())?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataType;
    use arrow_array::Int64Array;

    fn columns() -> Vec<Column> {
        vec![
            Column {
                name: "id".into(),
                data_type: DataType::Long,
                nullable: false,
            },
            Column {
                name: "score".into(),
                data_type: DataType::Double,
                nullable: true,
            },
        ]
    }

    /// Parse `csv` arriving as a single chunk.
    fn parse_one(csv: &'static str) -> Result<Vec<RecordBatch>, String> {
        parse_all([Bytes::from(csv)])
    }

    fn parse_all(chunks: impl IntoIterator<Item = Bytes>) -> Result<Vec<RecordBatch>, String> {
        let mut parser = Parser::new(columns().into());
        let mut batches = VecDeque::new();
        for chunk in chunks {
            parser.push(chunk, &mut batches)?;
        }
        parser.finish(&mut batches)?;
        Ok(batches.into())
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
        let id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
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
