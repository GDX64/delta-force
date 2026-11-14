//! Keyed changes applied to a table: [`Change`], [`Changes`], [`ChangesConfig`].
use std::collections::HashMap;

use futures_util::stream::{self, BoxStream, StreamExt};

use crate::{Error, Result, Row, Value};

/// One change to a row, identified by the key column in [`ChangesConfig`].
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Add a new row. Strict mode: fails if the key already exists.
    Insert(Row),
    /// Set the given columns on an existing row; other columns keep their
    /// values. Strict mode: fails if the key doesn't exist.
    Update(Row),
    /// Remove a row. Only the key column is needed. A missing key is a no-op.
    Delete(Row),
}

impl Change {
    pub fn insert(row: Row) -> Self {
        Change::Insert(row)
    }

    pub fn update(row: Row) -> Self {
        Change::Update(row)
    }

    pub fn delete(row: Row) -> Self {
        Change::Delete(row)
    }

    fn row(&self) -> &Row {
        match self {
            Change::Insert(r) | Change::Update(r) | Change::Delete(r) => r,
        }
    }
}

/// How changes that don't match the table are handled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApplyMode {
    /// Insert of an existing key or update of a missing key is an error.
    #[default]
    Strict,
}

/// How to interpret a stream of [`Change`]s.
#[derive(Debug, Clone, Default)]
pub struct ChangesConfig {
    key: Option<String>,
    mode: ApplyMode,
}

impl ChangesConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// The column that identifies a row.
    pub fn key(mut self, column: impl Into<String>) -> Self {
        self.key = Some(column.into());
        self
    }

    pub fn mode(mut self, mode: ApplyMode) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn key_column(&self) -> Result<&str> {
        self.key
            .as_deref()
            .ok_or_else(|| Error::InvalidConfig("changes need a key column".into()))
    }
}

/// A stream of changes plus the config that says how to apply them.
pub struct Changes {
    config: ChangesConfig,
    stream: BoxStream<'static, Result<Change>>,
}

impl Changes {
    /// Changes held in memory.
    pub fn in_memory(config: ChangesConfig, changes: impl IntoIterator<Item = Change>) -> Self {
        let changes: Vec<Change> = changes.into_iter().collect();
        Self {
            config,
            stream: stream::iter(changes.into_iter().map(Ok)).boxed(),
        }
    }

    pub fn config(&self) -> &ChangesConfig {
        &self.config
    }

    /// The changes as an async stream.
    pub fn into_stream(self) -> BoxStream<'static, Result<Change>> {
        self.stream
    }
}

/// A key value that can be hashed (doubles and nulls are not valid keys).
#[derive(PartialEq, Eq, Hash)]
enum Key {
    Boolean(bool),
    Integer(i64),
    String(String),
}

fn key_of(row: &Row, column: &str) -> Result<Key> {
    match row.value(column) {
        None | Some(Value::Null) => Err(Error::MissingValue(column.to_owned())),
        Some(Value::Boolean(b)) => Ok(Key::Boolean(*b)),
        Some(Value::String(s)) => Ok(Key::String(s.clone())),
        Some(v) => v.as_i64().map(Key::Integer).ok_or_else(|| {
            Error::InvalidConfig(format!("key column {column:?} cannot hold {}", v.kind()))
        }),
    }
}

fn key_text(row: &Row, column: &str) -> String {
    row.value(column).map_or_else(|| "null".into(), |v| format!("{v:?}"))
}

/// Apply `changes` to `rows` in order, by `key`. Returns the new rows.
pub(crate) fn apply_to_rows(rows: Vec<Row>, changes: &[Change], key: &str) -> Result<Vec<Row>> {
    let mut by_key: HashMap<Key, Row> = HashMap::with_capacity(rows.len());
    for row in rows {
        let text = key_text(&row, key);
        if by_key.insert(key_of(&row, key)?, row).is_some() {
            return Err(Error::DuplicateKey(text));
        }
    }

    for change in changes {
        let k = key_of(change.row(), key)?;
        match change {
            Change::Insert(row) => {
                if by_key.contains_key(&k) {
                    return Err(Error::DuplicateKey(key_text(row, key)));
                }
                by_key.insert(k, row.clone());
            }
            Change::Update(row) => {
                let existing = by_key
                    .get_mut(&k)
                    .ok_or_else(|| Error::KeyNotFound(key_text(row, key)))?;
                for (column, value) in row.columns() {
                    existing.set(column, value.clone());
                }
            }
            Change::Delete(_) => {
                by_key.remove(&k);
            }
        }
    }
    Ok(by_key.into_values().collect())
}
