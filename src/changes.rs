//! Keyed changes applied to a table: [`Change`], [`ChangesConfig`].
use std::collections::HashMap;

use crate::{Error, Result, Row, Schema, Value};

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

/// What a [`ChangesBatch`](crate::ChangesBatch) holds and how to apply it: the table's schema,
/// the key column and, for change files, the operation column.
#[derive(Debug, Clone)]
pub struct ChangesConfig {
    schema: Schema,
    key: Option<String>,
    operation: Option<String>,
    mode: ApplyMode,
}

impl ChangesConfig {
    /// Changes to a table with `schema`, from [`Table::schema`](crate::Table::schema).
    pub fn new(schema: Schema) -> Self {
        Self { schema, key: None, operation: None, mode: ApplyMode::default() }
    }

    /// The column that identifies a row.
    pub fn key(mut self, column: impl Into<String>) -> Self {
        self.key = Some(column.into());
        self
    }

    /// The column of a change file that says what each row does: `insert`,
    /// `update` or `delete` (or `I`, `U`, `D`). Needed to apply files pushed
    /// with [`ChangesBatch::push_file`](crate::ChangesBatch::push_file); it is not a table column.
    pub fn operation(mut self, column: impl Into<String>) -> Self {
        self.operation = Some(column.into());
        self
    }

    pub fn mode(mut self, mode: ApplyMode) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn operation_column(&self) -> Option<&str> {
        self.operation.as_deref()
    }

    /// The key column, which must be one of the schema's.
    pub(crate) fn key_column(&self) -> Result<&str> {
        let key = self
            .key
            .as_deref()
            .ok_or_else(|| Error::InvalidConfig("changes need a key column".into()))?;
        if !self.schema.column_names().any(|c| c == key) {
            return Err(Error::ColumnNotFound(key.to_owned()));
        }
        Ok(key)
    }
}

/// A key value that can be hashed (doubles and nulls are not valid keys).
#[derive(PartialEq, Eq, Hash)]
enum Key {
    Boolean(bool),
    Integer(i64),
    String(String),
}

/// Fails unless `row` has a usable key in `column`.
pub(crate) fn check_key(row: &Row, column: &str) -> Result<()> {
    key_of(row, column).map(|_| ())
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

/// Where an existing row lives: data file index and row position in it.
pub(crate) type Location = (usize, u64);

/// The outcome of applying changes: existing rows to delete, new rows to add.
#[derive(Debug, Default)]
pub(crate) struct Plan {
    pub delete: Vec<Location>,
    pub append: Vec<Row>,
}

/// The state of one key while a batch is applied.
struct Entry {
    /// The row already in the table, if any.
    origin: Option<Location>,
    /// The key's current value (`None` = deleted).
    row: Option<Row>,
    /// Whether `row` differs from what is stored at `origin`.
    changed: bool,
}

/// Applies changes in order, by key, to the table's live rows. Only the
/// rows that change end up in the plan; everything else is untouched.
pub(crate) struct Planner<'a> {
    key: &'a str,
    entries: HashMap<Key, Entry>,
}

impl<'a> Planner<'a> {
    pub fn new(existing: impl IntoIterator<Item = (Location, Row)>, key: &'a str) -> Result<Self> {
        let mut entries: HashMap<Key, Entry> = HashMap::new();
        for (location, row) in existing {
            let (k, text) = (key_of(&row, key)?, key_text(&row, key));
            let entry = Entry { origin: Some(location), row: Some(row), changed: false };
            if entries.insert(k, entry).is_some() {
                return Err(Error::DuplicateKey(text));
            }
        }
        Ok(Self { key, entries })
    }

    pub fn apply(&mut self, change: Change) -> Result<()> {
        let key = self.key;
        let k = key_of(change.row(), key)?;
        match change {
            Change::Insert(row) => {
                let entry = self
                    .entries
                    .entry(k)
                    .or_insert(Entry { origin: None, row: None, changed: false });
                if entry.row.is_some() {
                    return Err(Error::DuplicateKey(key_text(&row, key)));
                }
                entry.row = Some(row);
                entry.changed = true;
            }
            Change::Update(row) => {
                let current = self
                    .entries
                    .get_mut(&k)
                    .and_then(|e| e.row.as_mut().map(|r| (r, &mut e.changed)));
                let (current, changed) =
                    current.ok_or_else(|| Error::KeyNotFound(key_text(&row, key)))?;
                for (column, value) in row.columns() {
                    current.set(column, value.clone());
                }
                *changed = true;
            }
            Change::Delete(_) => {
                if let Some(entry) = self.entries.get_mut(&k) {
                    entry.row = None;
                }
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Plan {
        let mut plan = Plan::default();
        for entry in self.entries.into_values() {
            let replaced = entry.row.is_none() || entry.changed;
            if let (Some(location), true) = (entry.origin, replaced) {
                plan.delete.push(location);
            }
            if let (Some(row), true) = (entry.row, entry.changed) {
                plan.append.push(row);
            }
        }
        plan
    }
}
