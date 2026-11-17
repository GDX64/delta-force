use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong in `delta-force`.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    TableExists(String),
    TableNotFound(String),
    InvalidTableName(String),
    InvalidSchema(String),
    ColumnNotFound(String),
    /// A non-nullable column was missing or null.
    MissingValue(String),
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },
    /// A batch built from a schema that isn't the table's current one.
    SchemaMismatch,
    /// `write` was called with no rows.
    NoRows,
    InvalidConfig(String),
    /// A lake file that doesn't exist.
    FileNotFound(String),
    /// A lake file whose format can't be read (by extension).
    UnsupportedFormat(String),
    /// A lake file whose contents don't fit the table.
    InvalidFile { file: String, message: String },
    /// An insert used a key that already exists (or the table has duplicates).
    DuplicateKey(String),
    /// An update used a key that doesn't exist.
    KeyNotFound(String),
    /// Storage or Delta protocol failure.
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TableExists(t) => write!(f, "table {t:?} already exists"),
            Error::TableNotFound(t) => write!(f, "table {t:?} not found"),
            Error::InvalidTableName(t) => write!(f, "invalid table name {t:?}"),
            Error::InvalidSchema(m) => write!(f, "invalid schema: {m}"),
            Error::ColumnNotFound(c) => write!(f, "column {c:?} not found"),
            Error::MissingValue(c) => write!(f, "column {c:?} is not nullable but has no value"),
            Error::TypeMismatch { column, expected, found } => {
                write!(f, "column {column:?}: expected {expected}, found {found}")
            }
            Error::SchemaMismatch => {
                f.write_str("the batch was built for a different schema than the table's")
            }
            Error::NoRows => f.write_str("no rows to write"),
            Error::InvalidConfig(m) => write!(f, "invalid config: {m}"),
            Error::FileNotFound(p) => write!(f, "file {p:?} not found"),
            Error::UnsupportedFormat(p) => write!(f, "unsupported file format: {p:?}"),
            Error::InvalidFile { file, message } => write!(f, "{file}: {message}"),
            Error::DuplicateKey(k) => write!(f, "duplicate key {k}"),
            Error::KeyNotFound(k) => write!(f, "key {k} not found"),
            Error::Storage(e) => write!(f, "storage error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Storage(e.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Storage(e.into())
    }
}
