//! Dynamic rows: [`Value`], [`Row`], [`Rows`] and the [`row!`](crate::row) macro.
use std::cmp::Ordering;
use std::fmt;

use crate::{Error, Result};

/// A single cell value.
#[derive(Clone)]
#[non_exhaustive]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i32),
    Long(i64),
    Double(f64),
    String(String),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Short name of the value's type, used in error messages.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Boolean(_) => "boolean",
            Value::Integer(_) => "integer",
            Value::Long(_) => "long",
            Value::Double(_) => "double",
            Value::String(_) => "string",
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(i64::from(*v)),
            Value::Long(v) => Some(*v),
            _ => None,
        }
    }

    /// Order used by [`Rows::sorted_by`]: nulls first, then by value.
    fn sort_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Double(a), Value::Double(b)) => a.total_cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            _ => match (self.as_i64(), other.as_i64()) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => self.rank().cmp(&other.rank()),
            },
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Integer(_) | Value::Long(_) => 2,
            Value::Double(_) => 3,
            Value::String(_) => 4,
        }
    }
}

/// Integers compare by value regardless of width, so `row! { "id" => 1 }`
/// equals a row read back from a `long` column.
impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            _ => matches!((self.as_i64(), other.as_i64()), (Some(a), Some(b)) if a == b),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("null"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Long(v) => write!(f, "{v}"),
            Value::Double(v) => write!(f, "{v:?}"),
            Value::String(v) => write!(f, "{v:?}"),
        }
    }
}

macro_rules! value_from {
    ($($t:ty => $variant:ident),* $(,)?) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::$variant(v.into())
            }
        }
    )*};
}
value_from!(bool => Boolean, i32 => Integer, i64 => Long, f64 => Double, String => String, &str => String);

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map_or(Value::Null, Into::into)
    }
}

/// Types a [`Value`] can be read as, via [`Row::get`].
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Option<Self>;
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64().and_then(|v| i32::try_from(v).ok())
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64()
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Double(v) => Some(*v),
            _ => None,
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(None),
            v => T::from_value(v).map(Some),
        }
    }
}

/// One row: column names mapped to values. Build one with [`row!`](crate::row).
#[derive(Clone, Default)]
pub struct Row {
    values: Vec<(String, Value)>,
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set `column` to `value`, replacing any previous value.
    pub fn set(&mut self, column: impl Into<String>, value: impl Into<Value>) {
        let (column, value) = (column.into(), value.into());
        match self.values.iter_mut().find(|(c, _)| *c == column) {
            Some((_, v)) => *v = value,
            None => self.values.push((column, value)),
        }
    }

    /// Builder form of [`Row::set`].
    pub fn with(mut self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.set(column, value);
        self
    }

    /// The raw value of `column`, if the row has it.
    pub fn value(&self, column: &str) -> Option<&Value> {
        self.values.iter().find(|(c, _)| c == column).map(|(_, v)| v)
    }

    /// The value of `column` as `T`. Use `Option<T>` for nullable columns.
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T> {
        let value = self
            .value(column)
            .ok_or_else(|| Error::ColumnNotFound(column.to_owned()))?;
        T::from_value(value).ok_or_else(|| Error::TypeMismatch {
            column: column.to_owned(),
            expected: std::any::type_name::<T>().to_owned(),
            found: value.kind().to_owned(),
        })
    }

    /// `(column, value)` pairs in insertion order.
    pub fn columns(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.values.iter().map(|(c, v)| (c.as_str(), v))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Rows are equal when they have the same columns with equal values, in any order.
impl PartialEq for Row {
    fn eq(&self, other: &Row) -> bool {
        self.len() == other.len() && self.columns().all(|(c, v)| other.value(c) == Some(v))
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, (column, value)) in self.columns().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{column}: {value:?}")?;
        }
        f.write_str("}")
    }
}

/// Rows read from a table.
#[derive(Clone, Default, PartialEq)]
pub struct Rows(Vec<Row>);

impl Rows {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Row> {
        self.0.iter()
    }

    pub fn to_vec(&self) -> Vec<Row> {
        self.0.clone()
    }

    /// Sort by `column` (nulls and missing values first). Delta doesn't
    /// guarantee read order, so sort before comparing.
    pub fn sorted_by(mut self, column: &str) -> Self {
        let null = Value::Null;
        self.0.sort_by(|a, b| {
            let a = a.value(column).unwrap_or(&null);
            let b = b.value(column).unwrap_or(&null);
            a.sort_cmp(b)
        });
        self
    }
}

impl From<Vec<Row>> for Rows {
    fn from(rows: Vec<Row>) -> Self {
        Rows(rows)
    }
}

impl FromIterator<Row> for Rows {
    fn from_iter<I: IntoIterator<Item = Row>>(iter: I) -> Self {
        Rows(iter.into_iter().collect())
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<const N: usize> PartialEq<[Row; N]> for Rows {
    fn eq(&self, other: &[Row; N]) -> bool {
        self.0 == other
    }
}

impl PartialEq<[Row]> for Rows {
    fn eq(&self, other: &[Row]) -> bool {
        self.0 == other
    }
}

impl PartialEq<Vec<Row>> for Rows {
    fn eq(&self, other: &Vec<Row>) -> bool {
        &self.0 == other
    }
}

impl fmt::Debug for Rows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(&self.0).finish()
    }
}

/// Build a [`Row`]: `row! { "id" => 1, "name" => "a", "score" => null }`.
///
/// `null` is a null value; leaving a nullable column out also writes null.
#[macro_export]
macro_rules! row {
    () => { $crate::Row::new() };
    ($($body:tt)+) => { $crate::__row!(@acc [] $($body)+) };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __row {
    (@acc [$($out:tt)*]) => { $crate::Row::new() $($out)* };
    (@acc [$($out:tt)*] $k:expr => null $(, $($rest:tt)*)?) => {
        $crate::__row!(@acc [$($out)* .with($k, $crate::Value::Null)] $($($rest)*)?)
    };
    (@acc [$($out:tt)*] $k:expr => $v:expr $(, $($rest:tt)*)?) => {
        $crate::__row!(@acc [$($out)* .with($k, $v)] $($($rest)*)?)
    };
}
