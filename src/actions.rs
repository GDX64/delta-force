use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Action {
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "remove")]
    Remove(Remove),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub min_reader_version: u64,
    pub min_writer_version: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    pub schema_string: String,
}

impl Metadata {
    /// Render an Arrow [`Schema`] as a Delta `schemaString`: a JSON struct type
    /// such as `{"type":"struct","fields":[{"name":"a","type":"long","nullable":true,"metadata":{}}]}`.
    ///
    /// Arrow types with no Delta equivalent fall back to their Arrow display name.
    pub fn schema_string(schema: &Schema) -> String {
        struct_type(schema.fields().iter()).to_string()
    }
}

fn struct_type<'a>(fields: impl Iterator<Item = &'a std::sync::Arc<Field>>) -> serde_json::Value {
    let fields: Vec<_> = fields.map(|f| field_json(f)).collect();
    serde_json::json!({ "type": "struct", "fields": fields })
}

fn field_json(field: &Field) -> serde_json::Value {
    let metadata: serde_json::Map<String, serde_json::Value> = field
        .metadata()
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::json!({
        "name": field.name(),
        "type": type_json(field.data_type()),
        "nullable": field.is_nullable(),
        "metadata": metadata,
    })
}

fn type_json(data_type: &DataType) -> serde_json::Value {
    use serde_json::json;
    let primitive = match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::UInt8 => "byte",
        DataType::Int16 | DataType::UInt16 => "short",
        DataType::Int32 | DataType::UInt32 => "integer",
        DataType::Int64 | DataType::UInt64 => "long",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, None) => "timestamp_ntz",
        DataType::Timestamp(_, Some(_)) => "timestamp",
        DataType::Decimal128(p, s) => return json!(format!("decimal({p},{s})")),
        DataType::Decimal256(p, s) => return json!(format!("decimal({p},{s})")),
        DataType::Struct(fields) => return struct_type(fields.iter()),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            return json!({
                "type": "array",
                "elementType": type_json(f.data_type()),
                "containsNull": f.is_nullable(),
            });
        }
        DataType::Map(entries, _) => {
            if let DataType::Struct(kv) = entries.data_type()
                && let [k, v] = kv.iter().collect::<Vec<_>>()[..]
            {
                return json!({
                    "type": "map",
                    "keyType": type_json(k.data_type()),
                    "valueType": type_json(v.data_type()),
                    "valueContainsNull": v.is_nullable(),
                });
            }
            return json!(data_type.to_string());
        }
        other => return json!(other.to_string()),
    };
    json!(primitive)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    pub path: String,
    pub size: u64,
    pub data_change: bool,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    pub path: String,
    pub data_change: bool,
}
