use anyhow::{Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnSchema {
    pub name: String,
    pub unsigned: bool,
    pub binary: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CdcEvent {
    pub event_id: String,
    pub source: SourceIdentity,
    pub operation: Operation,
    pub timestamp_unix_seconds: u32,
    pub transaction_id: Option<String>,
    pub position: EventPosition,
    pub row_ordinal: usize,
    pub before: Option<Map<String, Value>>,
    pub after: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceIdentity {
    pub database: String,
    pub table: String,
    pub server_id: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventPosition {
    pub binlog_file: String,
    pub binlog_position: u32,
}

pub fn row(
    values: &[ColumnValue],
    included: &[bool],
    schema: &[ColumnSchema],
) -> Result<Map<String, Value>> {
    if values.len() != schema.len() || included.len() != schema.len() {
        bail!("row image width does not match resolved table schema");
    }
    let mut object = Map::new();
    for ((value, present), column) in values.iter().zip(included).zip(schema) {
        if *present {
            object.insert(
                column.name.clone(),
                value_json(value, column.unsigned, column.binary)?,
            );
        }
    }
    Ok(object)
}

fn value_json(value: &ColumnValue, unsigned: bool, binary: bool) -> Result<Value> {
    let value = match value {
        ColumnValue::None => Value::Null,
        ColumnValue::Tiny(n) if unsigned => json!(*n as u8),
        ColumnValue::Short(n) if unsigned => json!(*n as u16),
        ColumnValue::Long(n) if unsigned => json!(*n as u32),
        ColumnValue::LongLong(n) if unsigned => json!(*n as u64),
        ColumnValue::Tiny(n) => json!(n),
        ColumnValue::Short(n) => json!(n),
        ColumnValue::Long(n) => json!(n),
        ColumnValue::LongLong(n) => json!(n),
        ColumnValue::Float(n) if n.is_finite() => json!(n),
        ColumnValue::Double(n) if n.is_finite() => json!(n),
        ColumnValue::Float(_) | ColumnValue::Double(_) => {
            bail!("non-finite floating point value is not valid JSON")
        }
        ColumnValue::Decimal(v)
        | ColumnValue::Time(v)
        | ColumnValue::Date(v)
        | ColumnValue::DateTime(v) => json!(v),
        ColumnValue::Timestamp(micros) => json!({"$timestamp_micros": micros}),
        ColumnValue::Year(v) => json!(v),
        ColumnValue::String(bytes) if binary => json!({"$base64": STANDARD.encode(bytes)}),
        ColumnValue::String(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => json!(s),
            Err(_) => json!({"$base64": STANDARD.encode(bytes)}),
        },
        ColumnValue::Blob(bytes) | ColumnValue::Json(bytes) => {
            json!({"$base64": STANDARD.encode(bytes)})
        }
        ColumnValue::Bit(v) | ColumnValue::Set(v) => json!(v),
        ColumnValue::Enum(v) => json!(v),
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_bigint_keeps_full_u64_range() {
        let got = value_json(&ColumnValue::LongLong(-1), true, false).unwrap();
        assert_eq!(got, json!(u64::MAX));
    }

    #[test]
    fn invalid_text_and_binary_are_explicitly_base64_encoded() {
        assert_eq!(
            value_json(&ColumnValue::String(vec![0xff]), false, false).unwrap(),
            json!({"$base64":"/w=="})
        );
    }

    #[test]
    fn binary_text_column_is_never_mistaken_for_utf8() {
        assert_eq!(
            value_json(&ColumnValue::String(b"valid utf8".to_vec()), false, true).unwrap(),
            json!({"$base64":"dmFsaWQgdXRmOA=="})
        );
    }
}
