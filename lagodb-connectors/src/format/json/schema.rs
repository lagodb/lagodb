//! NDJSON schema inference.

use std::collections::HashMap;
use std::io::BufRead;
use std::num::NonZeroUsize;

use pgrx::pg_sys;
use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, Visitor};
use serde_json::Value;

use crate::error::ConnectorError;

use super::stream::JsonLineReader;
use crate::format::{
    FormatKind, InferredColumn, InferredSchema, PostgresType, SCHEMA_SAMPLE_RECORDS,
};

#[derive(Default)]
pub(super) struct JsonSchemaAccumulator {
    fields: Vec<JsonField>,
    indexes: HashMap<String, usize>,
}

impl JsonSchemaAccumulator {
    pub(super) fn read(
        mut self,
        input: impl BufRead,
        max_record_bytes: NonZeroUsize,
    ) -> Result<InferredSchema, ConnectorError> {
        let mut input = JsonLineReader::new(input, max_record_bytes);
        for _ in 0..SCHEMA_SAMPLE_RECORDS {
            if !input.read_next()? {
                break;
            }
            let logical_line = input.logical_line();
            let object = serde_json::from_slice::<JsonObject>(input.record())
                .map_err(|source| ConnectorError::Json {
                    line: logical_line,
                    source,
                })?;
            for (name, value_type) in object.into_last_values() {
                if let Some(index) = self.indexes.get(name.as_str()).copied() {
                    self.fields[index].value_type.merge(value_type);
                } else {
                    let index = self.fields.len();
                    self.indexes.insert(name.clone(), index);
                    self.fields.push(JsonField { name, value_type });
                }
            }
        }

        let columns = self
            .fields
            .into_iter()
            .map(JsonField::into_inferred)
            .collect();
        InferredSchema::new(FormatKind::Json, columns)
    }
}

struct JsonObject(Vec<(String, Value)>);

impl JsonObject {
    fn into_last_values(self) -> Vec<(String, JsonFieldType)> {
        let mut values: Vec<(String, JsonFieldType)> =
            Vec::with_capacity(self.0.len());
        let mut indexes = HashMap::with_capacity(self.0.len());
        for (name, value) in self.0 {
            let value_type = JsonFieldType::of(&value);
            if let Some(index) = indexes.get(name.as_str()).copied() {
                values[index].1 = value_type;
            } else {
                let index = values.len();
                indexes.insert(name.clone(), index);
                values.push((name, value_type));
            }
        }
        values
    }
}

impl<'de> Deserialize<'de> for JsonObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(JsonObjectVisitor)
    }
}

struct JsonObjectVisitor;

impl<'de> Visitor<'de> for JsonObjectVisitor {
    type Value = JsonObject;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("one JSON object")
    }

    fn visit_map<A>(self, mut values: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = Vec::with_capacity(values.size_hint().unwrap_or(0));
        while let Some(field) = values.next_entry()? {
            fields.push(field);
        }
        Ok(JsonObject(fields))
    }
}

struct JsonField {
    name: String,
    value_type: JsonFieldType,
}

impl JsonField {
    fn into_inferred(self) -> InferredColumn {
        InferredColumn::new(&self.name, self.value_type.postgres_type())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum JsonFieldType {
    Null,
    Boolean,
    Integer,
    Numeric,
    Text,
    Jsonb,
}

impl JsonFieldType {
    fn of(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(number) => {
                if number.as_i64().is_some()
                    || number
                        .as_u64()
                        .is_some_and(|value| value <= i64::MAX as u64)
                {
                    Self::Integer
                } else {
                    Self::Numeric
                }
            }
            Value::String(_) => Self::Text,
            Value::Array(_) | Value::Object(_) => Self::Jsonb,
        }
    }

    fn merge(&mut self, incoming: Self) {
        *self = match (*self, incoming) {
            (current, Self::Null) | (Self::Null, current) => current,
            (current, incoming) if current == incoming => current,
            (Self::Integer, Self::Numeric) | (Self::Numeric, Self::Integer) => {
                Self::Numeric
            }
            _ => Self::Jsonb,
        };
    }

    fn postgres_type(self) -> PostgresType {
        let oid = match self {
            Self::Boolean => pg_sys::BOOLOID,
            Self::Integer => pg_sys::INT8OID,
            Self::Numeric => pg_sys::NUMERICOID,
            Self::Text => pg_sys::TEXTOID,
            Self::Null | Self::Jsonb => pg_sys::JSONBOID,
        };
        PostgresType::new(FormatKind::Json, oid)
    }
}
