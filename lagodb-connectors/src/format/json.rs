//! NDJSON format object and streaming schema reader.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};

use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;
use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, Visitor};
use serde_json::Value;

use crate::error::ConnectorError;

use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatSchemaReader,
    FormatWriter, InferredColumn, InferredSchema, PostgresType, StorageFileReader,
    StreamCompression, StreamDecoder,
};

/// JSON-format processor. Each non-empty logical line is one complete value.
pub(crate) struct JsonFormat {
    pub(super) compression: StreamCompression,
}

impl JsonFormat {
    pub(crate) fn resolve(
        compression: StreamCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for json",
            ));
        }
        Ok(Self { compression })
    }
}

impl FormatObject for JsonFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Json
    }
}

impl FormatReader for JsonFormat {}

impl FormatWriter for JsonFormat {}

impl FormatSchemaReader for JsonFormat {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let source = StorageFileReader::new(file);
        let input = StreamDecoder::new(source, self.compression)?;
        JsonSchemaAccumulator::default().read(BufReader::new(input))
    }
}

#[derive(Default)]
struct JsonSchemaAccumulator {
    fields: Vec<JsonField>,
    indexes: HashMap<String, usize>,
}

impl JsonSchemaAccumulator {
    fn read(
        mut self,
        mut input: impl BufRead,
    ) -> Result<InferredSchema, ConnectorError> {
        let mut buffer = Vec::new();
        let mut logical_line = 0_u64;
        loop {
            buffer.clear();
            if input.read_until(b'\n', &mut buffer)? == 0 {
                break;
            }
            if buffer.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            logical_line += 1;
            let object =
                serde_json::from_slice::<JsonObject>(&buffer).map_err(|source| {
                    ConnectorError::Json {
                        line: logical_line,
                        source,
                    }
                })?;
            for (name, value) in object.0 {
                let value_type = JsonFieldType::of(&value);
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
