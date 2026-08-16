//! Shared NDJSON column binding and decoding.

use std::collections::HashMap;
use std::ffi::CStr;
use std::ops::Range;

use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::tuple::{ColumnDatumTarget, JsonDatumEncoder, JsonDatumKind};
use pgrx::{PgTryBuilder, pg_sys};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::value::RawValue;

use super::super::FormatKind;
use super::scalar::JsonScalarDecoder;
use crate::error::ConnectorError;

#[derive(Clone, Copy)]
enum JsonInputKind {
    Scalar,
    Json,
}

struct PgInputPlan {
    input_function: pg_sys::Oid,
    type_io_param: pg_sys::Oid,
    type_mod: i32,
}

impl PgInputPlan {
    fn bind(type_oid: pg_sys::Oid, type_mod: i32) -> Result<Self, ConnectorError> {
        let result = unsafe {
            PgTryBuilder::new(|| {
                let mut input_function = pg_sys::InvalidOid;
                let mut type_io_param = pg_sys::InvalidOid;
                pg_sys::getTypeInputInfo(
                    type_oid,
                    &mut input_function,
                    &mut type_io_param,
                );
                Ok((input_function, type_io_param))
            })
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute()
        };
        let (input_function, type_io_param) = result.map_err(ConnectorError::from)?;
        Ok(Self {
            input_function,
            type_io_param,
            type_mod,
        })
    }

    unsafe fn datum(&self, value: &CStr) -> pg_sys::Datum {
        unsafe {
            pg_sys::OidInputFunctionCall(
                self.input_function,
                value.as_ptr().cast_mut(),
                self.type_io_param,
                self.type_mod,
            )
        }
    }
}

pub(in crate::format) struct JsonColumn {
    name: Box<str>,
    input_kind: JsonInputKind,
    input: PgInputPlan,
    output: JsonDatumEncoder,
}

impl JsonColumn {
    #[inline]
    pub(in crate::format) fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub(in crate::format) const fn output_encoder(&self) -> JsonDatumEncoder {
        self.output
    }

    pub(in crate::format) unsafe fn input_datum(
        &self,
        value: &CStr,
    ) -> pg_sys::Datum {
        unsafe { self.input.datum(value) }
    }
}

pub(in crate::format) struct JsonColumnPlan {
    columns: Box<[JsonColumn]>,
    indexes: HashMap<Box<str>, usize>,
}

impl JsonColumnPlan {
    pub(in crate::format) fn bind<'a>(
        fields: impl IntoIterator<Item = (&'a str, pg_sys::Oid, i32)>,
    ) -> Result<Self, ConnectorError> {
        ColumnDatumTarget::validate_utf8_server_encoding()?;
        let mut columns = Vec::new();
        let mut indexes = HashMap::new();
        for (name, type_oid, type_mod) in fields {
            if indexes.contains_key(name) {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Json,
                    format!("column name {name:?} is not unique"),
                ));
            }
            let output = JsonDatumEncoder::bind(type_oid)
                .map_err(ConnectorError::json_datum)?;
            let input_kind = match output.kind() {
                JsonDatumKind::Boolean
                | JsonDatumKind::Numeric
                | JsonDatumKind::String => JsonInputKind::Scalar,
                JsonDatumKind::Json => JsonInputKind::Json,
                JsonDatumKind::Array => {
                    return Err(Self::unsupported_type(name, type_oid, "array"));
                }
                JsonDatumKind::Composite => {
                    return Err(Self::unsupported_type(name, type_oid, "composite"));
                }
                JsonDatumKind::Cast => {
                    return Err(Self::unsupported_type(
                        name,
                        type_oid,
                        "type with a one-way JSON cast",
                    ));
                }
                JsonDatumKind::Unsupported => {
                    return Err(Self::unsupported_type(name, type_oid, "pseudo"));
                }
            };
            let index = columns.len();
            indexes.insert(Box::<str>::from(name), index);
            columns.push(JsonColumn {
                name: name.into(),
                input_kind,
                input: PgInputPlan::bind(type_oid, type_mod)?,
                output,
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
            indexes,
        })
    }

    #[inline]
    pub(in crate::format) fn columns(&self) -> &[JsonColumn] {
        &self.columns
    }

    #[inline]
    pub(in crate::format) fn len(&self) -> usize {
        self.columns.len()
    }

    fn unsupported_type(
        name: &str,
        type_oid: pg_sys::Oid,
        kind: &'static str,
    ) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Json,
            format!(
                "column {name:?} uses PostgreSQL type OID {type_oid}, whose {kind} JSON representation is not reversible"
            ),
        )
    }
}

#[derive(Clone)]
struct JsonValueRange(Range<usize>);

pub(in crate::format) enum JsonInputValue<'a> {
    Null,
    Bytes(&'a [u8]),
    CStr(&'a CStr),
}

pub(in crate::format) struct JsonRecordDecoder {
    values: Vec<Option<JsonValueRange>>,
    scalar: JsonScalarDecoder,
}

impl JsonRecordDecoder {
    pub(in crate::format) fn new(column_count: usize) -> Self {
        Self {
            values: vec![None; column_count],
            scalar: JsonScalarDecoder::new(),
        }
    }

    pub(in crate::format) fn decode(
        &mut self,
        plan: &JsonColumnPlan,
        record: &[u8],
        logical_line: u64,
    ) -> Result<(), ConnectorError> {
        self.values.fill(None);
        let mut deserializer = serde_json::Deserializer::from_slice(record);
        RecordSeed {
            indexes: &plan.indexes,
            values: &mut self.values,
            record,
        }
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
        .map_err(|source| ConnectorError::Json {
            line: logical_line,
            source,
        })
    }

    pub(in crate::format) fn value<'a>(
        &'a mut self,
        record: &'a [u8],
        column: &JsonColumn,
        index: usize,
        logical_line: u64,
    ) -> Result<JsonInputValue<'a>, ConnectorError> {
        let Some(range) = self.values[index].as_ref().map(|value| value.0.clone())
        else {
            return Ok(JsonInputValue::Null);
        };
        let raw = &record[range];
        if raw == b"null" {
            return Ok(JsonInputValue::Null);
        }
        let value = match column.input_kind {
            JsonInputKind::Json => raw,
            JsonInputKind::Scalar if raw.len() >= 2 && raw[0] == b'"' => {
                let inner = &raw[1..raw.len() - 1];
                if !inner.contains(&b'\\') {
                    inner
                } else {
                    return self
                        .scalar
                        .decode(raw, column.name(), logical_line)
                        .map(JsonInputValue::CStr);
                }
            }
            JsonInputKind::Scalar if matches!(raw, b"true" | b"false") => raw,
            JsonInputKind::Scalar
                if raw
                    .first()
                    .is_some_and(|byte| *byte == b'-' || byte.is_ascii_digit()) =>
            {
                raw
            }
            JsonInputKind::Scalar => {
                return Err(ConnectorError::invalid_json_value(
                    logical_line,
                    column.name(),
                    "object and array values require a json or jsonb target column",
                ));
            }
        };
        Ok(JsonInputValue::Bytes(value))
    }
}

struct RecordSeed<'a> {
    indexes: &'a HashMap<Box<str>, usize>,
    values: &'a mut [Option<JsonValueRange>],
    record: &'a [u8],
}

impl<'de, 'a> DeserializeSeed<'de> for RecordSeed<'a>
where
    'a: 'de,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RecordVisitor {
            indexes: self.indexes,
            values: self.values,
            record: self.record,
        })
    }
}

struct RecordVisitor<'a> {
    indexes: &'a HashMap<Box<str>, usize>,
    values: &'a mut [Option<JsonValueRange>],
    record: &'a [u8],
}

impl<'de, 'a> Visitor<'de> for RecordVisitor<'a>
where
    'a: 'de,
{
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("one JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(index) = map.next_key_seed(FieldSeed {
            indexes: self.indexes,
        })? {
            match index {
                Some(index) => {
                    let value = map.next_value::<&RawValue>()?;
                    let bytes = value.get().as_bytes();
                    // SAFETY: serde_json borrowed RawValue directly from this
                    // record slice, so both pointers belong to one allocation.
                    let start =
                        unsafe { bytes.as_ptr().offset_from(self.record.as_ptr()) }
                            as usize;
                    self.values[index] =
                        Some(JsonValueRange(start..start + bytes.len()));
                }
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

struct FieldSeed<'a> {
    indexes: &'a HashMap<Box<str>, usize>,
}

impl<'de> DeserializeSeed<'de> for FieldSeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(FieldVisitor {
            indexes: self.indexes,
        })
    }
}

struct FieldVisitor<'a> {
    indexes: &'a HashMap<Box<str>, usize>,
}

impl Visitor<'_> for FieldVisitor<'_> {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object field name")
    }

    fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(self.indexes.get(value).copied())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(self.indexes.get(value).copied())
    }
}
