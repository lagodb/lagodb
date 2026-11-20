//! Shared NDJSON column binding and decoding.

use std::collections::HashMap;
use std::ops::Range;
use std::str;

use lagodb_core::datum::{
    ColumnDatumTarget, DatumConversionError, JsonDatumEncoder, JsonDatumKind,
    PgTextIo, PgType, SoftErrorContext,
};
use pgrx::pg_sys;
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

pub(crate) struct JsonColumn {
    name: Box<str>,
    input_kind: JsonInputKind,
    input: PgTextIo,
    output: JsonDatumEncoder,
}

impl JsonColumn {
    #[inline]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub(crate) const fn output_encoder(&self) -> JsonDatumEncoder {
        self.output
    }
}

pub(crate) struct JsonColumnPlan {
    columns: Box<[JsonColumn]>,
    indexes: HashMap<Box<str>, usize>,
}

impl JsonColumnPlan {
    pub(crate) fn bind<'a>(
        fields: impl IntoIterator<Item = (&'a str, pg_sys::Oid, i32)>,
    ) -> Result<Self, ConnectorError> {
        ColumnDatumTarget::validate_utf8_server_encoding()
            .map_err(DatumConversionError::from)?;
        let mut columns = Vec::new();
        let mut indexes = HashMap::new();
        for (name, type_oid, type_mod) in fields {
            if indexes.contains_key(name) {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Json,
                    format!("column name {name:?} is not unique"),
                ));
            }
            let output = JsonDatumEncoder::bind(type_oid);
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
            };
            let index = columns.len();
            indexes.insert(Box::<str>::from(name), index);
            columns.push(JsonColumn {
                name: name.into(),
                input_kind,
                input: PgTextIo::bind(PgType::new(type_oid, type_mod)),
                output,
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
            indexes,
        })
    }

    #[inline]
    pub(crate) fn columns(&self) -> &[JsonColumn] {
        &self.columns
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
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

pub(crate) struct JsonRecordDecoder {
    values: Vec<Option<JsonValueRange>>,
    scalar: JsonScalarDecoder,
}

impl JsonRecordDecoder {
    pub(crate) fn new(column_count: usize) -> Self {
        Self {
            values: vec![None; column_count],
            scalar: JsonScalarDecoder::new(),
        }
    }

    pub(crate) fn decode(
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

    /// Convert one bound column of the last decoded record through its
    /// PostgreSQL type input function. SQL NULL also reaches that function,
    /// as in PostgreSQL's COPY, so domain constraints apply to it. With
    /// `soft_errors`, an input error the type reports softly is saved there
    /// and returned as a row data error; otherwise PostgreSQL input errors
    /// unwind.
    ///
    /// # Safety
    ///
    /// `record` is the slice passed to the last successful [`Self::decode`]
    /// with the same plan, and `column` is that plan's column at `index`.
    /// PostgreSQL's current memory context owns the returned datum.
    pub(crate) unsafe fn datum(
        &mut self,
        record: &[u8],
        column: &JsonColumn,
        index: usize,
        logical_line: u64,
        soft_errors: Option<SoftErrorContext<'_>>,
    ) -> Result<Option<pg_sys::Datum>, ConnectorError> {
        let text = self.text(record, column, index, logical_line)?;
        // SAFETY: the text is NUL-free UTF-8 and the bound codec copies it
        // into its own buffer for this synchronous input call.
        match unsafe { column.input.input_or_save(text, soft_errors) } {
            Some(datum) => Ok(text.is_some().then_some(datum)),
            None => Err(ConnectorError::invalid_json_value(
                logical_line,
                column.name(),
                "value is not valid input for the column type",
            )),
        }
    }

    /// The column's input text, or `None` for SQL NULL.
    fn text<'a>(
        &'a mut self,
        record: &'a [u8],
        column: &JsonColumn,
        index: usize,
        logical_line: u64,
    ) -> Result<Option<&'a str>, ConnectorError> {
        let Some(range) = self.values[index].as_ref().map(|value| value.0.clone())
        else {
            return Ok(None);
        };
        let raw = &record[range];
        if raw == b"null" {
            return Ok(None);
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
                        .map(Some);
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
        // SAFETY: decode() captured this range from a serde_json RawValue,
        // whose text is valid UTF-8 without literal NUL bytes; the quote and
        // literal boundaries trimmed above are ASCII.
        Ok(Some(unsafe { str::from_utf8_unchecked(value) }))
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
