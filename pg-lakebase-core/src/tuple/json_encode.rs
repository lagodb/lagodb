//! Relation-bound PostgreSQL Datum-to-JSON object encoding.

use std::ffi::{CString, c_char};
use std::os::raw::c_uint;
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;

use pgrx::pg_sys::{self, Datum};
use pgrx::{PgMemoryContexts, PgTryBuilder};

use crate::diag::PgError;

use super::json::JsonValueError;

type PgJsonTypeCategory = c_uint;

const JSONTYPE_NULL: PgJsonTypeCategory = 0;
const JSONTYPE_BOOL: PgJsonTypeCategory = 1;
const JSONTYPE_NUMERIC: PgJsonTypeCategory = 2;
const JSONTYPE_DATE: PgJsonTypeCategory = 3;
const JSONTYPE_TIMESTAMP: PgJsonTypeCategory = 4;
const JSONTYPE_TIMESTAMPTZ: PgJsonTypeCategory = 5;
const JSONTYPE_JSON: PgJsonTypeCategory = 6;
const JSONTYPE_JSONB: PgJsonTypeCategory = 7;
const JSONTYPE_ARRAY: PgJsonTypeCategory = 8;
const JSONTYPE_COMPOSITE: PgJsonTypeCategory = 9;
const JSONTYPE_CAST: PgJsonTypeCategory = 10;
const JSONTYPE_OTHER: PgJsonTypeCategory = 11;

unsafe extern "C-unwind" {
    #[link_name = "json_categorize_type"]
    fn pg_json_categorize_type(
        type_oid: pg_sys::Oid,
        is_jsonb: bool,
        category: *mut PgJsonTypeCategory,
        output_function: *mut pg_sys::Oid,
    );
}

/// PostgreSQL's JSON representation class for a bound column type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonDatumKind {
    Boolean,
    Numeric,
    String,
    Json,
    Array,
    Composite,
    Cast,
    Unsupported,
}

/// Begin-time PostgreSQL Datum-to-JSON conversion plan.
#[derive(Clone, Copy, Debug)]
pub struct JsonDatumEncoder {
    category: PgJsonTypeCategory,
    output_function: pg_sys::Oid,
    kind: JsonDatumKind,
}

struct BoundJsonOutputColumn {
    prefix: Box<[u8]>,
    prefix_len: i32,
    encoder: JsonDatumEncoder,
}

/// Relation-bound PostgreSQL Datum-to-JSON object encoder.
///
/// Column names and datum output plans are bound once. Every encoded row is
/// appended directly to one reusable PostgreSQL `StringInfo` allocation.
pub struct BoundJsonObjectEncoder {
    columns: Box<[BoundJsonOutputColumn]>,
    buffer: JsonEncodeBuffer,
}

/// Owns a PostgreSQL memory context containing one reusable `StringInfo`.
struct JsonEncodeBuffer {
    buffer: NonNull<pg_sys::StringInfoData>,
    _context: PgMemoryContexts,
}

impl JsonDatumEncoder {
    /// Bind PostgreSQL's JSON category and output function once for a type.
    pub fn bind(type_oid: pg_sys::Oid) -> Result<Self, JsonValueError> {
        let (category, output_function) = unsafe {
            PgTryBuilder::new(|| {
                let mut category = JSONTYPE_NULL;
                let mut output_function = pg_sys::InvalidOid;
                pg_json_categorize_type(
                    type_oid,
                    false,
                    &mut category,
                    &mut output_function,
                );
                Ok((category, output_function))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }?;
        let kind = match category {
            JSONTYPE_BOOL => JsonDatumKind::Boolean,
            JSONTYPE_NUMERIC => JsonDatumKind::Numeric,
            JSONTYPE_DATE | JSONTYPE_TIMESTAMP | JSONTYPE_TIMESTAMPTZ
            | JSONTYPE_OTHER => JsonDatumKind::String,
            JSONTYPE_JSON | JSONTYPE_JSONB => JsonDatumKind::Json,
            JSONTYPE_ARRAY => JsonDatumKind::Array,
            JSONTYPE_COMPOSITE => JsonDatumKind::Composite,
            JSONTYPE_CAST => JsonDatumKind::Cast,
            _ => JsonDatumKind::Unsupported,
        };
        Ok(Self {
            category,
            output_function,
            kind,
        })
    }

    #[inline]
    pub const fn kind(self) -> JsonDatumKind {
        self.kind
    }

    fn supports_object_output(self) -> bool {
        matches!(
            self.kind,
            JsonDatumKind::Boolean
                | JsonDatumKind::Numeric
                | JsonDatumKind::String
                | JsonDatumKind::Json
        )
    }
}

impl BoundJsonObjectEncoder {
    /// Bind prevalidated column names and PostgreSQL JSON datum plans.
    pub fn bind<'a>(
        columns: impl IntoIterator<Item = (&'a str, JsonDatumEncoder)>,
    ) -> Result<Self, JsonValueError> {
        let mut buffer = JsonEncodeBuffer::new()?;
        let mut bound = Vec::new();
        for (index, (name, encoder)) in columns.into_iter().enumerate() {
            if !encoder.supports_object_output() {
                return Err(JsonValueError::UnsupportedOutputKind(encoder.kind()));
            }
            let prefix = buffer.encode_key_prefix(index, name)?;
            let prefix_len = i32::try_from(prefix.len())
                .map_err(|_| JsonValueError::OutputTooLarge)?;
            bound.push(BoundJsonOutputColumn {
                prefix,
                prefix_len,
                encoder,
            });
        }
        Ok(Self {
            columns: bound.into_boxed_slice(),
            buffer,
        })
    }

    /// Encode one row through the complete Begin-time plan.
    ///
    /// # Safety
    ///
    /// `values` must yield exactly one value per bound column in plan order.
    /// Every present Datum must be valid for the type used to bind that column
    /// and remain live until this method returns.
    pub unsafe fn encode_row(
        &mut self,
        values: impl ExactSizeIterator<Item = Option<Datum>>,
    ) -> Result<&[u8], JsonValueError> {
        self.buffer.reset();
        if self.columns.is_empty() {
            self.buffer.append_byte(b'{');
        }
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            for (column, value) in self.columns.iter().zip(values) {
                self.buffer.append_prefix(column);
                match value {
                    None => self.buffer.append_bytes(b"null"),
                    Some(datum) => {
                        // SAFETY: required by this method's bound-row
                        // contract and established for this column above.
                        unsafe { self.buffer.append_datum(column.encoder, datum) };
                    }
                }
            }
            self.buffer.append_byte(b'}');
            Ok::<(), JsonValueError>(())
        }))
        .catch_others(|error| Err(JsonValueError::Postgres(PgError::from(error))))
        .execute();
        result?;
        Ok(self.buffer.as_bytes())
    }
}

impl JsonEncodeBuffer {
    fn new() -> Result<Self, JsonValueError> {
        unsafe {
            PgTryBuilder::new(|| {
                let mut context =
                    PgMemoryContexts::new("pg-lakebase JSON encode buffer");
                let buffer = context.switch_to(|_| {
                    // SAFETY: makeStringInfo either returns a valid allocation
                    // or raises PostgreSQL ERROR; it never returns NULL.
                    NonNull::new_unchecked(pg_sys::makeStringInfo())
                });
                Ok(Self {
                    buffer,
                    _context: context,
                })
            })
            .catch_others(|error| Err(JsonValueError::Postgres(PgError::from(error))))
            .execute()
        }
    }

    fn encode_key_prefix(
        &mut self,
        index: usize,
        name: &str,
    ) -> Result<Box<[u8]>, JsonValueError> {
        let name = CString::new(name)?;
        self.reset();
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                self.append_byte(if index == 0 { b'{' } else { b',' });
                pg_sys::escape_json(self.buffer.as_ptr(), name.as_ptr());
                self.append_byte(b':');
                Ok::<(), JsonValueError>(())
            }))
            .catch_others(|error| Err(JsonValueError::Postgres(PgError::from(error))))
            .execute()
        }?;
        Ok(Box::from(self.as_bytes()))
    }

    #[inline]
    fn reset(&mut self) {
        unsafe { pg_sys::resetStringInfo(self.buffer.as_ptr()) };
    }

    #[inline]
    fn append_byte(&mut self, byte: u8) {
        unsafe { pg_sys::appendStringInfoChar(self.buffer.as_ptr(), byte as c_char) };
    }

    #[inline]
    fn append_bytes(&mut self, bytes: &[u8]) {
        let len = i32::try_from(bytes.len())
            .expect("a static JSON token length fits in i32");
        unsafe {
            pg_sys::appendBinaryStringInfo(
                self.buffer.as_ptr(),
                bytes.as_ptr().cast(),
                len,
            )
        };
    }

    #[inline]
    fn append_prefix(&mut self, column: &BoundJsonOutputColumn) {
        unsafe {
            pg_sys::appendBinaryStringInfo(
                self.buffer.as_ptr(),
                column.prefix.as_ptr().cast(),
                column.prefix_len,
            )
        };
    }

    /// Append one present Datum using PostgreSQL's scalar JSON semantics.
    ///
    /// # Safety
    ///
    /// `datum` must be valid for the type used to bind `encoder`.
    unsafe fn append_datum(&mut self, encoder: JsonDatumEncoder, datum: Datum) {
        match encoder.category {
            JSONTYPE_BOOL => {
                self.append_bytes(if unsafe { pg_sys::DatumGetBool(datum) } {
                    b"true"
                } else {
                    b"false"
                })
            }
            JSONTYPE_NUMERIC => {
                let output = unsafe {
                    pg_sys::OidOutputFunctionCall(encoder.output_function, datum)
                };
                let first = unsafe { *output.cast::<u8>() };
                let second = unsafe { *output.add(1).cast::<u8>() };
                let is_json_number = first.is_ascii_digit()
                    || (first == b'-' && second.is_ascii_digit());
                if !is_json_number {
                    self.append_byte(b'"');
                }
                unsafe {
                    pg_sys::appendStringInfoString(self.buffer.as_ptr(), output)
                };
                if !is_json_number {
                    self.append_byte(b'"');
                }
                unsafe { pg_sys::pfree(output.cast()) };
            }
            category
            @ (JSONTYPE_DATE | JSONTYPE_TIMESTAMP | JSONTYPE_TIMESTAMPTZ) => {
                let type_oid = match category {
                    JSONTYPE_DATE => pg_sys::DATEOID,
                    JSONTYPE_TIMESTAMP => pg_sys::TIMESTAMPOID,
                    JSONTYPE_TIMESTAMPTZ => pg_sys::TIMESTAMPTZOID,
                    _ => unreachable!(
                        "the matched JSON datetime category is exhaustive"
                    ),
                };
                let mut output = [0 as c_char; pg_sys::MAXDATELEN as usize + 1];
                unsafe {
                    pg_sys::JsonEncodeDateTime(
                        output.as_mut_ptr(),
                        datum,
                        type_oid,
                        std::ptr::null(),
                    )
                };
                self.append_byte(b'"');
                unsafe {
                    pg_sys::appendStringInfoString(
                        self.buffer.as_ptr(),
                        output.as_ptr(),
                    )
                };
                self.append_byte(b'"');
            }
            JSONTYPE_JSON | JSONTYPE_JSONB => {
                let output = unsafe {
                    pg_sys::OidOutputFunctionCall(encoder.output_function, datum)
                };
                unsafe {
                    pg_sys::appendStringInfoString(self.buffer.as_ptr(), output)
                };
                unsafe { pg_sys::pfree(output.cast()) };
            }
            JSONTYPE_OTHER => {
                let output = unsafe {
                    pg_sys::OidOutputFunctionCall(encoder.output_function, datum)
                };
                unsafe { pg_sys::escape_json(self.buffer.as_ptr(), output) };
                unsafe { pg_sys::pfree(output.cast()) };
            }
            _ => {
                unreachable!("unsupported JSON categories are rejected while binding")
            }
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let buffer = unsafe { self.buffer.as_ref() };
        unsafe {
            std::slice::from_raw_parts(buffer.data.cast::<u8>(), buffer.len as usize)
        }
    }
}
