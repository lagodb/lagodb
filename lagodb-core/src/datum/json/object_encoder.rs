//! Relation-bound PostgreSQL Datum-to-JSON object encoding.

use std::ffi::{CString, c_char};
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::pg_sys::{self, Datum};
use pgrx::{PgMemoryContexts, PgTryBuilder};

use crate::diag::PgError;

use super::{JsonDatumKind, JsonValueError};

// Mirrors `LagodbJsonCategory` in `csrc/compat/lagodb_json_category.h`.
type LagodbJsonCategory = u32;

const LAGODB_JSON_CATEGORY_BOOL: LagodbJsonCategory = 0;
const LAGODB_JSON_CATEGORY_NUMERIC: LagodbJsonCategory = 1;
const LAGODB_JSON_CATEGORY_DATE: LagodbJsonCategory = 2;
const LAGODB_JSON_CATEGORY_TIMESTAMP: LagodbJsonCategory = 3;
const LAGODB_JSON_CATEGORY_TIMESTAMPTZ: LagodbJsonCategory = 4;
const LAGODB_JSON_CATEGORY_JSON: LagodbJsonCategory = 5;
const LAGODB_JSON_CATEGORY_ARRAY: LagodbJsonCategory = 6;
const LAGODB_JSON_CATEGORY_COMPOSITE: LagodbJsonCategory = 7;
const LAGODB_JSON_CATEGORY_CAST: LagodbJsonCategory = 8;
const LAGODB_JSON_CATEGORY_OTHER: LagodbJsonCategory = 9;

unsafe extern "C-unwind" {
    fn lagodb_json_categorize_type(
        type_oid: pg_sys::Oid,
        output_function: *mut pg_sys::Oid,
    ) -> LagodbJsonCategory;
}

#[derive(Clone, Copy, Debug)]
enum JsonDatumCategory {
    Boolean,
    Numeric,
    Date(pg_sys::Oid),
    Json,
    String,
    Array,
    Composite,
    Cast,
}

/// Begin-time PostgreSQL Datum-to-JSON conversion plan.
#[derive(Clone, Copy, Debug)]
pub struct JsonDatumEncoder {
    category: JsonDatumCategory,
    output_function: pg_sys::Oid,
}

#[derive(Clone, Copy, Debug)]
enum JsonObjectDatumEncoder {
    Boolean,
    Numeric { output_function: pg_sys::Oid },
    Date { type_oid: pg_sys::Oid },
    Json { output_function: pg_sys::Oid },
    String { output_function: pg_sys::Oid },
}

struct BoundJsonOutputColumn {
    prefix: Box<[u8]>,
    prefix_len: i32,
    encoder: JsonObjectDatumEncoder,
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
                let mut output_function = pg_sys::InvalidOid;
                // Even valid type OIDs require syscache lookups whose cache
                // entries/resource-owner bookkeeping can allocate and ERROR.
                // This hand-declared C call needs a guard to turn longjmp into
                // Rust unwind before PgTryBuilder can capture it as a Result.
                let category = pg_guard_ffi_boundary(|| {
                    lagodb_json_categorize_type(type_oid, &mut output_function)
                });
                Ok((category, output_function))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }?;
        let category = match category {
            LAGODB_JSON_CATEGORY_BOOL => JsonDatumCategory::Boolean,
            LAGODB_JSON_CATEGORY_NUMERIC => JsonDatumCategory::Numeric,
            LAGODB_JSON_CATEGORY_DATE => JsonDatumCategory::Date(pg_sys::DATEOID),
            LAGODB_JSON_CATEGORY_TIMESTAMP => {
                JsonDatumCategory::Date(pg_sys::TIMESTAMPOID)
            }
            LAGODB_JSON_CATEGORY_TIMESTAMPTZ => {
                JsonDatumCategory::Date(pg_sys::TIMESTAMPTZOID)
            }
            LAGODB_JSON_CATEGORY_JSON => JsonDatumCategory::Json,
            LAGODB_JSON_CATEGORY_OTHER => JsonDatumCategory::String,
            LAGODB_JSON_CATEGORY_ARRAY => JsonDatumCategory::Array,
            LAGODB_JSON_CATEGORY_COMPOSITE => JsonDatumCategory::Composite,
            LAGODB_JSON_CATEGORY_CAST => JsonDatumCategory::Cast,
            other => unreachable!("unknown LagoDB JSON category {other}"),
        };
        Ok(Self {
            category,
            output_function,
        })
    }

    #[inline]
    pub const fn kind(self) -> JsonDatumKind {
        match self.category {
            JsonDatumCategory::Boolean => JsonDatumKind::Boolean,
            JsonDatumCategory::Numeric => JsonDatumKind::Numeric,
            JsonDatumCategory::Date(_) | JsonDatumCategory::String => {
                JsonDatumKind::String
            }
            JsonDatumCategory::Json => JsonDatumKind::Json,
            JsonDatumCategory::Array => JsonDatumKind::Array,
            JsonDatumCategory::Composite => JsonDatumKind::Composite,
            JsonDatumCategory::Cast => JsonDatumKind::Cast,
        }
    }

    fn object_encoder(self) -> Result<JsonObjectDatumEncoder, JsonValueError> {
        let encoder = match self.category {
            JsonDatumCategory::Boolean => JsonObjectDatumEncoder::Boolean,
            JsonDatumCategory::Numeric => JsonObjectDatumEncoder::Numeric {
                output_function: self.output_function,
            },
            JsonDatumCategory::Date(type_oid) => {
                JsonObjectDatumEncoder::Date { type_oid }
            }
            JsonDatumCategory::Json => JsonObjectDatumEncoder::Json {
                output_function: self.output_function,
            },
            JsonDatumCategory::String => JsonObjectDatumEncoder::String {
                output_function: self.output_function,
            },
            JsonDatumCategory::Array
            | JsonDatumCategory::Composite
            | JsonDatumCategory::Cast => {
                return Err(JsonValueError::UnsupportedOutputKind(self.kind()));
            }
        };
        Ok(encoder)
    }
}

impl BoundJsonObjectEncoder {
    /// Bind prevalidated column names and PostgreSQL JSON datum plans.
    pub fn bind<'a>(
        columns: impl IntoIterator<Item = (&'a str, JsonDatumEncoder)>,
    ) -> Result<Self, JsonValueError> {
        let mut buffer = JsonEncodeBuffer::new();
        let mut bound = Vec::new();
        for (index, (name, encoder)) in columns.into_iter().enumerate() {
            let encoder = encoder.object_encoder()?;
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
    fn new() -> Self {
        let mut context = PgMemoryContexts::new("lagodb JSON encode buffer");
        // SAFETY: `context` is live and remains owned by the returned object.
        // `makeStringInfo` allocates in the active context and either returns a
        // valid pointer or raises PostgreSQL ERROR; it never returns NULL.
        let buffer = unsafe {
            context.switch_to(|_| NonNull::new_unchecked(pg_sys::makeStringInfo()))
        };
        Self {
            buffer,
            _context: context,
        }
    }

    fn encode_key_prefix(
        &mut self,
        index: usize,
        name: &str,
    ) -> Result<Box<[u8]>, JsonValueError> {
        let name = CString::new(name)?;
        self.reset();
        self.append_byte(if index == 0 { b'{' } else { b',' });
        // SAFETY: `buffer` is the live StringInfo owned by this object and
        // `name` is a live NUL-terminated string for this call.
        unsafe { pg_sys::escape_json(self.buffer.as_ptr(), name.as_ptr()) };
        self.append_byte(b':');
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
    unsafe fn append_datum(&mut self, encoder: JsonObjectDatumEncoder, datum: Datum) {
        match encoder {
            JsonObjectDatumEncoder::Boolean => {
                // `DatumGetBool`: any nonzero Datum is true.
                self.append_bytes(if datum.value() != 0 {
                    b"true"
                } else {
                    b"false"
                })
            }
            JsonObjectDatumEncoder::Numeric { output_function } => {
                let output =
                    unsafe { pg_sys::OidOutputFunctionCall(output_function, datum) };
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
            JsonObjectDatumEncoder::Date { type_oid } => {
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
            JsonObjectDatumEncoder::Json { output_function } => {
                let output =
                    unsafe { pg_sys::OidOutputFunctionCall(output_function, datum) };
                unsafe {
                    pg_sys::appendStringInfoString(self.buffer.as_ptr(), output)
                };
                unsafe { pg_sys::pfree(output.cast()) };
            }
            JsonObjectDatumEncoder::String { output_function } => {
                let output =
                    unsafe { pg_sys::OidOutputFunctionCall(output_function, datum) };
                unsafe { pg_sys::escape_json(self.buffer.as_ptr(), output) };
                unsafe { pg_sys::pfree(output.cast()) };
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
