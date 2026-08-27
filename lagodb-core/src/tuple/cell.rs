//! `Cell`: a tagged value matching one PostgreSQL column type.
//!
//! Includes zero-copy view types ([`StringView`], [`ByteaView`]) for varlena
//! data borrowed from PostgreSQL or Arrow buffers, and the conversions to and
//! from PostgreSQL `Datum`.

use bytes::Bytes;
use pgrx::pg_sys::{self, Datum, Oid, bytea};
use pgrx::prelude::{Date, Interval, Time, Timestamp, TimestampWithTimeZone};
use pgrx::{AnyNumeric, FromDatum, IntoDatum, PgBuiltInOids, PgOid, datum::Uuid};
use std::ffi::CStr;
use std::fmt;

use crate::wrapper::PgOutputCString;

use super::datum::DatumConversionError;
use super::json::{JsonText, JsonbValue};

/// A non-owning view into UTF-8 string data.
///
/// `StringView` is a raw pointer plus length; it does not own the underlying
/// memory. The data typically lives in either:
///
/// - PostgreSQL palloc'd memory (e.g. a tuple slot's varlena buffer), in which
///   case the view is valid only until the surrounding memory context is reset
///   or the slot is reused.
/// - An Arrow buffer held by an upstream `RecordBatch`, in which case the
///   view is valid until that batch is dropped.
///
/// # Thread affinity
///
/// `StringView` intentionally does not implement `Send` or `Sync`. The value
/// is just `(*const u8, usize)`, but the backing memory may be PostgreSQL
/// palloc'd memory or an Arrow buffer owned by a scan batch. Keeping the view
/// thread-affine makes the borrowing contract visible to the type system.
#[derive(Debug, Clone, Copy)]
pub struct StringView {
    ptr: *const u8,
    len: usize,
}

impl StringView {
    /// Construct a view over caller-owned bytes.
    ///
    /// # Safety
    ///
    /// ptr must point to len bytes of valid UTF-8 that remain live for every
    /// use of the returned view.
    pub unsafe fn from_raw_parts(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }

    /// # Safety
    /// Caller must ensure that the pointer is valid and points to valid UTF-8 data
    /// for the lifetime of the return value.
    pub unsafe fn as_str<'a>(&self) -> &'a str {
        unsafe {
            let slice = std::slice::from_raw_parts(self.ptr, self.len);
            std::str::from_utf8_unchecked(slice)
        }
    }
}

/// A non-owning view into a byte slice.
///
/// Same lifetime/thread-affinity caveats as [`StringView`]: the value is a
/// raw pointer + length, and callers are responsible for keeping the backing
/// allocation alive while the view is used.
#[derive(Debug, Clone, Copy)]
pub struct ByteaView {
    ptr: *const u8,
    len: usize,
}

impl ByteaView {
    /// Construct a view over caller-owned bytes.
    ///
    /// # Safety
    ///
    /// ptr must point to len live bytes for every use of the returned view.
    pub unsafe fn from_raw_parts(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }

    /// # Safety
    /// Caller must ensure that the pointer is valid and points to valid data
    /// for the lifetime of the return value.
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[derive(Debug, Clone)]
pub enum Cell {
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(AnyNumeric),
    String(String),
    StringView(StringView),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    Timestamptz(TimestampWithTimeZone),
    Interval(Interval),
    /// Semantic PostgreSQL `json` value retaining its validated input text.
    Json(JsonText),
    /// Semantic PostgreSQL `jsonb` value represented by PostgreSQL output text.
    /// This is not PostgreSQL's internal varlena representation.
    Jsonb(JsonbValue),
    Bytea(Bytes),
    ByteaView(ByteaView),
    Uuid(Uuid),
    BoolArray(Vec<Option<bool>>),
    I16Array(Vec<Option<i16>>),
    I32Array(Vec<Option<i32>>),
    I64Array(Vec<Option<i64>>),
    F32Array(Vec<Option<f32>>),
    F64Array(Vec<Option<f64>>),
    StringArray(Vec<Option<String>>),
}

impl Cell {
    /// Checked semantic Datum conversion used by [`super::row_codec::RowDatumCodec`].
    ///
    /// The row codec validates the server encoding once before calling this
    /// method and keeps non-NULL conversion failures distinct from SQL NULL.
    pub(crate) unsafe fn from_polymorphic_datum_checked(
        datum: Datum,
        is_null: bool,
        typoid: Oid,
    ) -> Result<Option<Self>, DatumConversionError> {
        if is_null {
            return Ok(None);
        }
        let cell = match typoid {
            pg_sys::JSONOID => unsafe { JsonText::from_datum(datum, false) }
                .map(|value| value.map(Cell::Json))?,
            pg_sys::JSONBOID => unsafe { JsonbValue::from_datum(datum, false) }
                .map(|value| value.map(Cell::Jsonb))?,
            _ => unsafe { Self::from_standard_datum(datum, false, typoid) }
                .ok_or(DatumConversionError::InvalidInput { target: typoid })
                .map(Some)?,
        };
        Ok(cell)
    }

    /// Convert a non-JSON PostgreSQL datum using the native pgrx datum
    /// implementations.
    ///
    /// JSON and JSONB are intentionally handled by
    /// [`Self::from_polymorphic_datum_checked`], because their conversions
    /// have a structured error path and require the row codec's encoding
    /// capability check.
    unsafe fn from_standard_datum(
        datum: Datum,
        is_null: bool,
        typoid: Oid,
    ) -> Option<Self> {
        if is_null {
            return None;
        }

        unsafe {
            let oid = PgOid::from(typoid);
            match oid {
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                    bool::from_datum(datum, false).map(Cell::Bool)
                }
                PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
                    i8::from_datum(datum, false).map(Cell::I8)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                    i16::from_datum(datum, false).map(Cell::I16)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                    f32::from_datum(datum, false).map(Cell::F32)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                    i32::from_datum(datum, false).map(Cell::I32)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                    f64::from_datum(datum, false).map(Cell::F64)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                    i64::from_datum(datum, false).map(Cell::I64)
                }
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                    AnyNumeric::from_datum(datum, false).map(Cell::Numeric)
                }
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
                | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
                | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID) => {
                    String::from_datum(datum, false).map(Cell::String)
                }
                PgOid::BuiltIn(PgBuiltInOids::JSONOID)
                | PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => None,
                PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => {
                    let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
                    let c_str = CStr::from_ptr((*name_ptr).data.as_ptr());
                    Some(Cell::String(c_str.to_string_lossy().into_owned()))
                }
                PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                    Date::from_datum(datum, false).map(Cell::Date)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                    Time::from_datum(datum, false).map(Cell::Time)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                    Timestamp::from_datum(datum, false).map(Cell::Timestamp)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                    TimestampWithTimeZone::from_datum(datum, false)
                        .map(Cell::Timestamptz)
                }
                PgOid::BuiltIn(PgBuiltInOids::INTERVALOID) => {
                    Interval::from_datum(datum, false).map(Cell::Interval)
                }
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                    let ptr = datum.cast_mut_ptr::<bytea>();
                    // SAFETY: ptr is valid for a BYTEAOID datum supplied by
                    // PostgreSQL, and this copy completes before the source
                    // slot can be reused.
                    let slice = pgrx::varlena::varlena_to_byte_slice(ptr);
                    Some(Cell::Bytea(Bytes::copy_from_slice(slice)))
                }
                PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => {
                    Uuid::from_datum(datum, false).map(Cell::Uuid)
                }
                PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                    Vec::<Option<bool>>::from_datum(datum, false).map(Cell::BoolArray)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                    Vec::<Option<i16>>::from_datum(datum, false).map(Cell::I16Array)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                    Vec::<Option<i32>>::from_datum(datum, false).map(Cell::I32Array)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                    Vec::<Option<i64>>::from_datum(datum, false).map(Cell::I64Array)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                    Vec::<Option<f32>>::from_datum(datum, false).map(Cell::F32Array)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                    Vec::<Option<f64>>::from_datum(datum, false).map(Cell::F64Array)
                }
                PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID)
                | PgOid::BuiltIn(PgBuiltInOids::VARCHARARRAYOID)
                | PgOid::BuiltIn(PgBuiltInOids::BPCHARARRAYOID)
                | PgOid::BuiltIn(PgBuiltInOids::NAMEARRAYOID)
                | PgOid::BuiltIn(PgBuiltInOids::JSONARRAYOID) => {
                    Vec::<Option<String>>::from_datum(datum, false)
                        .map(Cell::StringArray)
                }
                _ => None,
            }
        }
    }

    /// Convert any borrowed view variant into an owned cell.
    ///
    /// Arrow/slot readers may use `StringView` and `ByteaView` for hot-path
    /// zero-copy decoding. A `Row` that outlives the source batch/slot must own
    /// those buffers instead.
    pub fn into_owned(self) -> Self {
        match self {
            Cell::StringView(view) => {
                // SAFETY: the caller owns the source lifetime decision. This
                // method copies the bytes immediately, so the returned cell no
                // longer borrows from `view`.
                Cell::String(unsafe { view.as_str() }.to_owned())
            }
            Cell::ByteaView(view) => {
                // SAFETY: as above, the slice is copied before returning.
                Cell::Bytea(Bytes::copy_from_slice(unsafe { view.as_slice() }))
            }
            other => other,
        }
    }

    /// Check if cell is an array type
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            Cell::BoolArray(_)
                | Cell::I16Array(_)
                | Cell::I32Array(_)
                | Cell::I64Array(_)
                | Cell::F32Array(_)
                | Cell::F64Array(_)
                | Cell::StringArray(_)
        )
    }

    /// Returns the estimated memory size of the cell in bytes
    pub fn mem_size(&self) -> usize {
        match self {
            Cell::Bool(_) => std::mem::size_of::<bool>(),
            Cell::I8(_) => std::mem::size_of::<i8>(),
            Cell::I16(_) => std::mem::size_of::<i16>(),
            Cell::F32(_) => std::mem::size_of::<f32>(),
            Cell::I32(_) => std::mem::size_of::<i32>(),
            Cell::F64(_) => std::mem::size_of::<f64>(),
            Cell::I64(_) => std::mem::size_of::<i64>(),
            Cell::Numeric(_) => std::mem::size_of::<AnyNumeric>(),
            Cell::String(s) => std::mem::size_of::<String>() + s.len(),
            Cell::StringView(_) => std::mem::size_of::<StringView>(),
            Cell::Date(_) => std::mem::size_of::<Date>(),
            Cell::Time(_) => std::mem::size_of::<Time>(),
            Cell::Timestamp(_) => std::mem::size_of::<Timestamp>(),
            Cell::Timestamptz(_) => std::mem::size_of::<TimestampWithTimeZone>(),
            Cell::Interval(_) => std::mem::size_of::<Interval>(),
            Cell::Json(value) => value.mem_size(),
            Cell::Jsonb(value) => value.mem_size(),
            Cell::Bytea(b) => std::mem::size_of::<Bytes>() + b.len(),
            Cell::ByteaView(_) => std::mem::size_of::<ByteaView>(),
            Cell::Uuid(_) => std::mem::size_of::<Uuid>(),
            Cell::BoolArray(v) => {
                std::mem::size_of::<Vec<Option<bool>>>()
                    + v.len() * std::mem::size_of::<Option<bool>>()
            }
            Cell::I16Array(v) => {
                std::mem::size_of::<Vec<Option<i16>>>()
                    + v.len() * std::mem::size_of::<Option<i16>>()
            }
            Cell::I32Array(v) => {
                std::mem::size_of::<Vec<Option<i32>>>()
                    + v.len() * std::mem::size_of::<Option<i32>>()
            }
            Cell::I64Array(v) => {
                std::mem::size_of::<Vec<Option<i64>>>()
                    + v.len() * std::mem::size_of::<Option<i64>>()
            }
            Cell::F32Array(v) => {
                std::mem::size_of::<Vec<Option<f32>>>()
                    + v.len() * std::mem::size_of::<Option<f32>>()
            }
            Cell::F64Array(v) => {
                std::mem::size_of::<Vec<Option<f64>>>()
                    + v.len() * std::mem::size_of::<Option<f64>>()
            }
            Cell::StringArray(v) => {
                std::mem::size_of::<Vec<Option<String>>>()
                    + v.len() * (std::mem::size_of::<Option<String>>() + 16) // Rough estimate of 16 bytes per string to keep O(1)
            }
        }
    }
}

fn write_array<T: std::fmt::Display>(
    array: &[Option<T>],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(f, "[")?;
    for (i, e) in array.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        match e {
            Some(val) => write!(f, "{}", val)?,
            None => write!(f, "null")?,
        }
    }
    write!(f, "]")
}

unsafe fn write_pg_output(
    f: &mut fmt::Formatter<'_>,
    output_fn: unsafe fn(pg_sys::FunctionCallInfo) -> Datum,
    arg: Option<Datum>,
    context: &'static str,
    quoted: bool,
) -> fmt::Result {
    let output = unsafe { PgOutputCString::from_function_call(output_fn, &[arg]) }
        .expect(context);
    let text = output.as_cstr().to_string_lossy();

    if quoted {
        write!(f, "'{}'", text)
    } else {
        write!(f, "{}", text)
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        macro_rules! write_hex_bytes {
            ($bytes:expr) => {{
                let bytes = $bytes;
                if bytes.is_empty() {
                    write!(f, "''")
                } else {
                    write!(f, r#"'\x"#)?;
                    for b in bytes {
                        write!(f, "{:02X}", b)?;
                    }
                    write!(f, "'")
                }
            }};
        }

        match self {
            Cell::Bool(v) => write!(f, "{}", v),
            Cell::I8(v) => write!(f, "{}", v),
            Cell::I16(v) => write!(f, "{}", v),
            Cell::F32(v) => write!(f, "{}", v),
            Cell::I32(v) => write!(f, "{}", v),
            Cell::F64(v) => write!(f, "{}", v),
            Cell::I64(v) => write!(f, "{}", v),
            Cell::Numeric(v) => write!(f, "{}", v),
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::StringView(v) => {
                let s = unsafe { v.as_str() };
                write!(f, "'{}'", s)
            }
            Cell::Date(v) => unsafe {
                write_pg_output(
                    f,
                    pg_sys::date_out,
                    (*v).into_datum(),
                    "cell should be a valid date",
                    true,
                )
            },
            Cell::Time(v) => unsafe {
                write_pg_output(
                    f,
                    pg_sys::time_out,
                    (*v).into_datum(),
                    "cell should be a valid time",
                    true,
                )
            },
            Cell::Timestamp(v) => unsafe {
                write_pg_output(
                    f,
                    pg_sys::timestamp_out,
                    (*v).into_datum(),
                    "cell should be a valid timestamp",
                    true,
                )
            },
            Cell::Timestamptz(v) => unsafe {
                write_pg_output(
                    f,
                    pg_sys::timestamptz_out,
                    (*v).into_datum(),
                    "cell should be a valid timestamptz",
                    true,
                )
            },
            Cell::Interval(v) => unsafe {
                write_pg_output(
                    f,
                    pg_sys::interval_out,
                    (*v).into_datum(),
                    "cell should be a valid interval",
                    false,
                )
            },
            Cell::Json(value) => write!(f, "{value}"),
            Cell::Jsonb(value) => write!(f, "{value}"),
            Cell::Bytea(v) => write_hex_bytes!(v),
            Cell::ByteaView(v) => {
                let slice = unsafe { v.as_slice() };
                write_hex_bytes!(slice)
            }
            Cell::Uuid(v) => write!(f, "'{}'", v),
            Cell::BoolArray(v) => write_array(v, f),
            Cell::I16Array(v) => write_array(v, f),
            Cell::I32Array(v) => write_array(v, f),
            Cell::I64Array(v) => write_array(v, f),
            Cell::F32Array(v) => write_array(v, f),
            Cell::F64Array(v) => write_array(v, f),
            Cell::StringArray(v) => write_array(v, f),
        }
    }
}
