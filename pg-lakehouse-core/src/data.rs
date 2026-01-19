//! PostgreSQL data types for Cell and Row
//!
//! This module provides high-level abstractions for PostgreSQL data values,
//! including the `Cell` enum representing individual column values and
//! the `Row` struct representing a complete table row.

use pgrx::prelude::{Date, Interval, Time, Timestamp, TimestampWithTimeZone};

use pgrx::{
    AnyNumeric, FromDatum, IntoDatum, PgBuiltInOids, PgOid,
    datum::Uuid,
    fcinfo,
    pg_sys::{self, Datum, Oid, POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE, bytea},
};
use std::ffi::{CStr, CString};
use std::fmt;
use std::mem;

use crate::pg_wrapper::PgWrapper;
use bytes::Bytes;

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in days.
pub const PG_EPOCH_DAYS_DIFF: i32 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i32;

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in microseconds.
pub const PG_EPOCH_USECS_DIFF: i64 =
    (PG_EPOCH_DAYS_DIFF as i64) * (pgrx::datum::USECS_PER_DAY as i64);

#[derive(Debug, Clone, Copy)]
pub struct StringView {
    pub ptr: *const u8,
    pub len: usize,
}

unsafe impl Send for StringView {}
unsafe impl Sync for StringView {}

impl StringView {
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

#[derive(Debug, Clone, Copy)]
pub struct ByteaView {
    pub ptr: *const u8,
    pub len: usize,
}

unsafe impl Send for ByteaView {}
unsafe impl Sync for ByteaView {}

impl ByteaView {
    /// # Safety
    /// Caller must ensure that the pointer is valid and points to valid data
    /// for the lifetime of the return value.
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[derive(Debug)]
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
    Json(Bytes),
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
            Cell::Json(b) => std::mem::size_of::<Bytes>() + b.len(),
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

unsafe impl Send for Cell {}

impl Clone for Cell {
    fn clone(&self) -> Self {
        match self {
            Cell::Bool(v) => Cell::Bool(*v),
            Cell::I8(v) => Cell::I8(*v),
            Cell::I16(v) => Cell::I16(*v),
            Cell::F32(v) => Cell::F32(*v),
            Cell::I32(v) => Cell::I32(*v),
            Cell::F64(v) => Cell::F64(*v),
            Cell::I64(v) => Cell::I64(*v),
            Cell::Numeric(v) => Cell::Numeric(v.clone()),
            Cell::String(v) => Cell::String(v.clone()),
            Cell::StringView(v) => Cell::StringView(*v),
            Cell::Date(v) => Cell::Date(*v),
            Cell::Time(v) => Cell::Time(*v),
            Cell::Timestamp(v) => Cell::Timestamp(*v),
            Cell::Timestamptz(v) => Cell::Timestamptz(*v),
            Cell::Interval(v) => Cell::Interval(*v),
            Cell::Json(v) => Cell::Json(v.clone()),
            Cell::Bytea(v) => Cell::Bytea(v.clone()),
            Cell::ByteaView(v) => Cell::ByteaView(*v),
            Cell::Uuid(v) => Cell::Uuid(*v),
            Cell::BoolArray(v) => Cell::BoolArray(v.clone()),
            Cell::I16Array(v) => Cell::I16Array(v.clone()),
            Cell::I32Array(v) => Cell::I32Array(v.clone()),
            Cell::I64Array(v) => Cell::I64Array(v.clone()),
            Cell::F32Array(v) => Cell::F32Array(v.clone()),
            Cell::F64Array(v) => Cell::F64Array(v.clone()),
            Cell::StringArray(v) => Cell::StringArray(v.clone()),
        }
    }
}

fn write_array<T: std::fmt::Display>(
    array: &[Option<T>],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let res = array
        .iter()
        .map(|e| match e {
            Some(val) => format!("{}", val),
            None => "null".to_owned(),
        })
        .collect::<Vec<String>>()
        .join(",");
    write!(f, "[{}]", res)
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
                let dt = fcinfo::direct_function_call_as_datum(
                    pg_sys::date_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid date");
                let dt_cstr = CStr::from_ptr(dt.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    dt_cstr.to_str().expect("date should be a valid string")
                )
            },
            Cell::Time(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::time_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid time");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr.to_str().expect("time should be a valid string")
                )
            },
            Cell::Timestamp(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamp_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamp");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamp should be a valid string")
                )
            },
            Cell::Timestamptz(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamptz_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamptz");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamptz should be a valid string")
                )
            },
            Cell::Interval(v) => write!(f, "{}", v),
            Cell::Json(b) => unsafe {
                let datum = Datum::from(b.as_ptr());
                let out = fcinfo::direct_function_call_as_datum(
                    pg_sys::jsonb_out,
                    &[Some(datum)],
                )
                .expect("jsonb_out failed");
                let out_cstr = CStr::from_ptr(out.cast_mut_ptr());
                write!(f, "{}", out_cstr.to_string_lossy())
            },
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

impl Cell {
    /// Convert cell to datum with type information.
    ///
    /// This method is needed for types where the target column type
    /// must be known to create the correct Datum format.
    ///
    /// # Arguments
    /// * `typoid` - The target PostgreSQL type OID
    /// * `typmod` - The target type modifier
    ///
    /// # Safety
    /// This function is unsafe because it calls PostgreSQL internal functions.
    pub unsafe fn into_datum_typed(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => match self {
                Cell::ByteaView(v) => unsafe {
                    PgWrapper::jsonb_in_from_bytes(v.ptr, v.len).ok()
                },
                _ => None,
            },
            PgOid::BuiltIn(PgBuiltInOids::JSONOID) => match self {
                Cell::StringView(v) => unsafe {
                    PgWrapper::json_in_from_bytes(v.ptr, v.len).ok()
                },
                _ => None,
            },
            PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => match self {
                Cell::StringView(v) => unsafe {
                    let c_str = CString::new(v.as_str()).ok()?;
                    fcinfo::direct_function_call_as_datum(
                        pg_sys::namein,
                        &[Some(Datum::from(c_str.as_ptr()))],
                    )
                },
                _ => None,
            },
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => match self {
                Cell::I16(v) => Some(v.into_datum().unwrap()),
                Cell::I32(v) => Some((v as i16).into_datum().unwrap()),
                Cell::I64(v) => Some((v as i16).into_datum().unwrap()),
                _ => self.into_datum(),
            },
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => match self {
                Cell::I16(v) => Some((v as i32).into_datum().unwrap()),
                Cell::I32(v) => Some(v.into_datum().unwrap()),
                Cell::I64(v) => Some((v as i32).into_datum().unwrap()),
                _ => self.into_datum(),
            },
            _ => self.into_datum(),
        }
    }
}

impl IntoDatum for Cell {
    fn into_datum(self) -> Option<Datum> {
        match self {
            Cell::Bool(v) => v.into_datum(),
            Cell::I8(v) => v.into_datum(),
            Cell::I16(v) => v.into_datum(),
            Cell::F32(v) => v.into_datum(),
            Cell::I32(v) => v.into_datum(),
            Cell::F64(v) => v.into_datum(),
            Cell::I64(v) => v.into_datum(),
            Cell::Numeric(v) => v.into_datum(),
            Cell::String(v) => v.into_datum(),
            Cell::StringView(v) => {
                let s = unsafe { v.as_str() };
                s.into_datum()
            }
            Cell::Date(v) => v.into_datum(),
            Cell::Time(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Timestamptz(v) => v.into_datum(),
            Cell::Interval(v) => v.into_datum(),
            Cell::Json(v) => v.into_datum(),
            Cell::Bytea(v) => v.as_ref().into_datum(),
            Cell::ByteaView(v) => {
                let slice = unsafe { v.as_slice() };
                slice.into_datum()
            }
            Cell::Uuid(v) => v.into_datum(),
            Cell::BoolArray(v) => v.into_datum(),
            Cell::I16Array(v) => v.into_datum(),
            Cell::I32Array(v) => v.into_datum(),
            Cell::I64Array(v) => v.into_datum(),
            Cell::F32Array(v) => v.into_datum(),
            Cell::F64Array(v) => v.into_datum(),
            Cell::StringArray(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
        Oid::INVALID
    }

    fn is_compatible_with(other: Oid) -> bool {
        Self::type_oid() == other
            || other == pg_sys::BOOLOID
            || other == pg_sys::CHAROID
            || other == pg_sys::INT2OID
            || other == pg_sys::FLOAT4OID
            || other == pg_sys::INT4OID
            || other == pg_sys::FLOAT8OID
            || other == pg_sys::INT8OID
            || other == pg_sys::NUMERICOID
            || other == pg_sys::TEXTOID
            || other == pg_sys::VARCHAROID
            || other == pg_sys::BPCHAROID
            || other == pg_sys::NAMEOID
            || other == pg_sys::JSONOID
            || other == pg_sys::DATEOID
            || other == pg_sys::TIMEOID
            || other == pg_sys::TIMESTAMPOID
            || other == pg_sys::TIMESTAMPTZOID
            || other == pg_sys::INTERVALOID
            || other == pg_sys::JSONBOID
            || other == pg_sys::BYTEAOID
            || other == pg_sys::UUIDOID
            || other == pg_sys::BOOLARRAYOID
            || other == pg_sys::INT2ARRAYOID
            || other == pg_sys::INT4ARRAYOID
            || other == pg_sys::INT8ARRAYOID
            || other == pg_sys::FLOAT4ARRAYOID
            || other == pg_sys::FLOAT8ARRAYOID
            || other == pg_sys::TEXTARRAYOID
            || other == pg_sys::VARCHARARRAYOID
            || other == pg_sys::BPCHARARRAYOID
            || other == pg_sys::NAMEARRAYOID
            || other == pg_sys::JSONARRAYOID
    }
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(
        datum: Datum,
        is_null: bool,
        typoid: Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        unsafe {
            let oid = PgOid::from(typoid);
            match oid {
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                    bool::from_datum(datum, is_null).map(Cell::Bool)
                }
                PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
                    i8::from_datum(datum, is_null).map(Cell::I8)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                    i16::from_datum(datum, is_null).map(Cell::I16)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                    f32::from_datum(datum, is_null).map(Cell::F32)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                    i32::from_datum(datum, is_null).map(Cell::I32)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                    f64::from_datum(datum, is_null).map(Cell::F64)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                    i64::from_datum(datum, is_null).map(Cell::I64)
                }
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                    AnyNumeric::from_datum(datum, is_null).map(Cell::Numeric)
                }
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
                | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
                | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
                | PgOid::BuiltIn(PgBuiltInOids::JSONOID) => {
                    String::from_datum(datum, is_null).map(Cell::String)
                }
                PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => {
                    if is_null {
                        None
                    } else {
                        let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
                        let c_str = CStr::from_ptr((*name_ptr).data.as_ptr());
                        Some(Cell::String(c_str.to_string_lossy().into_owned()))
                    }
                }
                PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                    Date::from_datum(datum, is_null).map(Cell::Date)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                    Time::from_datum(datum, is_null).map(Cell::Time)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                    Timestamp::from_datum(datum, is_null).map(Cell::Timestamp)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                    TimestampWithTimeZone::from_datum(datum, is_null)
                        .map(Cell::Timestamptz)
                }
                PgOid::BuiltIn(PgBuiltInOids::INTERVALOID) => {
                    Interval::from_datum(datum, is_null).map(Cell::Interval)
                }
                PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                    if is_null {
                        None
                    } else {
                        // Extract raw varlena bytes including header
                        let ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
                        let varsize = pgrx::varlena::varsize(ptr);
                        let slice =
                            std::slice::from_raw_parts(ptr as *const u8, varsize);
                        Some(Cell::Json(Bytes::copy_from_slice(slice)))
                    }
                }
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                    let ptr = datum.cast_mut_ptr::<bytea>();
                    if ptr.is_null() {
                        None
                    } else {
                        // SAFETY: ptr is a valid pointer to varlena because it comes from a Datum of type BYTEAOID
                        let slice = pgrx::varlena::varlena_to_byte_slice(ptr);
                        Some(Cell::Bytea(Bytes::copy_from_slice(slice)))
                    }
                }
                PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => {
                    Uuid::from_datum(datum, is_null).map(Cell::Uuid)
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
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    pub cells: Vec<Option<Cell>>,
    pub size: usize,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut cells = Vec::with_capacity(capacity);
        cells.resize_with(capacity, || None);
        Self { cells, size: 0 }
    }

    pub fn push(&mut self, cell: Option<Cell>) {
        if let Some(ref c) = cell {
            self.size += c.mem_size();
        }
        self.cells.push(cell);
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Option<Cell>> {
        self.cells.get(index)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Option<Cell>> {
        self.cells.iter()
    }

    pub fn iter_with_index(&self) -> impl Iterator<Item = (usize, &Option<Cell>)> {
        self.cells.iter().enumerate()
    }

    #[inline]
    pub fn replace_with(&mut self, src: Row) {
        let _ = mem::replace(self, src);
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.size = 0;
    }

    pub unsafe fn update_from_slot(&mut self, slot: *mut pg_sys::TupleTableSlot) {
        unsafe {
            // Ensure slot contents are accessible (deform tuple if needed)
            pg_sys::slot_getallattrs(slot);

            let tup_desc = (*slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            let values = std::slice::from_raw_parts((*slot).tts_values, natts);
            let nulls = std::slice::from_raw_parts((*slot).tts_isnull, natts);
            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

            // Resize and fill
            self.cells.resize_with(natts, || None);
            self.size = 0;

            for i in 0..natts {
                self.cells[i] = if nulls[i] {
                    None
                } else {
                    let attr = &attrs[i];
                    Cell::from_polymorphic_datum(values[i], false, attr.atttypid)
                        .inspect(|c| {
                            self.size += c.mem_size();
                        })
                };
            }
        }
    }

    pub unsafe fn from_slot(slot: *mut pg_sys::TupleTableSlot) -> Self {
        unsafe {
            let mut row = Self::new();
            row.update_from_slot(slot);
            row
        }
    }
}
