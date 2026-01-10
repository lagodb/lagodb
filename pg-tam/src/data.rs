//! PostgreSQL data types for Cell and Row
//!
//! This module provides high-level abstractions for PostgreSQL data values,
//! including the `Cell` enum representing individual column values and
//! the `Row` struct representing a complete table row.

use pgrx::datum::datetime_support::{DateTimeParts, HasExtractableParts};
use pgrx::prelude::{Date, Interval, Time, Timestamp, TimestampWithTimeZone};

use crate::pg_wrapper::PgWrapper;
use pgrx::{
    AnyNumeric, FromDatum, IntoDatum, JsonB, PgBuiltInOids, PgOid,
    datum::Uuid,
    fcinfo,
    pg_sys::{self, Datum, Oid, bytea},
};
use std::ffi::CStr;
use std::fmt;
use std::mem;

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
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    Timestamptz(TimestampWithTimeZone),
    Interval(Interval),
    Json(JsonB),
    Bytea(Vec<u8>),
    Uuid(Uuid),
    Composite(JsonB),
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
            Cell::Date(_) => std::mem::size_of::<Date>(),
            Cell::Time(_) => std::mem::size_of::<Time>(),
            Cell::Timestamp(_) => std::mem::size_of::<Timestamp>(),
            Cell::Timestamptz(_) => std::mem::size_of::<TimestampWithTimeZone>(),
            Cell::Interval(_) => std::mem::size_of::<Interval>(),
            Cell::Json(_) => std::mem::size_of::<JsonB>() + 32, // Rough estimate for JsonB payload
            Cell::Bytea(b) => std::mem::size_of::<Vec<u8>>() + b.len(),
            Cell::Uuid(_) => std::mem::size_of::<Uuid>(),
            Cell::Composite(_) => std::mem::size_of::<JsonB>() + 32,
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

    pub fn to_json_value(&self) -> serde_json::Value {
        use serde_json::{Number, Value};
        match self {
            Cell::Bool(v) => Value::Bool(*v),
            Cell::I8(v) => Value::Number((*v).into()),
            Cell::I16(v) => Value::Number((*v).into()),
            Cell::F32(v) => Number::from_f64(*v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Cell::I32(v) => Value::Number((*v).into()),
            Cell::F64(v) => Number::from_f64(*v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Cell::I64(v) => Value::Number((*v).into()),
            Cell::Numeric(v) => Value::String(v.to_string()),
            Cell::String(v) => Value::String(v.clone()),
            Cell::Json(v) | Cell::Composite(v) => v.0.clone(),
            Cell::Bytea(v) => {
                // Encode using the same logic as Display for consistency, or standard hex
                // Since this is for JSON/Arrow conversion, standard hex (without \x) is usually safer for downstream parsers,
                // but let's stick to a clean hex string.
                let hex = v
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<String>>()
                    .join("");
                Value::String(hex)
            }
            Cell::BoolArray(v) => Value::Array(
                v.iter()
                    .map(|o| o.map(Value::Bool).unwrap_or(Value::Null))
                    .collect(),
            ),
            Cell::I16Array(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.map(|i| Value::Number(i.into())).unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::I32Array(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.map(|i| Value::Number(i.into())).unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::I64Array(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.map(|i| Value::Number(i.into())).unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::F32Array(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.and_then(|f| Number::from_f64(f as f64).map(Value::Number))
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::F64Array(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.and_then(|f| Number::from_f64(f).map(Value::Number))
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::StringArray(v) => Value::Array(
                v.iter()
                    .map(|o| {
                        o.as_ref()
                            .map(|s| Value::String(s.clone()))
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Cell::Date(d) => {
                // PostgreSQL Date epoch: 2000-01-01, Arrow: 1970-01-01. Diff: 10957 days.
                const PG_EPOCH_DAYS: i32 = 10957;
                let pg_days = d.to_pg_epoch_days();
                Value::Number((pg_days + PG_EPOCH_DAYS).into())
            }
            Cell::Time(t) => {
                // Return microseconds since midnight
                let epoch = t
                    .extract_part(DateTimeParts::Epoch)
                    .and_then(|n| n.try_into().ok())
                    .unwrap_or(0.0);
                Value::Number(((epoch * 1_000_000.0) as i64).into())
            }
            Cell::Timestamp(ts) => {
                // Return microseconds since Unix Epoch (1970)
                let epoch = ts
                    .extract_part(DateTimeParts::Epoch)
                    .and_then(|n| n.try_into().ok())
                    .unwrap_or(0.0);
                Value::Number(((epoch * 1_000_000.0) as i64).into())
            }
            Cell::Timestamptz(ts) => {
                // Return microseconds since Unix Epoch (1970)
                let epoch = ts
                    .extract_part(DateTimeParts::Epoch)
                    .and_then(|n| n.try_into().ok())
                    .unwrap_or(0.0);
                Value::Number(((epoch * 1_000_000.0) as i64).into())
            }
            // For other types (Interval, Uuid), match Display format
            _ => Value::String(self.to_string().trim_matches('\'').to_string()),
        }
    }

    unsafe fn from_composite_datum(datum: Datum) -> Option<Self> {
        unsafe {
            let datum_header = PgWrapper::datum_get_heap_tuple_header(datum);
            let tup_type = PgWrapper::heap_tuple_header_get_type_id(datum_header);
            let tup_typmod = PgWrapper::heap_tuple_header_get_typmod(datum_header);
            let tup_desc = pg_sys::lookup_rowtype_tupdesc(tup_type, tup_typmod);

            let mut tuple_data: pg_sys::HeapTupleData = std::mem::zeroed();
            tuple_data.t_len =
                PgWrapper::heap_tuple_header_get_datum_length(datum_header);
            tuple_data.t_data = datum_header;

            let natts = (*tup_desc).natts as usize;
            let mut values = vec![std::mem::zeroed::<Datum>(); natts];
            let mut nulls = vec![false; natts];

            pg_sys::heap_deform_tuple(
                &mut tuple_data,
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);
            let mut map = serde_json::Map::new();

            for i in 0..natts {
                let attr = &attrs[i];
                if attr.attisdropped {
                    continue;
                }

                let name = CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_string_lossy()
                    .to_string();

                let val = if nulls[i] {
                    serde_json::Value::Null
                } else {
                    Cell::from_polymorphic_datum(values[i], false, attr.atttypid)
                        .map(|c| c.to_json_value())
                        .unwrap_or(serde_json::Value::Null)
                };

                map.insert(name, val);
            }

            PgWrapper::release_tuple_desc(tup_desc);
            Some(Cell::Composite(JsonB(serde_json::Value::Object(map))))
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
            Cell::Date(v) => Cell::Date(*v),
            Cell::Time(v) => Cell::Time(*v),
            Cell::Timestamp(v) => Cell::Timestamp(*v),
            Cell::Timestamptz(v) => Cell::Timestamptz(*v),
            Cell::Interval(v) => Cell::Interval(*v),
            Cell::Json(v) => Cell::Json(JsonB(v.0.clone())),
            Cell::Composite(v) => Cell::Composite(JsonB(v.0.clone())),
            Cell::Bytea(v) => Cell::Bytea(v.clone()),
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
            Cell::Json(v) | Cell::Composite(v) => write!(f, "{:?}", v),
            Cell::Bytea(v) => {
                let hex = v
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<String>>()
                    .join("");
                if hex.is_empty() {
                    write!(f, "''")
                } else {
                    write!(f, r#"'\x{}'"#, hex)
                }
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
    /// This method is needed for composite types where the target column type
    /// must be known to create the correct Datum format. For `Cell::Composite`,
    /// if the target type is a composite type, the JSON data will be converted
    /// to the proper HeapTuple format expected by PostgreSQL.
    ///
    /// # Arguments
    /// * `typoid` - The target PostgreSQL type OID
    /// * `typmod` - The target type modifier
    ///
    /// # Safety
    /// This function is unsafe because it calls PostgreSQL internal functions.
    pub unsafe fn into_datum_typed(self, typoid: Oid, typmod: i32) -> Option<Datum> {
        unsafe {
            match self {
                Cell::Composite(ref jsonb) => {
                    // Check if target type is composite
                    let typtype = pg_sys::get_typtype(typoid);
                    if typtype as u8 == pg_sys::TYPTYPE_COMPOSITE {
                        // Convert JSON to composite datum
                        return Self::json_to_composite_datum(
                            &jsonb.0, typoid, typmod,
                        );
                    }
                }
                _ => {}
            }
            // Fallback to the regular into_datum for all other cases
            self.into_datum()
        }
    }

    /// Convert a JSON value to a PostgreSQL composite type Datum.
    ///
    /// This function takes a JSON object and converts it to a HeapTuple
    /// that matches the target composite type's structure.
    unsafe fn json_to_composite_datum(
        json: &serde_json::Value,
        typoid: Oid,
        typmod: i32,
    ) -> Option<Datum> {
        unsafe {
            use serde_json::Value as JsonValue;

            let obj = match json {
                JsonValue::Object(map) => map,
                JsonValue::Null => return None,
                _ => return None, // Not a valid composite representation
            };

            // Get the TupleDesc for the target composite type
            let tup_desc = pg_sys::lookup_rowtype_tupdesc(typoid, typmod);
            if tup_desc.is_null() {
                return None;
            }

            let natts = (*tup_desc).natts as usize;
            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

            // Prepare values and nulls arrays
            let mut values: Vec<Datum> = vec![Datum::from(0); natts];
            let mut nulls: Vec<bool> = vec![true; natts]; // Start with all nulls

            for i in 0..natts {
                let attr = &attrs[i];
                if attr.attisdropped {
                    continue;
                }

                // Get field name
                let field_name = CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_string_lossy()
                    .to_string();

                // Look up the field in the JSON object
                if let Some(json_val) = obj.get(&field_name) {
                    if !json_val.is_null() {
                        // Convert JSON value to Cell, then to Datum
                        if let Some(cell) =
                            Self::cell_from_json(json_val, attr.atttypid)
                        {
                            // Recursively handle nested composite types
                            if let Some(datum) =
                                cell.into_datum_typed(attr.atttypid, attr.atttypmod)
                            {
                                values[i] = datum;
                                nulls[i] = false;
                            }
                        }
                    }
                }
            }

            // Build the HeapTuple
            let tuple = pg_sys::heap_form_tuple(
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            // Release the TupleDesc reference
            PgWrapper::release_tuple_desc(tup_desc);

            if tuple.is_null() {
                return None;
            }

            // Convert HeapTuple to Datum (HeapTupleHeader)
            Some(pg_sys::HeapTupleHeaderGetDatum((*tuple).t_data))
        }
    }

    /// Convert a JSON value to a Cell based on the target PostgreSQL type.
    ///
    /// This function is the inverse of `to_json_value`. It must be able to
    /// reconstruct the original Cell from the JSON representation.
    fn cell_from_json(json: &serde_json::Value, typoid: Oid) -> Option<Cell> {
        use serde_json::Value as JsonValue;

        match PgOid::from(typoid) {
            // Boolean
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => json.as_bool().map(Cell::Bool),

            // Integer types
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
                json.as_i64().map(|v| Cell::I8(v as i8))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                json.as_i64().map(|v| Cell::I16(v as i16))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                json.as_i64().map(|v| Cell::I32(v as i32))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => json.as_i64().map(Cell::I64),

            // Floating point types
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                json.as_f64().map(|v| Cell::F32(v as f32))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => json.as_f64().map(Cell::F64),

            // Numeric (stored as string in JSON)
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                let s = match json {
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::String(s) => s.clone(),
                    _ => return None,
                };
                unsafe {
                    let c_str = std::ffi::CString::new(s).ok()?;
                    let args = vec![
                        Some(pg_sys::Datum::from(c_str.as_ptr())),
                        pg_sys::InvalidOid.into_datum(),
                        (-1i32).into_datum(),
                    ];
                    let datum = fcinfo::direct_function_call_as_datum(
                        pg_sys::numeric_in,
                        &args,
                    )?;
                    AnyNumeric::from_datum(datum, false).map(Cell::Numeric)
                }
            }

            // String types
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
            | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
            | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
            | PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => match json {
                JsonValue::String(s) => Some(Cell::String(s.clone())),
                JsonValue::Number(n) => Some(Cell::String(n.to_string())),
                JsonValue::Bool(b) => Some(Cell::String(b.to_string())),
                _ => None,
            },

            // Bytea (stored as hex string in JSON)
            PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                let hex_str = json.as_str()?;
                // Parse hex string to bytes
                if hex_str.len() % 2 != 0 {
                    return None;
                }
                let bytes: Option<Vec<u8>> = (0..hex_str.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).ok())
                    .collect();
                bytes.map(Cell::Bytea)
            }

            // Date (stored as days since Unix epoch in JSON)
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                // to_json_value outputs: pg_days + 10957 (days since Unix epoch)
                // We need to reverse: pg_days = json_days - 10957
                const PG_EPOCH_DAYS: i32 = 10957;
                let arrow_days = json.as_i64()? as i32;
                let pg_days = arrow_days - PG_EPOCH_DAYS;
                // SAFETY: pg_days is a valid date value
                Some(Cell::Date(unsafe { Date::from_pg_epoch_days(pg_days) }))
            }

            // Time (stored as microseconds since midnight in JSON)
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                let micros = json.as_i64()?;
                Some(Cell::Time(Time::modular_from_raw(micros)))
            }

            // Timestamp (stored as microseconds since Unix epoch in JSON)
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                // to_json_value outputs: microseconds since Unix epoch (1970)
                // PostgreSQL Timestamp is microseconds since 2000-01-01
                const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
                let unix_micros = json.as_i64()?;
                let pg_micros = unix_micros - PG_EPOCH_MICROS;
                Some(Cell::Timestamp(Timestamp::saturating_from_raw(pg_micros)))
            }

            // Timestamptz (stored as microseconds since Unix epoch in JSON)
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
                let unix_micros = json.as_i64()?;
                let pg_micros = unix_micros - PG_EPOCH_MICROS;
                // SAFETY: TimestampWithTimeZone is a wrapper around i64
                Some(Cell::Timestamptz(unsafe {
                    mem::transmute::<i64, TimestampWithTimeZone>(pg_micros)
                }))
            }

            // Interval (stored as string in JSON via Display)
            PgOid::BuiltIn(PgBuiltInOids::INTERVALOID) => {
                let s = json.as_str()?;
                unsafe {
                    let c_str = std::ffi::CString::new(s).ok()?;
                    let args = vec![
                        Some(pg_sys::Datum::from(c_str.as_ptr())),
                        pg_sys::InvalidOid.into_datum(),
                        (-1i32).into_datum(),
                    ];
                    let datum = fcinfo::direct_function_call_as_datum(
                        pg_sys::interval_in,
                        &args,
                    )?;
                    Interval::from_datum(datum, false).map(Cell::Interval)
                }
            }

            // UUID (stored as string in JSON)
            PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => {
                let s = json.as_str()?;
                let uuid = uuid::Uuid::parse_str(s).ok()?;
                Some(Cell::Uuid(Uuid::from_bytes(*uuid.as_bytes())))
            }

            // JSONB (binary storage)
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                Some(Cell::Json(JsonB(json.clone())))
            }

            // JSON (text storage) - stored as string, same as in from_polymorphic_datum
            PgOid::BuiltIn(PgBuiltInOids::JSONOID) => {
                // JSON type is stored as text in PostgreSQL, so we return it as a string
                Some(Cell::String(json.to_string()))
            }

            // Array types
            PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<bool>> = arr
                    .iter()
                    .map(|v| if v.is_null() { None } else { v.as_bool() })
                    .collect();
                Some(Cell::BoolArray(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<i16>> = arr
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_i64().map(|n| n as i16)
                        }
                    })
                    .collect();
                Some(Cell::I16Array(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<i32>> = arr
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_i64().map(|n| n as i32)
                        }
                    })
                    .collect();
                Some(Cell::I32Array(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<i64>> = arr
                    .iter()
                    .map(|v| if v.is_null() { None } else { v.as_i64() })
                    .collect();
                Some(Cell::I64Array(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<f32>> = arr
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_f64().map(|n| n as f32)
                        }
                    })
                    .collect();
                Some(Cell::F32Array(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<f64>> = arr
                    .iter()
                    .map(|v| if v.is_null() { None } else { v.as_f64() })
                    .collect();
                Some(Cell::F64Array(vec))
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID)
            | PgOid::BuiltIn(PgBuiltInOids::VARCHARARRAYOID)
            | PgOid::BuiltIn(PgBuiltInOids::BPCHARARRAYOID)
            | PgOid::BuiltIn(PgBuiltInOids::NAMEARRAYOID) => {
                let arr = json.as_array()?;
                let vec: Vec<Option<String>> = arr
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_str().map(|s| s.to_string())
                        }
                    })
                    .collect();
                Some(Cell::StringArray(vec))
            }

            // Fallback for composite types and others
            _ => {
                let typtype = unsafe { pg_sys::get_typtype(typoid) };
                if typtype as u8 == pg_sys::TYPTYPE_COMPOSITE {
                    Some(Cell::Composite(JsonB(json.clone())))
                } else {
                    // Fallback: try to use as JSON
                    Some(Cell::Json(JsonB(json.clone())))
                }
            }
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
            Cell::Date(v) => v.into_datum(),
            Cell::Time(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Timestamptz(v) => v.into_datum(),
            Cell::Interval(v) => v.into_datum(),
            Cell::Json(v) => v.into_datum(),
            Cell::Bytea(v) => v.as_slice().into_datum(),
            Cell::Uuid(v) => v.into_datum(),
            Cell::Composite(_) => None,
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
            || unsafe {
                pg_sys::get_typtype(other) == pg_sys::TYPTYPE_COMPOSITE as i8
            }
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
                | PgOid::BuiltIn(PgBuiltInOids::NAMEOID)
                | PgOid::BuiltIn(PgBuiltInOids::JSONOID) => {
                    String::from_datum(datum, is_null).map(Cell::String)
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
                    JsonB::from_datum(datum, is_null).map(Cell::Json)
                }
                PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                    let ptr = datum.cast_mut_ptr::<bytea>();
                    if ptr.is_null() {
                        None
                    } else {
                        // SAFETY: ptr is a valid pointer to varlena because it comes from a Datum of type BYTEAOID
                        let slice = pgrx::varlena::varlena_to_byte_slice(ptr);
                        Some(Cell::Bytea(slice.to_vec()))
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
                _ => {
                    let typtype = pg_sys::get_typtype(typoid);
                    if typtype as u8 == pg_sys::TYPTYPE_COMPOSITE {
                        Cell::from_composite_datum(datum)
                    } else {
                        None
                    }
                }
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
