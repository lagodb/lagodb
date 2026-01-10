use std::ffi::CString;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use iceberg_lite::spec::PrimitiveType;
use pg_tam::data::Cell;
use pgrx::datum::Uuid;
use pgrx::prelude::{AnyNumeric, Date, Time, Timestamp, TimestampWithTimeZone};
use pgrx::{FromDatum, IntoDatum, fcinfo, pg_sys};

use super::json::format_decimal_to_string;
use crate::error::{IcebergError, IcebergResult};

/// Extract a primitive cell value from an Arrow column.
pub(crate) fn extract_primitive_cell(
    column: &dyn Array,
    row_idx: usize,
    primitive_type: &PrimitiveType,
) -> IcebergResult<Option<Cell>> {
    match primitive_type {
        PrimitiveType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or(IcebergError::ArrowTypeMismatch("Boolean".to_string()))?;
            Ok(Some(Cell::Bool(array.value(row_idx))))
        }
        PrimitiveType::Int => {
            let array = column.as_primitive::<Int32Type>();
            Ok(Some(Cell::I32(array.value(row_idx))))
        }
        PrimitiveType::Long => {
            let array = column.as_primitive::<Int64Type>();
            Ok(Some(Cell::I64(array.value(row_idx))))
        }
        PrimitiveType::Float => {
            let array = column.as_primitive::<Float32Type>();
            Ok(Some(Cell::F32(array.value(row_idx))))
        }
        PrimitiveType::Double => {
            let array = column.as_primitive::<Float64Type>();
            Ok(Some(Cell::F64(array.value(row_idx))))
        }
        PrimitiveType::String => match column.data_type() {
            arrow_schema::DataType::Utf8 => {
                let array = column.as_string::<i32>();
                Ok(Some(Cell::String(array.value(row_idx).to_string())))
            }
            arrow_schema::DataType::LargeUtf8 => {
                let array = column.as_string::<i64>();
                Ok(Some(Cell::String(array.value(row_idx).to_string())))
            }
            _ => Err(IcebergError::ArrowTypeMismatch(format!(
                "String or LargeString (actual: {:?})",
                column.data_type()
            ))),
        },
        PrimitiveType::Binary => {
            if let Some(array) =
                column.as_any().downcast_ref::<arrow_array::BinaryArray>()
            {
                let bytes = array.value(row_idx);
                Ok(Some(Cell::Bytea(bytes.to_vec())))
            } else if let Some(array) = column
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
            {
                let bytes = array.value(row_idx);
                Ok(Some(Cell::Bytea(bytes.to_vec())))
            } else {
                Err(IcebergError::ArrowTypeMismatch(format!(
                    "Binary or LargeBinary (actual: {:?})",
                    column.data_type()
                )))
            }
        }
        PrimitiveType::Fixed(_) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .ok_or(IcebergError::ArrowTypeMismatch(
                    "FixedSizeBinary".to_string(),
                ))?;
            let bytes = array.value(row_idx);
            Ok(Some(Cell::Bytea(bytes.to_vec())))
        }
        PrimitiveType::Date => {
            const PG_EPOCH_DAYS: i32 = 10957;
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .ok_or(IcebergError::ArrowTypeMismatch("Date32".to_string()))?;
            let arrow_days = array.value(row_idx);
            let pg_days = arrow_days - PG_EPOCH_DAYS;
            Ok(Some(Cell::Date(unsafe {
                Date::from_pg_epoch_days(pg_days)
            })))
        }
        PrimitiveType::Time => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .ok_or(IcebergError::ArrowTypeMismatch(
                    "Time64Microsecond".to_string(),
                ))?;
            let micros = array.value(row_idx);
            Ok(Some(Cell::Time(Time::modular_from_raw(micros))))
        }
        PrimitiveType::Timestamp => {
            const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
            let array = column.as_primitive::<TimestampMicrosecondType>();
            let unix_micros = array.value(row_idx);
            let pg_micros = unix_micros - PG_EPOCH_MICROS;
            Ok(Some(Cell::Timestamp(Timestamp::saturating_from_raw(
                pg_micros,
            ))))
        }
        PrimitiveType::TimestampNs => {
            const PG_EPOCH_NANOS: i64 = 946_684_800_000_000_000;
            let array = column.as_primitive::<TimestampNanosecondType>();
            let unix_nanos = array.value(row_idx);
            let pg_micros = (unix_nanos - PG_EPOCH_NANOS) / 1000;
            Ok(Some(Cell::Timestamp(Timestamp::saturating_from_raw(
                pg_micros,
            ))))
        }
        PrimitiveType::Timestamptz => {
            const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
            let array = column.as_primitive::<TimestampMicrosecondType>();
            let unix_micros = array.value(row_idx);
            let pg_micros = unix_micros - PG_EPOCH_MICROS;
            Ok(Some(Cell::Timestamptz(unsafe {
                std::mem::transmute::<i64, TimestampWithTimeZone>(pg_micros)
            })))
        }
        PrimitiveType::TimestamptzNs => {
            const PG_EPOCH_NANOS: i64 = 946_684_800_000_000_000;
            let array = column.as_primitive::<TimestampNanosecondType>();
            let unix_nanos = array.value(row_idx);
            let pg_micros = (unix_nanos - PG_EPOCH_NANOS) / 1000;
            Ok(Some(Cell::Timestamptz(unsafe {
                std::mem::transmute::<i64, TimestampWithTimeZone>(pg_micros)
            })))
        }
        PrimitiveType::Uuid => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .ok_or(IcebergError::ArrowTypeMismatch(
                    "FixedSizeBinary (UUID)".to_string(),
                ))?;
            let bytes = array.value(row_idx);
            if bytes.len() != 16 {
                return Err(IcebergError::ArrowTypeMismatch(
                    "UUID must be 16 bytes".to_string(),
                ));
            }
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(bytes);
            Ok(Some(Cell::Uuid(Uuid::from_bytes(uuid_bytes))))
        }
        PrimitiveType::Decimal { precision, scale } => {
            extract_decimal_cell(column, row_idx, *precision, *scale)
        }
    }
}

/// Extract a Decimal cell from an Arrow Decimal128 column.
pub(crate) fn extract_decimal_cell(
    column: &dyn Array,
    row_idx: usize,
    precision: u32,
    scale: u32,
) -> IcebergResult<Option<Cell>> {
    let array = column.as_primitive::<Decimal128Type>();
    let mantissa = array.value(row_idx);

    let numeric_str =
        format_decimal_to_string(mantissa, precision as u8, scale as i8);

    let c_str = CString::new(numeric_str).map_err(|_| {
        IcebergError::ArrowTypeMismatch(
            "Failed to create CString for Decimal".to_string(),
        )
    })?;

    let type_mod =
        (((precision as i32) << 16) | (scale as i32)) + pg_sys::VARHDRSZ as i32;

    unsafe {
        let args = vec![
            Some(pg_sys::Datum::from(c_str.as_ptr())),
            pg_sys::InvalidOid.into_datum(),
            type_mod.into_datum(),
        ];

        let datum = fcinfo::direct_function_call_as_datum(pg_sys::numeric_in, &args)
            .ok_or(IcebergError::SpiError(
                "numeric_in returned null".to_string(),
            ))?;

        let numeric =
            AnyNumeric::from_datum(datum, false).ok_or(IcebergError::SpiError(
                "Failed to convert datum to AnyNumeric".to_string(),
            ))?;

        Ok(Some(Cell::Numeric(numeric)))
    }
}
