use std::sync::Arc;

use super::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
use crate::access::traits::{ArrowToCell, RowsToArrow};
use crate::error::{IcebergError, IcebergResult};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef};
use iceberg_lite::spec::PrimitiveType;
use pg_lakebase_core::data::{ByteaView, Cell, Row, StringView};
use pgrx::datum::Uuid;
use pgrx::datum::datetime_support::{DateTimeParts, HasExtractableParts};
use pgrx::prelude::{AnyNumeric, Date, Time, Timestamp, TimestampWithTimeZone};

/// Implementation of ArrowToCell for PrimitiveType.
impl ArrowToCell for PrimitiveType {
    fn extract(
        &self,
        column: &dyn Array,
        row_idx: usize,
    ) -> IcebergResult<Option<Cell>> {
        match self {
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
            PrimitiveType::String => {
                let s = match column.data_type() {
                    arrow_schema::DataType::Utf8 => {
                        column.as_string::<i32>().value(row_idx)
                    }
                    arrow_schema::DataType::LargeUtf8 => {
                        column.as_string::<i64>().value(row_idx)
                    }
                    _ => {
                        return Err(IcebergError::ArrowTypeMismatch(format!(
                            "String or LargeString (actual: {:?})",
                            column.data_type()
                        )));
                    }
                };
                Ok(Some(Cell::StringView(StringView {
                    ptr: s.as_ptr(),
                    len: s.len(),
                })))
            }
            PrimitiveType::Binary => {
                let bytes = match column.data_type() {
                    arrow_schema::DataType::Binary => {
                        column.as_binary::<i32>().value(row_idx)
                    }
                    arrow_schema::DataType::LargeBinary => {
                        column.as_binary::<i64>().value(row_idx)
                    }
                    _ => {
                        return Err(IcebergError::ArrowTypeMismatch(format!(
                            "Binary or LargeBinary (actual: {:?})",
                            column.data_type()
                        )));
                    }
                };
                Ok(Some(Cell::ByteaView(ByteaView {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                })))
            }
            PrimitiveType::Fixed(_) => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch(
                        "FixedSizeBinary".to_string(),
                    ))?;
                let bytes = array.value(row_idx);
                Ok(Some(Cell::ByteaView(ByteaView {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                })))
            }
            PrimitiveType::Date => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Date32Array>()
                    .ok_or(IcebergError::ArrowTypeMismatch("Date32".to_string()))?;
                let arrow_days = array.value(row_idx);
                let pg_days = arrow_days - PG_EPOCH_DAYS_DIFF;
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
                let array = column.as_primitive::<TimestampMicrosecondType>();
                let unix_micros = array.value(row_idx);
                let pg_micros = unix_micros - PG_EPOCH_USECS_DIFF;
                Ok(Some(Cell::Timestamp(Timestamp::saturating_from_raw(
                    pg_micros,
                ))))
            }
            PrimitiveType::TimestampNs => {
                let array = column.as_primitive::<TimestampNanosecondType>();
                let unix_nanos = array.value(row_idx);
                let pg_micros = (unix_nanos / 1000) - PG_EPOCH_USECS_DIFF;
                Ok(Some(Cell::Timestamp(Timestamp::saturating_from_raw(
                    pg_micros,
                ))))
            }
            PrimitiveType::Timestamptz => {
                let array = column.as_primitive::<TimestampMicrosecondType>();
                let unix_micros = array.value(row_idx);
                let pg_micros = unix_micros - PG_EPOCH_USECS_DIFF;
                Ok(Some(Cell::Timestamptz(unsafe {
                    std::mem::transmute::<i64, TimestampWithTimeZone>(pg_micros)
                })))
            }
            PrimitiveType::TimestamptzNs => {
                let array = column.as_primitive::<TimestampNanosecondType>();
                let unix_nanos = array.value(row_idx);
                let pg_micros = (unix_nanos / 1000) - PG_EPOCH_USECS_DIFF;
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
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            } => {
                let array =
                    column.as_primitive::<arrow_array::types::Decimal128Type>();
                let val_str = array.value_as_string(row_idx);
                let numeric =
                    AnyNumeric::try_from(val_str.as_str()).map_err(|e| {
                        IcebergError::ArrowTypeMismatch(format!(
                            "Failed to convert decimal string '{}' to AnyNumeric: {}",
                            val_str, e
                        ))
                    })?;

                Ok(Some(Cell::Numeric(numeric)))
            }
        }
    }
}

/// Implementation of RowsToArrow for PrimitiveType.
impl RowsToArrow for PrimitiveType {
    fn build(&self, rows: &[Row], col_idx: usize) -> IcebergResult<ArrayRef> {
        macro_rules! build_array {
            ($builder_ty:ty, [ $($pattern:pat => $value:expr),+ ]) => {{
                let mut builder = <$builder_ty>::with_capacity(rows.len());
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        $($pattern => builder.append_value($value),)+
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }};
        }

        match self {
            PrimitiveType::Boolean => build_array!(
                arrow_array::builder::BooleanBuilder,
                [Some(Cell::Bool(v)) => *v]
            ),
            PrimitiveType::Int => build_array!(
                arrow_array::builder::Int32Builder,
                [
                    Some(Cell::I32(v)) => *v,
                    Some(Cell::I16(v)) => *v as i32,
                    Some(Cell::I8(v)) => *v as i32
                ]
            ),
            PrimitiveType::Long => build_array!(
                arrow_array::builder::Int64Builder,
                [
                    Some(Cell::I64(v)) => *v,
                    Some(Cell::I32(v)) => *v as i64,
                    Some(Cell::I16(v)) => *v as i64
                ]
            ),
            PrimitiveType::Float => build_array!(
                arrow_array::builder::Float32Builder,
                [Some(Cell::F32(v)) => *v]
            ),
            PrimitiveType::Double => build_array!(
                arrow_array::builder::Float64Builder,
                [
                    Some(Cell::F64(v)) => *v,
                    Some(Cell::F32(v)) => *v as f64
                ]
            ),
            PrimitiveType::String => {
                let mut builder = arrow_array::builder::StringBuilder::with_capacity(
                    rows.len(),
                    1024,
                );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::String(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Binary => {
                let mut builder = arrow_array::builder::BinaryBuilder::with_capacity(
                    rows.len(),
                    1024,
                );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Bytea(bytes)) => builder.append_value(bytes),
                        Some(Cell::Json(bytes)) => {
                            builder.append_value(bytes);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Fixed(len) => {
                let fixed_len = *len as usize;
                let mut builder =
                    arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(
                        rows.len(),
                        *len as i32,
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Bytea(bytes)) => {
                            let mut fixed = vec![0u8; fixed_len];
                            let copy_len = bytes.len().min(fixed_len);
                            fixed[..copy_len].copy_from_slice(&bytes[..copy_len]);
                            builder
                                .append_value(&fixed)
                                .map_err(IcebergError::ArrowError)?;
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Date => build_array!(
                arrow_array::builder::Date32Builder,
                [Some(Cell::Date(d)) => d.to_pg_epoch_days() + PG_EPOCH_DAYS_DIFF]
            ),
            PrimitiveType::Time => build_array!(
                arrow_array::builder::Time64MicrosecondBuilder,
                [Some(Cell::Time(t)) => {
                    let hour = t
                        .extract_part(DateTimeParts::Hour)
                        .and_then(|n| n.try_into().ok())
                        .unwrap_or(0i64);
                    let minute = t
                        .extract_part(DateTimeParts::Minute)
                        .and_then(|n| n.try_into().ok())
                        .unwrap_or(0i64);
                    let second: f64 = t
                        .extract_part(DateTimeParts::Second)
                        .and_then(|n| n.try_into().ok())
                        .unwrap_or(0.0);
                    hour * 3_600_000_000
                        + minute * 60_000_000
                        + (second * 1_000_000.0) as i64
                }]
            ),
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => build_array!(
                arrow_array::builder::TimestampMicrosecondBuilder,
                [Some(Cell::Timestamp(ts)) => {
                    let epoch: f64 = ts
                        .extract_part(DateTimeParts::Epoch)
                        .and_then(|n| n.try_into().ok())
                        .unwrap_or(0.0);
                    (epoch * 1_000_000.0) as i64
                }]
            ),
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                let mut builder =
                    arrow_array::builder::TimestampMicrosecondBuilder::with_capacity(
                        rows.len(),
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Timestamptz(ts)) => {
                            let epoch: f64 = ts
                                .extract_part(DateTimeParts::Epoch)
                                .and_then(|n| n.try_into().ok())
                                .unwrap_or(0.0);
                            builder.append_value((epoch * 1_000_000.0) as i64);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish().with_timezone("+00:00")))
            }
            PrimitiveType::Uuid => {
                let mut builder =
                    arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(
                        rows.len(),
                        16,
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Uuid(u)) => {
                            builder
                                .append_value(u.as_bytes())
                                .map_err(IcebergError::ArrowError)?;
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Decimal { precision, scale } => {
                let target_scale = *scale as u32;
                let mut builder =
                    arrow_array::builder::Decimal128Builder::with_capacity(
                        rows.len(),
                    )
                    .with_precision_and_scale(*precision as u8, *scale as i8)
                    .map_err(IcebergError::ArrowError)?;
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Numeric(n)) => {
                            let scale_factor = 10_i128.pow(target_scale);
                            let scaled = n.clone() * scale_factor;
                            let truncated = scaled.floor();
                            match i128::try_from(truncated) {
                                Ok(v) => builder.append_value(v),
                                Err(_) => builder.append_null(),
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }
}
