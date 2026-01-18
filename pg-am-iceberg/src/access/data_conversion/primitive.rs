use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use iceberg_lite::spec::PrimitiveType;
use pg_lakehouse_core::data::{Cell, Row};
use pgrx::datum::datetime_support::{DateTimeParts, HasExtractableParts};
use serde_json::Value as JsonValue;

use crate::error::{IcebergError, IcebergResult};

/// Build an Arrow array for primitive types.
pub(crate) fn build_primitive_array(
    rows: &[Row],
    col_idx: usize,
    primitive_type: &PrimitiveType,
) -> IcebergResult<ArrayRef> {
    match primitive_type {
        PrimitiveType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Bool(v) => Some(*v),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        PrimitiveType::Int => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::I32(v) => Some(*v),
                            Cell::I16(v) => Some(*v as i32),
                            Cell::I8(v) => Some(*v as i32),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        PrimitiveType::Long => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::I64(v) => Some(*v),
                            Cell::I32(v) => Some(*v as i64),
                            Cell::I16(v) => Some(*v as i64),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        PrimitiveType::Float => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::F32(v) => Some(*v),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        PrimitiveType::Double => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::F64(v) => Some(*v),
                            Cell::F32(v) => Some(*v as f64),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        PrimitiveType::String => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::String(v) => Some(v.clone()),
                            Cell::Json(v) | Cell::Composite(v) => {
                                Some(v.0.to_string())
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        PrimitiveType::Binary => {
            let values_iter = rows.iter().map(|row| {
                row.get(col_idx).and_then(|cell| cell.as_ref()).and_then(
                    |c| match c {
                        Cell::Bytea(bytes) => Some(bytes.clone()),
                        _ => None,
                    },
                )
            });
            Ok(Arc::new(BinaryArray::from_iter(values_iter)))
        }
        PrimitiveType::Fixed(len) => {
            let fixed_len = *len as usize;
            let values: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Bytea(bytes) => {
                                let mut fixed = vec![0u8; fixed_len];
                                let copy_len = bytes.len().min(fixed_len);
                                fixed[..copy_len].copy_from_slice(&bytes[..copy_len]);
                                Some(fixed)
                            }
                            _ => None,
                        })
                })
                .collect();

            let array =
                arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values.into_iter(),
                    *len as i32,
                )
                .map_err(IcebergError::ArrowError)?;
            Ok(Arc::new(array))
        }
        PrimitiveType::Date => {
            const PG_EPOCH_DAYS: i32 = 10957;
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Date(d) => {
                                let pg_days = d.to_pg_epoch_days();
                                Some(pg_days + PG_EPOCH_DAYS)
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        PrimitiveType::Time => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Time(t) => {
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
                                let micros = hour * 3_600_000_000
                                    + minute * 60_000_000
                                    + (second * 1_000_000.0) as i64;
                                Some(micros)
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(Time64MicrosecondArray::from(values)))
        }
        PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Timestamp(ts) => {
                                let epoch: f64 = ts
                                    .extract_part(DateTimeParts::Epoch)
                                    .and_then(|n| n.try_into().ok())
                                    .unwrap_or(0.0);
                                Some((epoch * 1_000_000.0) as i64)
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Timestamptz(ts) => {
                                let epoch: f64 = ts
                                    .extract_part(DateTimeParts::Epoch)
                                    .and_then(|n| n.try_into().ok())
                                    .unwrap_or(0.0);
                                Some((epoch * 1_000_000.0) as i64)
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone("+00:00"),
            ))
        }
        PrimitiveType::Uuid => {
            let values: Vec<Option<[u8; 16]>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Uuid(u) => Some(*u.as_bytes()),
                            _ => None,
                        })
                })
                .collect();
            let array =
                arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values.into_iter(),
                    16,
                )
                .map_err(IcebergError::ArrowError)?;
            Ok(Arc::new(array))
        }
        PrimitiveType::Decimal { precision, scale } => {
            let target_scale = *scale as u32;
            let values: Vec<Option<i128>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|cell| cell.as_ref())
                        .and_then(|c| match c {
                            Cell::Numeric(n) => {
                                let s = n.to_string();
                                match rust_decimal::Decimal::from_str_exact(&s) {
                                    Ok(dec) => {
                                        let mut scaled = dec;
                                        scaled.rescale(target_scale);
                                        Some(scaled.mantissa())
                                    }
                                    Err(_) => None,
                                }
                            }
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(
                arrow_array::Decimal128Array::from(values)
                    .with_precision_and_scale(*precision as u8, *scale as i8)
                    .map_err(IcebergError::ArrowError)?,
            ))
        }
    }
}

pub(crate) fn build_primitive_array_from_json(
    values: &[Option<&JsonValue>],
    primitive_type: &PrimitiveType,
) -> IcebergResult<ArrayRef> {
    match primitive_type {
        PrimitiveType::Boolean => {
            let v: Vec<Option<bool>> =
                values.iter().map(|o| o.and_then(|j| j.as_bool())).collect();
            Ok(Arc::new(BooleanArray::from(v)))
        }
        PrimitiveType::Int => {
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|o| o.and_then(|j| j.as_i64().map(|n| n as i32)))
                .collect();
            Ok(Arc::new(Int32Array::from(v)))
        }
        PrimitiveType::Long => {
            let v: Vec<Option<i64>> =
                values.iter().map(|o| o.and_then(|j| j.as_i64())).collect();
            Ok(Arc::new(Int64Array::from(v)))
        }
        PrimitiveType::Float => {
            let v: Vec<Option<f32>> = values
                .iter()
                .map(|o| o.and_then(|j| j.as_f64().map(|n| n as f32)))
                .collect();
            Ok(Arc::new(Float32Array::from(v)))
        }
        PrimitiveType::Double => {
            let v: Vec<Option<f64>> =
                values.iter().map(|o| o.and_then(|j| j.as_f64())).collect();
            Ok(Arc::new(Float64Array::from(v)))
        }
        PrimitiveType::String => {
            let v: Vec<Option<String>> = values
                .iter()
                .map(|o| {
                    o.and_then(|j| match j {
                        JsonValue::String(s) => Some(s.clone()),
                        JsonValue::Number(n) => Some(n.to_string()),
                        JsonValue::Bool(b) => Some(b.to_string()),
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(v)))
        }
        PrimitiveType::Binary => {
            let v: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|o| {
                    o.and_then(|j| j.as_str()).and_then(|s| {
                        if s.len() % 2 != 0 {
                            return None;
                        }
                        (0..s.len())
                            .step_by(2)
                            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
                            .collect()
                    })
                })
                .collect();
            Ok(Arc::new(BinaryArray::from_iter(
                v.iter().map(|opt| opt.as_deref()),
            )))
        }
        PrimitiveType::Fixed(len) => {
            let fixed_len = *len as usize;
            let v: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|o| {
                    o.and_then(|j| j.as_str()).and_then(|s| {
                        if s.len() % 2 != 0 {
                            return None;
                        }
                        let bytes: Option<Vec<u8>> = (0..s.len())
                            .step_by(2)
                            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
                            .collect();
                        bytes.map(|b| {
                            let mut fixed = vec![0u8; fixed_len];
                            let copy_len = b.len().min(fixed_len);
                            fixed[..copy_len].copy_from_slice(&b[..copy_len]);
                            fixed
                        })
                    })
                })
                .collect();
            let array =
                arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    v.into_iter(),
                    *len as i32,
                )
                .map_err(IcebergError::ArrowError)?;
            Ok(Arc::new(array))
        }
        PrimitiveType::Date => {
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|o| o.and_then(|j| j.as_i64().map(|n| n as i32)))
                .collect();
            Ok(Arc::new(Date32Array::from(v)))
        }
        PrimitiveType::Time => {
            let v: Vec<Option<i64>> =
                values.iter().map(|o| o.and_then(|j| j.as_i64())).collect();
            Ok(Arc::new(Time64MicrosecondArray::from(v)))
        }
        PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
            let v: Vec<Option<i64>> =
                values.iter().map(|o| o.and_then(|j| j.as_i64())).collect();
            Ok(Arc::new(TimestampMicrosecondArray::from(v)))
        }
        PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
            let v: Vec<Option<i64>> =
                values.iter().map(|o| o.and_then(|j| j.as_i64())).collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(v).with_timezone("+00:00"),
            ))
        }
        PrimitiveType::Uuid => {
            let v: Vec<Option<[u8; 16]>> = values
                .iter()
                .map(|o| {
                    let s = o.and_then(|j| j.as_str())?;
                    uuid::Uuid::parse_str(s).ok().map(|u| *u.as_bytes())
                })
                .collect();
            let array =
                arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    v.into_iter(),
                    16,
                )
                .map_err(IcebergError::ArrowError)?;
            Ok(Arc::new(array))
        }
        PrimitiveType::Decimal { precision, scale } => {
            let target_scale = *scale as u32;
            let v: Vec<Option<i128>> = values
                .iter()
                .map(|o| {
                    let s = match *o {
                        Some(JsonValue::String(s)) => s.clone(),
                        Some(JsonValue::Number(n)) => n.to_string(),
                        _ => return None,
                    };
                    let dec = rust_decimal::Decimal::from_str_exact(&s).ok()?;
                    let mut scaled = dec;
                    scaled.rescale(target_scale);
                    Some(scaled.mantissa())
                })
                .collect();
            Ok(Arc::new(
                arrow_array::Decimal128Array::from(v)
                    .with_precision_and_scale(*precision as u8, *scale as i8)
                    .map_err(IcebergError::ArrowError)?,
            ))
        }
    }
}
