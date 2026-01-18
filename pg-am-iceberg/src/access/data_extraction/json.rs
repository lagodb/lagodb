use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use pgrx::datum::Uuid;
use serde_json::Value as JsonValue;
use serde_json::{Map as JsonMap, Number};

use crate::error::{IcebergError, IcebergResult};

/// Extract a complex type (Struct/Map/List) as a JSON Value.
pub(crate) fn extract_complex_type_as_json(
    column: &dyn Array,
    row_idx: usize,
) -> IcebergResult<JsonValue> {
    if column.is_null(row_idx) {
        return Ok(JsonValue::Null);
    }

    match column.data_type() {
        DataType::Struct(fields) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or(IcebergError::ArrowTypeMismatch("StructArray".to_string()))?;

            let mut map = JsonMap::new();
            for (i, field) in fields.iter().enumerate() {
                let child_array = struct_array.column(i);
                let child_value = extract_complex_type_as_json(child_array, row_idx)?;
                map.insert(field.name().clone(), child_value);
            }
            Ok(JsonValue::Object(map))
        }
        DataType::Map(_, _) => {
            let map_array =
                column
                    .as_any()
                    .downcast_ref::<arrow_array::MapArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch("MapArray".to_string()))?;

            let keys = map_array.keys();
            let values = map_array.values();

            let offset = map_array.value_offsets()[row_idx] as usize;
            let end = map_array.value_offsets()[row_idx + 1] as usize;
            let len = end - offset;

            let mut map = JsonMap::new();
            for i in 0..len {
                let key_idx = offset + i;
                let key_str = extract_key_as_string(keys, key_idx)?;
                let value = extract_complex_type_as_json(values, key_idx)?;
                map.insert(key_str, value);
            }
            Ok(JsonValue::Object(map))
        }
        DataType::List(_) => {
            let list_array = column
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .ok_or(IcebergError::ArrowTypeMismatch("ListArray".to_string()))?;

            let values_array = list_array.values();
            let offset = list_array.value_offsets()[row_idx] as usize;
            let end = list_array.value_offsets()[row_idx + 1] as usize;
            let len = end - offset;

            let mut vec = Vec::with_capacity(len);
            for i in 0..len {
                let val_idx = offset + i;
                vec.push(extract_complex_type_as_json(values_array, val_idx)?);
            }
            Ok(JsonValue::Array(vec))
        }
        _ => {
            // Primitive types
            extract_primitive_as_json(column, row_idx)
        }
    }
}

/// Helper to convert simple Arrow types to JSON value.
pub(crate) fn extract_primitive_as_json(
    column: &dyn Array,
    row_idx: usize,
) -> IcebergResult<JsonValue> {
    if column.is_null(row_idx) {
        return Ok(JsonValue::Null);
    }

    use DataType::*;
    match column.data_type() {
        Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            Ok(JsonValue::Bool(array.value(row_idx)))
        }
        Int8 => {
            let array = column.as_primitive::<arrow_array::types::Int8Type>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Int16 => {
            let array = column.as_primitive::<arrow_array::types::Int16Type>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Int32 => {
            let array = column.as_primitive::<arrow_array::types::Int32Type>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Int64 => {
            let array = column.as_primitive::<arrow_array::types::Int64Type>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Float32 => {
            let array = column.as_primitive::<arrow_array::types::Float32Type>();
            let val = array.value(row_idx);
            Number::from_f64(val as f64)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    IcebergError::DatumConversionError("NaN or Infinity float".into())
                })
        }
        Float64 => {
            let array = column.as_primitive::<arrow_array::types::Float64Type>();
            let val = array.value(row_idx);
            Number::from_f64(val).map(JsonValue::Number).ok_or_else(|| {
                IcebergError::DatumConversionError("NaN or Infinity float".into())
            })
        }
        Utf8 => {
            let array = column.as_string::<i32>();
            Ok(JsonValue::String(array.value(row_idx).to_string()))
        }
        LargeUtf8 => {
            let array = column.as_string::<i64>();
            Ok(JsonValue::String(array.value(row_idx).to_string()))
        }
        Binary | LargeBinary => {
            let bytes = if let Some(array) =
                column.as_any().downcast_ref::<arrow_array::BinaryArray>()
            {
                array.value(row_idx)
            } else {
                column
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap()
                    .value(row_idx)
            };
            let hex = bytes
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<String>();
            Ok(JsonValue::String(hex))
        }
        FixedSizeBinary(size) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .unwrap();
            let bytes = array.value(row_idx);
            if *size == 16 {
                // Try to represent as UUID if it's 16 bytes
                let mut uuid_bytes = [0u8; 16];
                uuid_bytes.copy_from_slice(bytes);
                let u = Uuid::from_bytes(uuid_bytes);
                return Ok(JsonValue::String(u.to_string()));
            }
            let hex = bytes
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<String>();
            Ok(JsonValue::String(hex))
        }
        Date32 => {
            let array = column.as_primitive::<arrow_array::types::Date32Type>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Time64(arrow_schema::TimeUnit::Microsecond) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .unwrap();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            let array =
                column.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx))))
        }
        Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            let array =
                column.as_primitive::<arrow_array::types::TimestampNanosecondType>();
            Ok(JsonValue::Number(Number::from(array.value(row_idx) / 1000)))
        }
        Decimal128(precision, scale) => {
            let array = column.as_primitive::<arrow_array::types::Decimal128Type>();
            let mantissa = array.value(row_idx);
            let s = format_decimal_to_string(mantissa, *precision, *scale as i8);
            Ok(JsonValue::String(s))
        }
        _ => {
            // Fallback for other types - treat as string or null
            Ok(JsonValue::String("Unsupported JSON type".to_string()))
        }
    }
}

pub(crate) fn format_decimal_to_string(
    mantissa: i128,
    _precision: u8,
    scale: i8,
) -> String {
    let scale_factor = 10_i128.pow(scale.abs() as u32);
    let integer_part = mantissa / scale_factor;
    let fractional_part = (mantissa % scale_factor).abs();

    if scale > 0 {
        format!(
            "{}.{:0>width$}",
            integer_part,
            fractional_part,
            width = scale as usize
        )
    } else {
        integer_part.to_string()
    }
}

/// Helper to extract map keys as strings
pub(crate) fn extract_key_as_string(
    column: &dyn Array,
    row_idx: usize,
) -> IcebergResult<String> {
    if column.is_null(row_idx) {
        return Ok("null".to_string());
    }
    match column.data_type() {
        DataType::Utf8 => {
            let array = column.as_string::<i32>();
            Ok(array.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let array = column.as_string::<i64>();
            Ok(array.value(row_idx).to_string())
        }
        _ => {
            // For non-string keys, rely on JSON conversion and stringify
            let json = extract_primitive_as_json(column, row_idx)?;
            match json {
                JsonValue::String(s) => Ok(s),
                _ => Ok(json.to_string()),
            }
        }
    }
}
