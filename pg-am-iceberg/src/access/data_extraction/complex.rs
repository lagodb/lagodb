use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_schema::DataType;
use pg_lakehouse_core::data::Cell;
use pgrx::JsonB;

use super::json::extract_complex_type_as_json;
use crate::error::{IcebergError, IcebergResult};

/// Helper to extract values from a primitive array into a Vec<Option<T>>
fn extract_primitive_array_to_vec<T, U>(
    array: &arrow_array::PrimitiveArray<T>,
) -> Vec<Option<U>>
where
    T: arrow_array::types::ArrowPrimitiveType,
    U: From<T::Native>,
{
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(U::from(array.value(i)))
            }
        })
        .collect()
}

/// Extract a list/array cell value from an Arrow column.
pub(crate) fn extract_list_cell(
    column: &dyn Array,
    row_idx: usize,
) -> IcebergResult<Option<Cell>> {
    let list_array = column
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .ok_or(IcebergError::ArrowTypeMismatch("ListArray".to_string()))?;

    let values = list_array.value(row_idx);

    match values.data_type() {
        DataType::Int32 => {
            let arr = values.as_primitive::<Int32Type>();
            let vec = extract_primitive_array_to_vec(arr);
            Ok(Some(Cell::I32Array(vec)))
        }
        DataType::Int64 => {
            let arr = values.as_primitive::<Int64Type>();
            let vec = extract_primitive_array_to_vec(arr);
            Ok(Some(Cell::I64Array(vec)))
        }
        DataType::Float32 => {
            let arr = values.as_primitive::<Float32Type>();
            let vec = extract_primitive_array_to_vec(arr);
            Ok(Some(Cell::F32Array(vec)))
        }
        DataType::Float64 => {
            let arr = values.as_primitive::<Float64Type>();
            let vec = extract_primitive_array_to_vec(arr);
            Ok(Some(Cell::F64Array(vec)))
        }
        DataType::Boolean => {
            let arr = values
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or(IcebergError::ArrowTypeMismatch("BooleanArray".to_string()))?;
            let vec: Vec<Option<bool>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(Some(Cell::BoolArray(vec)))
        }
        DataType::Utf8 => {
            let arr = values.as_string::<i32>();
            let vec: Vec<Option<String>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                })
                .collect();
            Ok(Some(Cell::StringArray(vec)))
        }
        DataType::Int16 => {
            let arr = values.as_primitive::<arrow_array::types::Int16Type>();
            let vec = extract_primitive_array_to_vec(arr);
            Ok(Some(Cell::I16Array(vec)))
        }
        _ => {
            let json_value = extract_complex_type_as_json(column, row_idx)?;
            Ok(Some(Cell::Json(JsonB(json_value))))
        }
    }
}
