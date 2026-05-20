use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use iceberg_lite::spec::{ListType, PrimitiveType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_lakebase_core::tuple::{Cell, Row};

use super::traits::{ArrowToCell, RowsToArrow};
use crate::access::conversion::schema::iceberg_type_to_arrow_type;
use crate::error::{IcebergError, IcebergResult};

/// Helper to build a list array by appending values to a builder.
fn build_list_array<B, F>(
    rows: &[Row],
    col_idx: usize,
    element_field: Field,
    mut append_fn: F,
) -> IcebergResult<ArrayRef>
where
    B: arrow_array::builder::ArrayBuilder + Default,
    F: FnMut(&mut B, Option<&Cell>),
{
    let mut builder =
        arrow_array::builder::ListBuilder::with_capacity(B::default(), rows.len())
            .with_field(Arc::new(element_field));

    for row in rows {
        let cell = row.get(col_idx).and_then(|c| c.as_ref());
        append_fn(builder.values(), cell);
        builder.append(cell.is_some());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

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

/// Implementation of ArrowToCell for ListType.
impl ArrowToCell for ListType {
    fn extract(
        &self,
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
                    .ok_or(IcebergError::ArrowTypeMismatch(
                        "BooleanArray".to_string(),
                    ))?;
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
            _ => Err(IcebergError::UnsupportedColumnType(format!(
                "Unsupported Arrow type for List element at row {}: {:?}",
                row_idx,
                values.data_type()
            ))),
        }
    }
}

/// Build an Arrow list array.
impl RowsToArrow for ListType {
    fn build(&self, rows: &[Row], col_idx: usize) -> IcebergResult<ArrayRef> {
        let element_iceberg_type = &self.element_field.field_type;
        let element_arrow_type = iceberg_type_to_arrow_type(element_iceberg_type)?;

        let mut element_field =
            Field::new("element", element_arrow_type, !self.element_field.required);
        element_field.set_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            self.element_field.id.to_string(),
        )]));

        match element_iceberg_type.as_ref() {
            Type::Primitive(p) => match p {
                PrimitiveType::Boolean => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::BooleanBuilder, cell| {
                        if let Some(Cell::BoolArray(arr)) = cell {
                            for v in arr {
                                builder.append_option(*v);
                            }
                        }
                    },
                ),
                PrimitiveType::Int => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::Int32Builder, cell| {
                        match cell {
                            Some(Cell::I32Array(arr)) => {
                                for v in arr {
                                    builder.append_option(*v);
                                }
                            }
                            Some(Cell::I16Array(arr)) => {
                                for v in arr {
                                    builder.append_option(v.map(|x| x as i32));
                                }
                            }
                            _ => {}
                        }
                    },
                ),
                PrimitiveType::Long => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::Int64Builder, cell| {
                        if let Some(Cell::I64Array(arr)) = cell {
                            for v in arr {
                                builder.append_option(*v);
                            }
                        }
                    },
                ),
                PrimitiveType::Float => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::Float32Builder, cell| {
                        if let Some(Cell::F32Array(arr)) = cell {
                            for v in arr {
                                builder.append_option(*v);
                            }
                        }
                    },
                ),
                PrimitiveType::Double => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::Float64Builder, cell| {
                        if let Some(Cell::F64Array(arr)) = cell {
                            for v in arr {
                                builder.append_option(*v);
                            }
                        }
                    },
                ),
                PrimitiveType::String => build_list_array(
                    rows,
                    col_idx,
                    element_field,
                    |builder: &mut arrow_array::builder::StringBuilder, cell| {
                        if let Some(Cell::StringArray(arr)) = cell {
                            for val in arr {
                                match val {
                                    Some(s) => builder.append_value(s),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    },
                ),
                _ => Err(IcebergError::UnsupportedColumnType(format!(
                    "Unsupported element type in List for column {}: {:?}",
                    col_idx, p
                ))),
            },
            _ => Err(IcebergError::UnsupportedColumnType(format!(
                "Nested lists are not supported for column {}: {:?}",
                col_idx, element_iceberg_type
            ))),
        }
    }
}
