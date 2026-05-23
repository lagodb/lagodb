use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_schema::{DataType, Field};
use iceberg_lite::spec::{ListType, PrimitiveType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_lakebase_core::tuple::{Cell, Row};

use super::traits::{ArrowToCell, RowsToArrow};
use crate::access::conversion::schema::iceberg_type_to_arrow_type;
use crate::error::{IcebergError, IcebergResult};

#[derive(Clone, Copy)]
pub(crate) enum SupportedListElement {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
}

impl SupportedListElement {
    pub(crate) fn from_primitive(p: &PrimitiveType) -> Option<Self> {
        match p {
            PrimitiveType::Boolean => Some(Self::Boolean),
            PrimitiveType::Int => Some(Self::Int),
            PrimitiveType::Long => Some(Self::Long),
            PrimitiveType::Float => Some(Self::Float),
            PrimitiveType::Double => Some(Self::Double),
            PrimitiveType::String => Some(Self::String),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "string",
        }
    }
}

/// Helper to build a list array by appending values to a builder.
fn build_list_array<B, F>(
    rows: &[Row],
    col_idx: usize,
    element_field: Field,
    expected_element: &'static str,
    mut append_fn: F,
) -> IcebergResult<ArrayRef>
where
    B: arrow_array::builder::ArrayBuilder + Default,
    F: FnMut(&mut B, &Cell) -> bool,
{
    let mut builder =
        arrow_array::builder::ListBuilder::with_capacity(B::default(), rows.len())
            .with_field(Arc::new(element_field));

    for (row_idx, row) in rows.iter().enumerate() {
        let cell = row.get(col_idx).and_then(|c| c.as_ref());
        match cell {
            Some(cell) => {
                if !append_fn(builder.values(), cell) {
                    return Err(IcebergError::IncompatibleColumnType(
                        format!(
                            "List<{expected_element}> for row {row_idx} column {col_idx}"
                        ),
                        "row cell has incompatible array type".to_string(),
                    ));
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
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

fn extract_string_array_to_vec<O>(
    array: &GenericStringArray<O>,
) -> Vec<Option<String>>
where
    O: OffsetSizeTrait,
{
    array
        .iter()
        .map(|value| value.map(ToOwned::to_owned))
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
                let vec = extract_string_array_to_vec(arr);
                Ok(Some(Cell::StringArray(vec)))
            }
            DataType::LargeUtf8 => {
                let arr = values.as_string::<i64>();
                let vec = extract_string_array_to_vec(arr);
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
            Type::Primitive(p) => {
                let Some(element) = SupportedListElement::from_primitive(p) else {
                    return Err(IcebergError::UnsupportedColumnType(format!(
                        "Unsupported element type in List for column {}: {:?}",
                        col_idx, p
                    )));
                };
                let expected_element = element.name();

                match element {
                    SupportedListElement::Boolean => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::BooleanBuilder, cell| {
                            let Cell::BoolArray(arr) = cell else {
                                return false;
                            };
                            for v in arr {
                                builder.append_option(*v);
                            }
                            true
                        },
                    ),
                    SupportedListElement::Int => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::Int32Builder, cell| {
                            match cell {
                                Cell::I32Array(arr) => {
                                    for v in arr {
                                        builder.append_option(*v);
                                    }
                                    true
                                }
                                Cell::I16Array(arr) => {
                                    for v in arr {
                                        builder.append_option(v.map(|x| x as i32));
                                    }
                                    true
                                }
                                _ => false,
                            }
                        },
                    ),
                    SupportedListElement::Long => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::Int64Builder, cell| {
                            let Cell::I64Array(arr) = cell else {
                                return false;
                            };
                            for v in arr {
                                builder.append_option(*v);
                            }
                            true
                        },
                    ),
                    SupportedListElement::Float => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::Float32Builder, cell| {
                            let Cell::F32Array(arr) = cell else {
                                return false;
                            };
                            for v in arr {
                                builder.append_option(*v);
                            }
                            true
                        },
                    ),
                    SupportedListElement::Double => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::Float64Builder, cell| {
                            let Cell::F64Array(arr) = cell else {
                                return false;
                            };
                            for v in arr {
                                builder.append_option(*v);
                            }
                            true
                        },
                    ),
                    SupportedListElement::String => build_list_array(
                        rows,
                        col_idx,
                        element_field,
                        expected_element,
                        |builder: &mut arrow_array::builder::StringBuilder, cell| {
                            let Cell::StringArray(arr) = cell else {
                                return false;
                            };
                            for val in arr {
                                match val {
                                    Some(s) => builder.append_value(s),
                                    None => builder.append_null(),
                                }
                            }
                            true
                        },
                    ),
                }
            }
            _ => Err(IcebergError::UnsupportedColumnType(format!(
                "Nested lists are not supported for column {}: {:?}",
                col_idx, element_iceberg_type
            ))),
        }
    }
}
