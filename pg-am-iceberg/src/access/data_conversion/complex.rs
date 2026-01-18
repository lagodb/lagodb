use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, ListArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use iceberg_lite::spec::{ListType, MapType, StructType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_lakehouse_core::data::{Cell, Row};
use serde_json::Value as JsonValue;

use crate::access::data_conversion::primitive::build_primitive_array_from_json;
use crate::access::data_conversion::schema::iceberg_type_to_arrow_type;
use crate::error::IcebergResult;

/// Macro to build a list array for primitive numeric types.
macro_rules! build_primitive_list_array {
    ($rows:expr, $col_idx:expr, $element_field:expr, $cell_variant:ident, $value_builder:ty, $value_type:ty, $default:expr) => {{
        let mut builder = arrow_array::builder::ListBuilder::with_capacity(
            <$value_builder>::new(),
            $rows.len(),
        )
        .with_field(Arc::new($element_field.clone()));

        for row in $rows {
            if let Some(Some(Cell::$cell_variant(arr))) = row.get($col_idx) {
                let values: Vec<Option<$value_type>> = arr.clone();
                let _ = builder.values().append_values(
                    &values
                        .iter()
                        .map(|v| v.unwrap_or($default))
                        .collect::<Vec<_>>(),
                    &values.iter().map(|v| v.is_some()).collect::<Vec<_>>(),
                );
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Build an Arrow list array.
pub(crate) fn build_list_array(
    rows: &[Row],
    col_idx: usize,
    list: &ListType,
) -> IcebergResult<ArrayRef> {
    let element_iceberg_type = &list.element_field.field_type;
    let element_arrow_type = iceberg_type_to_arrow_type(element_iceberg_type)?;

    let mut element_field =
        Field::new("element", element_arrow_type, !list.element_field.required);
    element_field.set_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        list.element_field.id.to_string(),
    )]));

    // Check first non-null cell to determine element type for fast path
    let first_cell = rows
        .iter()
        .find_map(|row| row.get(col_idx).and_then(|cell| cell.as_ref()));

    if let Some(cell) = first_cell {
        match cell {
            Cell::I32Array(_) | Cell::I16Array(_) => {
                let mut builder = arrow_array::builder::ListBuilder::with_capacity(
                    arrow_array::builder::Int32Builder::new(),
                    rows.len(),
                )
                .with_field(Arc::new(element_field.clone()));

                for row in rows {
                    if let Some(Some(cell)) = row.get(col_idx) {
                        let values: Option<Vec<Option<i32>>> = match cell {
                            Cell::I32Array(arr) => Some(arr.clone()),
                            Cell::I16Array(arr) => Some(
                                arr.iter().map(|v| v.map(|x| x as i32)).collect(),
                            ),
                            _ => None,
                        };
                        if let Some(values) = values {
                            builder.values().append_values(
                                &values
                                    .iter()
                                    .map(|v| v.unwrap_or(0))
                                    .collect::<Vec<_>>(),
                                &values
                                    .iter()
                                    .map(|v| v.is_some())
                                    .collect::<Vec<_>>(),
                            );
                            builder.append(true);
                        } else {
                            builder.append(false);
                        }
                    } else {
                        builder.append(false);
                    }
                }
                return Ok(Arc::new(builder.finish()));
            }
            Cell::I64Array(_) => {
                return build_primitive_list_array!(
                    rows,
                    col_idx,
                    element_field,
                    I64Array,
                    arrow_array::builder::Int64Builder,
                    i64,
                    0
                );
            }
            Cell::F32Array(_) => {
                return build_primitive_list_array!(
                    rows,
                    col_idx,
                    element_field,
                    F32Array,
                    arrow_array::builder::Float32Builder,
                    f32,
                    0.0
                );
            }
            Cell::F64Array(_) => {
                return build_primitive_list_array!(
                    rows,
                    col_idx,
                    element_field,
                    F64Array,
                    arrow_array::builder::Float64Builder,
                    f64,
                    0.0
                );
            }
            Cell::BoolArray(_) => {
                return build_primitive_list_array!(
                    rows,
                    col_idx,
                    element_field,
                    BoolArray,
                    arrow_array::builder::BooleanBuilder,
                    bool,
                    false
                );
            }
            Cell::StringArray(_) => {
                let mut builder = arrow_array::builder::ListBuilder::with_capacity(
                    arrow_array::builder::StringBuilder::new(),
                    rows.len(),
                )
                .with_field(Arc::new(element_field.clone()));

                for row in rows {
                    if let Some(Some(Cell::StringArray(arr))) = row.get(col_idx) {
                        for val in arr {
                            match val {
                                Some(s) => builder.values().append_value(s),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }
                return Ok(Arc::new(builder.finish()));
            }
            _ => {}
        }
    }

    let mut flat_values = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    let mut current_offset = 0;
    let mut validity = Vec::with_capacity(rows.len());

    for row in rows {
        let cell = row.get(col_idx).and_then(|c| c.as_ref());

        match cell {
            Some(cell) => {
                let json_val = cell.to_json_value();
                if let JsonValue::Array(arr) = json_val {
                    validity.push(true);
                    for item in arr {
                        flat_values.push(Some(item));
                        current_offset += 1;
                    }
                } else {
                    validity.push(false);
                }
            }
            _ => {
                validity.push(false);
            }
        }
        offsets.push(current_offset);
    }

    let flat_refs: Vec<Option<&JsonValue>> =
        flat_values.iter().map(|v| v.as_ref()).collect();

    let element_array =
        build_arrow_array_from_json(&flat_refs, element_iceberg_type)?;

    Ok(Arc::new(ListArray::new(
        Arc::new(element_field),
        OffsetBuffer::new(offsets.into()),
        element_array,
        Some(NullBuffer::from(validity)),
    )))
}

/// Build an Arrow struct array.
pub(crate) fn build_struct_array(
    rows: &[Row],
    col_idx: usize,
    struct_type: &StructType,
) -> IcebergResult<ArrayRef> {
    let mut arrow_fields = Vec::with_capacity(struct_type.fields().len());
    let mut arrow_arrays = Vec::with_capacity(struct_type.fields().len());

    for field in struct_type.fields() {
        let field_name = &field.name;
        let field_type = &field.field_type;

        let arrow_type = iceberg_type_to_arrow_type(field_type)?;
        let mut arrow_field = Field::new(field_name, arrow_type, !field.required);
        arrow_field.set_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field.id.to_string(),
        )]));
        arrow_fields.push(arrow_field);

        let values: Vec<Option<&JsonValue>> = rows
            .iter()
            .map(|row| {
                row.get(col_idx)
                    .and_then(|c| c.as_ref())
                    .and_then(|c| match c {
                        Cell::Json(jsonb) | Cell::Composite(jsonb) => {
                            match &jsonb.0 {
                                JsonValue::Object(map) => map.get(field_name),
                                _ => None,
                            }
                        }
                        _ => None,
                    })
            })
            .collect();

        let array = build_arrow_array_from_json(&values, field_type)?;
        arrow_arrays.push(array);
    }

    let null_buffer = if rows
        .iter()
        .any(|row| row.get(col_idx).and_then(|c| c.as_ref()).is_none())
    {
        let booleans: Vec<bool> = rows
            .iter()
            .map(|row| row.get(col_idx).and_then(|c| c.as_ref()).is_some())
            .collect();
        Some(NullBuffer::from(booleans))
    } else {
        None
    };

    Ok(Arc::new(StructArray::new(
        arrow_fields.into(),
        arrow_arrays,
        null_buffer,
    )))
}

/// Build an Arrow map array.
pub(crate) fn build_map_array(
    rows: &[Row],
    col_idx: usize,
    map_type: &MapType,
) -> IcebergResult<ArrayRef> {
    let key_type = &map_type.key_field.field_type;
    let value_type = &map_type.value_field.field_type;

    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    let mut current_offset = 0;

    let mut validity = Vec::with_capacity(rows.len());

    for row in rows {
        let cell = row.get(col_idx).and_then(|c| c.as_ref());

        match cell {
            Some(Cell::Json(jsonb)) | Some(Cell::Composite(jsonb)) => {
                match &jsonb.0 {
                    JsonValue::Object(obj) => {
                        validity.push(true);
                        for (k, v) in obj {
                            keys.push(Some(JsonValue::String(k.clone())));
                            values.push(Some(v.clone()));
                            current_offset += 1;
                        }
                    }
                    _ => {
                        validity.push(false);
                    }
                }
            }
            _ => {
                validity.push(false);
            }
        }
        offsets.push(current_offset);
    }

    let key_refs: Vec<Option<&JsonValue>> = keys.iter().map(|k| k.as_ref()).collect();
    let value_refs: Vec<Option<&JsonValue>> =
        values.iter().map(|v| v.as_ref()).collect();

    let key_array = build_arrow_array_from_json(&key_refs, key_type)?;
    let value_array = build_arrow_array_from_json(&value_refs, value_type)?;

    let entries_field = Arc::new(Field::new(
        "entries",
        arrow_schema::DataType::Struct(
            vec![
                Field::new("key", key_array.data_type().clone(), false),
                Field::new(
                    "value",
                    value_array.data_type().clone(),
                    !map_type.value_field.required,
                ),
            ]
            .into(),
        ),
        false,
    ));

    Ok(Arc::new(arrow_array::MapArray::new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        StructArray::try_new(
            vec![
                Field::new("key", key_array.data_type().clone(), false),
                Field::new(
                    "value",
                    value_array.data_type().clone(),
                    !map_type.value_field.required,
                ),
            ]
            .into(),
            vec![key_array, value_array],
            None,
        )
        .map_err(|e| crate::error::IcebergError::ArrowError(e))?,
        Some(NullBuffer::from(validity)),
        false,
    )))
}

/// Helper to build Arrow arrays from JSON values.
pub(crate) fn build_arrow_array_from_json(
    values: &[Option<&JsonValue>],
    iceberg_type: &Type,
) -> IcebergResult<ArrayRef> {
    match iceberg_type {
        Type::Primitive(p) => build_primitive_array_from_json(values, p),
        Type::List(list) => {
            let element_iceberg_type = &list.element_field.field_type;
            let element_arrow_type =
                iceberg_type_to_arrow_type(element_iceberg_type)?;

            let mut element_field = Field::new(
                "element",
                element_arrow_type,
                !list.element_field.required,
            );
            element_field.set_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                list.element_field.id.to_string(),
            )]));

            let mut flat_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0);
            let mut current_offset = 0;
            let mut validity = Vec::with_capacity(values.len());

            for val in values {
                match val {
                    Some(JsonValue::Array(arr)) => {
                        validity.push(true);
                        for item in arr {
                            flat_values.push(Some(item));
                            current_offset += 1;
                        }
                    }
                    Some(JsonValue::Null) | None => {
                        validity.push(false);
                    }
                    _ => {
                        validity.push(false);
                    }
                }
                offsets.push(current_offset);
            }

            let element_array =
                build_arrow_array_from_json(&flat_values, element_iceberg_type)?;

            Ok(Arc::new(ListArray::new(
                Arc::new(element_field),
                OffsetBuffer::new(offsets.into()),
                element_array,
                Some(NullBuffer::from(validity)),
            )))
        }
        Type::Struct(s) => {
            let mut arrow_fields = Vec::with_capacity(s.fields().len());
            let mut arrow_arrays = Vec::with_capacity(s.fields().len());

            for field in s.fields() {
                let field_name = &field.name;
                let field_type = &field.field_type;

                let arrow_type = iceberg_type_to_arrow_type(field_type)?;
                let mut arrow_field =
                    Field::new(field_name, arrow_type, !field.required);
                arrow_field.set_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.id.to_string(),
                )]));
                arrow_fields.push(arrow_field);

                let field_values: Vec<Option<&JsonValue>> = values
                    .iter()
                    .map(|o| {
                        o.and_then(|j| match j {
                            JsonValue::Object(map) => map.get(field_name),
                            _ => None,
                        })
                    })
                    .collect();

                let array = build_arrow_array_from_json(&field_values, field_type)?;
                arrow_arrays.push(array);
            }

            let validity: Vec<bool> = values.iter().map(|o| o.is_some()).collect();

            Ok(Arc::new(StructArray::new(
                arrow_fields.into(),
                arrow_arrays,
                Some(NullBuffer::from(validity)),
            )))
        }
        Type::Map(m) => {
            let key_type = &m.key_field.field_type;
            let value_type = &m.value_field.field_type;

            let mut keys = Vec::new();
            let mut value_vals = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0);
            let mut current_offset = 0;
            let mut validity = Vec::with_capacity(values.len());

            for val in values {
                match val {
                    Some(JsonValue::Object(obj)) => {
                        validity.push(true);
                        for (k, v) in obj {
                            keys.push(Some(JsonValue::String(k.clone())));
                            value_vals.push(Some(v));
                            current_offset += 1;
                        }
                    }
                    _ => {
                        validity.push(false);
                    }
                }
                offsets.push(current_offset);
            }

            let key_refs: Vec<Option<&JsonValue>> =
                keys.iter().map(|k| k.as_ref()).collect();
            let key_array = build_arrow_array_from_json(&key_refs, key_type)?;
            let value_array = build_arrow_array_from_json(&value_vals, value_type)?;

            let entries_field = Arc::new(Field::new(
                "entries",
                arrow_schema::DataType::Struct(
                    vec![
                        Field::new("key", key_array.data_type().clone(), false),
                        Field::new(
                            "value",
                            value_array.data_type().clone(),
                            !m.value_field.required,
                        ),
                    ]
                    .into(),
                ),
                false,
            ));

            Ok(Arc::new(arrow_array::MapArray::new(
                entries_field,
                OffsetBuffer::new(offsets.into()),
                StructArray::try_new(
                    vec![
                        Field::new("key", key_array.data_type().clone(), false),
                        Field::new(
                            "value",
                            value_array.data_type().clone(),
                            !m.value_field.required,
                        ),
                    ]
                    .into(),
                    vec![key_array, value_array],
                    None,
                )
                .map_err(|e| crate::error::IcebergError::ArrowError(e))?,
                Some(NullBuffer::from(validity)),
                false,
            )))
        }
    }
}
