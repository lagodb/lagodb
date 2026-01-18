use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use iceberg_lite::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::error::IcebergResult;

/// Convert an Iceberg schema to an Arrow schema.
///
/// This function creates an Arrow schema from an Iceberg schema, including
/// field IDs as metadata for Parquet writer compatibility.
pub fn iceberg_schema_to_arrow_schema(
    schema: &IcebergSchema,
) -> IcebergResult<Schema> {
    let fields: Vec<Field> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            let arrow_type = iceberg_type_to_arrow_type(&field.field_type)?;
            let mut arrow_field =
                Field::new(&field.name, arrow_type, !field.required);

            // Add field ID as metadata for Parquet compatibility
            arrow_field.set_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                field.id.to_string(),
            )]));

            Ok(arrow_field)
        })
        .collect::<IcebergResult<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

/// Convert an Iceberg Type to an Arrow DataType.
pub(crate) fn iceberg_type_to_arrow_type(
    iceberg_type: &Type,
) -> IcebergResult<DataType> {
    match iceberg_type {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => Ok(DataType::Boolean),
            PrimitiveType::Int => Ok(DataType::Int32),
            PrimitiveType::Long => Ok(DataType::Int64),
            PrimitiveType::Float => Ok(DataType::Float32),
            PrimitiveType::Double => Ok(DataType::Float64),
            PrimitiveType::Decimal { precision, scale } => {
                Ok(DataType::Decimal128(*precision as u8, *scale as i8))
            }
            PrimitiveType::Date => Ok(DataType::Date32),
            PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => Ok(
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            ),
            PrimitiveType::String => Ok(DataType::Utf8),
            PrimitiveType::Binary => Ok(DataType::Binary),
            PrimitiveType::Fixed(len) => Ok(DataType::FixedSizeBinary(*len as i32)),
            PrimitiveType::Uuid => Ok(DataType::FixedSizeBinary(16)),
        },
        Type::List(list) => {
            let element_type =
                iceberg_type_to_arrow_type(&list.element_field.field_type)?;
            let mut element_field =
                Field::new("element", element_type, !list.element_field.required);
            element_field.set_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                list.element_field.id.to_string(),
            )]));
            Ok(DataType::List(Arc::new(element_field)))
        }
        Type::Struct(struct_type) => {
            let fields: Vec<Field> = struct_type
                .fields()
                .iter()
                .map(|f| {
                    let dt = iceberg_type_to_arrow_type(&f.field_type)?;
                    let mut field = Field::new(&f.name, dt, !f.required);
                    field.set_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        f.id.to_string(),
                    )]));
                    Ok(field)
                })
                .collect::<IcebergResult<Vec<_>>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Type::Map(map) => {
            let key_type = iceberg_type_to_arrow_type(&map.key_field.field_type)?;
            let value_type = iceberg_type_to_arrow_type(&map.value_field.field_type)?;

            let mut key_field = Field::new("key", key_type, false);
            key_field.set_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                map.key_field.id.to_string(),
            )]));

            let mut value_field =
                Field::new("value", value_type, !map.value_field.required);
            value_field.set_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                map.value_field.id.to_string(),
            )]));

            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key_field, value_field].into()),
                    false,
                )),
                false,
            ))
        }
    }
}
