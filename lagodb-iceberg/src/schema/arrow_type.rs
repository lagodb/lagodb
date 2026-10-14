//! Preserve source semantics in Arrow schemas without changing Arrow buffers.

use std::sync::Arc;

use arrow_schema::extension::Uuid;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg_lite::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use iceberg_lite::spec::{PrimitiveType, Schema as IcebergSchema, Type};

use crate::error::{IcebergError, IcebergResult};

/// Enrich the upstream physical schema for read/write conversion plans and
/// the query scan's Arrow C Schema. UUID is the only supported Iceberg leaf whose
/// semantics cannot be recovered from its physical Arrow type.
pub(crate) struct SemanticArrowType<'a>(&'a Type);

impl<'a> SemanticArrowType<'a> {
    pub(crate) fn validate_supported(&self) -> IcebergResult<()> {
        match self.0 {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean
                | PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::Float
                | PrimitiveType::Double
                | PrimitiveType::Decimal { .. }
                | PrimitiveType::Date
                | PrimitiveType::Time
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
                | PrimitiveType::String
                | PrimitiveType::Uuid
                | PrimitiveType::Binary => Ok(()),
                PrimitiveType::Fixed(len) if i32::try_from(*len).is_err() => {
                    Err(IcebergError::UnsupportedColumnType(format!(
                        "fixed[{len}] exceeds Arrow FixedSizeBinary i32 width limit"
                    )))
                }
                PrimitiveType::Fixed(_) => Ok(()),
                PrimitiveType::Unknown => {
                    Err(IcebergError::UnsupportedColumnType(primitive.to_string()))
                }
            },
            Type::List(list) => {
                Self::new(&list.element_field.field_type).validate_supported()
            }
            Type::Struct(structure) => {
                structure.fields().iter().try_for_each(|field| {
                    Self::new(&field.field_type).validate_supported()
                })
            }
            Type::Map(map) => {
                Self::new(&map.key_field.field_type).validate_supported()?;
                Self::new(&map.value_field.field_type).validate_supported()
            }
            Type::Variant(_) => Err(IcebergError::UnsupportedColumnType(
                "Variant type is not supported".to_string(),
            )),
        }
    }

    pub(crate) const fn new(source: &'a Type) -> Self {
        Self(source)
    }

    pub(crate) fn field(&self, name: &str, nullable: bool) -> IcebergResult<Field> {
        let field = Field::new(name, type_to_arrow_type(self.0)?, nullable);
        self.annotate(field)
    }

    pub(crate) fn annotate(&self, mut field: Field) -> IcebergResult<Field> {
        let data_type = match (self.0, field.data_type()) {
            (Type::Primitive(PrimitiveType::Uuid), _) => {
                field.try_with_extension_type(Uuid)?;
                return Ok(field);
            }
            (Type::Struct(source), DataType::Struct(fields)) => {
                let fields = source
                    .fields()
                    .iter()
                    .zip(fields)
                    .map(|(source, field)| {
                        Self::new(&source.field_type).annotate(field.as_ref().clone())
                    })
                    .collect::<IcebergResult<Vec<_>>>()?;
                DataType::Struct(fields.into())
            }
            (Type::List(source), DataType::List(element)) => {
                let element = Self::new(&source.element_field.field_type)
                    .annotate(element.as_ref().clone())?;
                DataType::List(Arc::new(element))
            }
            (Type::Map(source), DataType::Map(entries, sorted)) => {
                // The upstream converter above constructs exactly these two
                // fields in key/value order; this is not external schema input.
                let DataType::Struct(fields) = entries.data_type() else {
                    unreachable!("Iceberg map converter produces struct entries")
                };
                let key = Self::new(&source.key_field.field_type)
                    .annotate(fields[0].as_ref().clone())?;
                let value = Self::new(&source.value_field.field_type)
                    .annotate(fields[1].as_ref().clone())?;
                let entries = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(vec![key, value].into()));
                DataType::Map(Arc::new(entries), *sorted)
            }
            _ => return Ok(field),
        };
        Ok(field.with_data_type(data_type))
    }
}

/// Schema conversion shared with the Iceberg Parquet writer.
pub(crate) trait IcebergSchemaExt {
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema>;
}

impl IcebergSchemaExt for IcebergSchema {
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        let physical = schema_to_arrow_schema(self)?;
        let fields = self
            .as_struct()
            .fields()
            .iter()
            .zip(physical.fields())
            .map(|(source, field)| {
                SemanticArrowType::new(&source.field_type)
                    .annotate(field.as_ref().clone())
            })
            .collect::<IcebergResult<Vec<_>>>()?;
        Ok(ArrowSchema::new_with_metadata(
            fields,
            physical.metadata().clone(),
        ))
    }
}
