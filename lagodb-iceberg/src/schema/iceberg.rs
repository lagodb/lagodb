//! Checked Iceberg schema field lookup and Arrow type/field/schema conversions.

use arrow_schema::extension::Uuid;
use arrow_schema::{Field, Schema as ArrowSchema};
use iceberg_lite::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use iceberg_lite::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

use crate::error::{IcebergError, IcebergResult};

/// Validate that an Iceberg type is supported by LagoDB's Arrow codecs.
///
/// Upstream Arrow converters map `Unknown` to `Null`, oversized `Fixed` to
/// `LargeBinary`, and `Variant` to its storage `Struct`, so capability
/// validation must precede physical type conversion.
pub(crate) trait IcebergTypeExt {
    fn validate_supported(&self) -> IcebergResult<()>;
}

impl IcebergTypeExt for Type {
    fn validate_supported(&self) -> IcebergResult<()> {
        match self {
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
            Type::List(list) => list.element_field.field_type.validate_supported(),
            Type::Struct(structure) => structure
                .fields()
                .iter()
                .try_for_each(|field| field.field_type.validate_supported()),
            Type::Map(map) => {
                map.key_field.field_type.validate_supported()?;
                map.value_field.field_type.validate_supported()
            }
            Type::Variant(_) => Err(IcebergError::UnsupportedColumnType(
                "Variant type is not supported".to_owned(),
            )),
        }
    }
}

/// Build a single projected Arrow field for read codecs and scan Arrow C Schemas.
///
/// Unlike [`IcebergSchemaExt::to_arrow_schema`], the top-level field omits
/// Iceberg `PARQUET:field_id` and `doc` metadata. Upstream `type_to_arrow_type`
/// already annotates nested `Struct`/`List`/`Map` UUID children; only a
/// top-level UUID needs its extension attached here.
pub(super) trait IcebergFieldExt {
    fn to_projection_field(&self) -> IcebergResult<Field>;
}

impl IcebergFieldExt for NestedField {
    fn to_projection_field(&self) -> IcebergResult<Field> {
        self.field_type.validate_supported()?;
        let mut field = Field::new(
            &self.name,
            type_to_arrow_type(&self.field_type)?,
            !self.required,
        );
        if matches!(
            self.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Uuid)
        ) {
            field.try_with_extension_type(Uuid)?;
        }
        Ok(field)
    }
}

/// Shared top-level field lookup and checked conversion to the canonical
/// Iceberg Arrow schema (preserving `PARQUET:field_id`, `doc`, and recursive
/// `arrow.uuid` annotations).
pub(super) trait IcebergSchemaExt {
    fn require_field_by_name(&self, name: &str) -> IcebergResult<&NestedField>;

    fn require_field_by_id(&self, field_id: i32) -> IcebergResult<&NestedField>;

    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema>;
}

impl IcebergSchemaExt for IcebergSchema {
    fn require_field_by_name(&self, name: &str) -> IcebergResult<&NestedField> {
        self.as_struct()
            .field_by_name(name)
            .map(AsRef::as_ref)
            .ok_or_else(|| IcebergError::ColumnNotFound(name.to_owned()))
    }

    fn require_field_by_id(&self, field_id: i32) -> IcebergResult<&NestedField> {
        self.as_struct()
            .field_by_id(field_id)
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                IcebergError::ColumnNotFound(format!("field id {field_id}"))
            })
    }

    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        self.as_struct()
            .fields()
            .iter()
            .try_for_each(|field| field.field_type.validate_supported())?;
        Ok(schema_to_arrow_schema(self)?)
    }
}
