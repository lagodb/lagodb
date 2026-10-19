//! Compile JSONB leaf semantics from the Arrow field contract, once per column.

use arrow_schema::extension::{ExtensionType, Uuid};
use arrow_schema::{DataType, Field};

use super::NestedJsonbPlan;
use crate::{
    ArrowConversionError, ArrowConversionResult, PgColumnType, resolve_cell_rule,
};

impl NestedJsonbPlan {
    /// Compile shared recursive semantics from an Arrow field. UUID leaves
    /// carry `arrow.uuid`; unannotated fixed binary leaves remain binary.
    /// Providers preserve these fields through Arrow C Schema and projections.
    pub fn from_field(field: &Field) -> ArrowConversionResult<Self> {
        if let Some(extension) = field.extension_type_name() {
            if extension != Uuid::NAME {
                return Err(ArrowConversionError::UnsupportedColumnType(format!(
                    "JSONB conversion for extension {extension}"
                )));
            }
            field.try_extension_type::<Uuid>()?;
            return Self::scalar(
                field,
                resolve_cell_rule(field.data_type(), PgColumnType::Uuid)?,
            );
        }
        let target = match field.data_type() {
            DataType::Struct(fields) => {
                // No duplicate-name pass is needed here. Iceberg's Schema builder
                // runs for both schema construction and metadata deserialization
                // and recursively rejects duplicate field paths before a foreign
                // Struct can be bound to JSONB. Managed Structs bind to PostgreSQL
                // composites or intervals, while Parquet admission rejects nested
                // container-to-JSONB mappings. Current storage-provider paths that
                // reach this branch have therefore already established unique
                // object keys; a new structural source must establish the same
                // invariant at its binding boundary.
                let children = fields
                    .iter()
                    .map(|field| {
                        Ok((field.name().as_str(), Self::from_field(field)?))
                    })
                    .collect::<ArrowConversionResult<Vec<_>>>()?;
                return Self::structure(field, children);
            }
            DataType::List(element) => {
                return Ok(Self::list(field, Self::from_field(element)?));
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(ArrowConversionError::ArrowTypeMismatch(
                        "map entries must be a key/value struct".into(),
                    ));
                };
                let [key, value] = fields.as_ref() else {
                    return Err(ArrowConversionError::ArrowTypeMismatch(
                        "map entries must have two fields".into(),
                    ));
                };
                return Ok(Self::map(
                    field,
                    Self::from_field(key)?,
                    Self::from_field(value)?,
                ));
            }
            DataType::Boolean => PgColumnType::Bool,
            DataType::Int32 => PgColumnType::Int4,
            DataType::Int64 => PgColumnType::Int8,
            DataType::Float32 => PgColumnType::Float4,
            DataType::Float64 => PgColumnType::Float8,
            DataType::Utf8 | DataType::LargeUtf8 => PgColumnType::Text,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_) => PgColumnType::Bytea,
            DataType::Decimal128(_, _) => PgColumnType::Numeric,
            DataType::Date32 => PgColumnType::Date,
            DataType::Time64(_) => PgColumnType::Time,
            DataType::Timestamp(_, zone) => {
                if zone.is_some() {
                    PgColumnType::Timestamptz
                } else {
                    PgColumnType::Timestamp
                }
            }
            other => {
                return Err(ArrowConversionError::UnsupportedColumnType(format!(
                    "{other:?} is not a semantic JSONB value"
                )));
            }
        };
        Self::scalar(field, resolve_cell_rule(field.data_type(), target)?)
    }
}
