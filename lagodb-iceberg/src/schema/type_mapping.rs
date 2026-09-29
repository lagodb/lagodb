//! Iceberg type conversion and PostgreSQL column rules, independent of slot positions.
//!
//! Arrow types come from iceberg-lite's converter, shared with the Parquet writer.
//! Live-column rules use the actual PostgreSQL OID; schema creation uses canonical
//! OIDs and typmods. JSONB's private Binary representation is selected explicitly.

use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg_lite::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use iceberg_lite::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use lagodb_arrow::{
    ArrowConversionError, ColumnRule, PgColumnType, resolve_column_rule,
};
use lagodb_core::tuple::numeric_typmod;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

/// Canonical PostgreSQL type for schema creation, distinct from live-column binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CanonicalPgType {
    oid: pg_sys::Oid,
    typmod: i32,
    column_type: PgColumnType,
}

impl CanonicalPgType {
    const fn plain(oid: pg_sys::Oid, column_type: PgColumnType) -> Self {
        Self {
            oid,
            typmod: -1,
            column_type,
        }
    }

    pub(crate) const fn oid(self) -> pg_sys::Oid {
        self.oid
    }

    pub(crate) const fn typmod(self) -> i32 {
        self.typmod
    }

    const fn column_type(self) -> PgColumnType {
        self.column_type
    }
}

/// Type descriptors and rules used by schema creation and column binding.
pub(crate) trait IcebergTypeExt {
    /// Reject types that PostgreSQL row reads and writes cannot materialize.
    fn validate_supported(&self) -> IcebergResult<()>;

    /// Convert to Arrow without applying PostgreSQL row-materialization limits.
    fn to_arrow_type(&self) -> IcebergResult<DataType>;

    /// Canonical conversion bucket, including the element OID for arrays.
    fn canonical_column_type(&self) -> Option<PgColumnType>;

    /// Canonical PostgreSQL OID and typmod for schema creation.
    fn canonical_pg_type(&self) -> Option<CanonicalPgType>;

    /// Resolve a format-only rule, used for write columns with no live source.
    fn resolve_rule(&self, pg: PgColumnType) -> IcebergResult<ColumnRule>;

    /// Resolve a live column's rule from its actual OID, including the JSONB codec.
    fn resolve_rule_for_oid(
        &self,
        target_oid: pg_sys::Oid,
    ) -> IcebergResult<ColumnRule>;
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
            Type::List(list) => match list.element_field.field_type.as_ref() {
                Type::Primitive(
                    PrimitiveType::Boolean
                    | PrimitiveType::Int
                    | PrimitiveType::Long
                    | PrimitiveType::Float
                    | PrimitiveType::Double
                    | PrimitiveType::String,
                ) => Ok(()),
                other => Err(IcebergError::UnsupportedColumnType(format!(
                    "list element type {other:?} is not supported"
                ))),
            },
            Type::Struct(_) => Err(IcebergError::UnsupportedColumnType(
                "Struct type is not supported".to_string(),
            )),
            Type::Map(_) => Err(IcebergError::UnsupportedColumnType(
                "Map type is not supported".to_string(),
            )),
            Type::Variant(_) => Err(IcebergError::UnsupportedColumnType(
                "Variant type is not supported".to_string(),
            )),
        }
    }

    fn to_arrow_type(&self) -> IcebergResult<DataType> {
        Ok(type_to_arrow_type(self)?)
    }

    fn canonical_column_type(&self) -> Option<PgColumnType> {
        self.canonical_pg_type().map(CanonicalPgType::column_type)
    }

    fn canonical_pg_type(&self) -> Option<CanonicalPgType> {
        let postgres = match self {
            Type::Primitive(p) => match p {
                PrimitiveType::Boolean => {
                    CanonicalPgType::plain(pg_sys::BOOLOID, PgColumnType::Bool)
                }
                PrimitiveType::Int => {
                    CanonicalPgType::plain(pg_sys::INT4OID, PgColumnType::Int4)
                }
                PrimitiveType::Long => {
                    CanonicalPgType::plain(pg_sys::INT8OID, PgColumnType::Int8)
                }
                PrimitiveType::Float => {
                    CanonicalPgType::plain(pg_sys::FLOAT4OID, PgColumnType::Float4)
                }
                PrimitiveType::Double => {
                    CanonicalPgType::plain(pg_sys::FLOAT8OID, PgColumnType::Float8)
                }
                PrimitiveType::Decimal { precision, scale } => {
                    return Some(CanonicalPgType {
                        oid: pg_sys::NUMERICOID,
                        typmod: numeric_typmod(*precision, *scale as i32),
                        column_type: PgColumnType::Numeric,
                    });
                }
                PrimitiveType::Date => {
                    CanonicalPgType::plain(pg_sys::DATEOID, PgColumnType::Date)
                }
                PrimitiveType::Time => {
                    CanonicalPgType::plain(pg_sys::TIMEOID, PgColumnType::Time)
                }
                PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                    CanonicalPgType::plain(
                        pg_sys::TIMESTAMPOID,
                        PgColumnType::Timestamp,
                    )
                }
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                    CanonicalPgType::plain(
                        pg_sys::TIMESTAMPTZOID,
                        PgColumnType::Timestamptz,
                    )
                }
                PrimitiveType::String => {
                    CanonicalPgType::plain(pg_sys::TEXTOID, PgColumnType::Text)
                }
                PrimitiveType::Uuid => {
                    CanonicalPgType::plain(pg_sys::UUIDOID, PgColumnType::Uuid)
                }
                // Binary's canonical PG type is bytea; live JSONB binds its own codec.
                PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
                    CanonicalPgType::plain(pg_sys::BYTEAOID, PgColumnType::Bytea)
                }
                PrimitiveType::Unknown => return None,
            },
            Type::List(list) => match list.element_field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Boolean) => CanonicalPgType::plain(
                    pg_sys::BOOLARRAYOID,
                    PgColumnType::Array(pg_sys::BOOLOID),
                ),
                Type::Primitive(PrimitiveType::Int) => CanonicalPgType::plain(
                    pg_sys::INT4ARRAYOID,
                    PgColumnType::Array(pg_sys::INT4OID),
                ),
                Type::Primitive(PrimitiveType::Long) => CanonicalPgType::plain(
                    pg_sys::INT8ARRAYOID,
                    PgColumnType::Array(pg_sys::INT8OID),
                ),
                Type::Primitive(PrimitiveType::Float) => CanonicalPgType::plain(
                    pg_sys::FLOAT4ARRAYOID,
                    PgColumnType::Array(pg_sys::FLOAT4OID),
                ),
                Type::Primitive(PrimitiveType::Double) => CanonicalPgType::plain(
                    pg_sys::FLOAT8ARRAYOID,
                    PgColumnType::Array(pg_sys::FLOAT8OID),
                ),
                Type::Primitive(PrimitiveType::String) => CanonicalPgType::plain(
                    pg_sys::TEXTARRAYOID,
                    PgColumnType::Array(pg_sys::TEXTOID),
                ),
                _ => return None,
            },
            Type::Struct(_) | Type::Map(_) | Type::Variant(_) => return None,
        };
        Some(postgres)
    }

    fn resolve_rule(&self, pg: PgColumnType) -> IcebergResult<ColumnRule> {
        self.validate_supported()?;
        let arrow_dt = self.to_arrow_type()?;
        resolve_column_rule(&arrow_dt, pg).map_err(IcebergError::from)
    }

    fn resolve_rule_for_oid(
        &self,
        target_oid: pg_sys::Oid,
    ) -> IcebergResult<ColumnRule> {
        let pg = PgColumnType::from_pg_type(target_oid).ok_or_else(|| {
            IcebergError::UnsupportedColumnType(format!(
                "PostgreSQL OID {} has no Arrow conversion target",
                u32::from(target_oid)
            ))
        })?;
        if target_oid != pg_sys::JSONBOID {
            return self.resolve_rule(pg);
        }

        self.validate_supported()?;
        let arrow_dt = self.to_arrow_type()?;
        match arrow_dt {
            DataType::Binary | DataType::LargeBinary => {
                Ok(ColumnRule::PostgresJsonbVarlena)
            }
            _ => Err(IcebergError::from(
                ArrowConversionError::IncompatibleColumnType(
                    format!("{arrow_dt:?}"),
                    "JSONB requires the provider's Binary JSONB codec".to_owned(),
                ),
            )),
        }
    }
}

/// Schema conversion shared with the Iceberg Parquet writer.
pub(crate) trait IcebergSchemaExt {
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema>;
}

impl IcebergSchemaExt for IcebergSchema {
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        Ok(schema_to_arrow_schema(self)?)
    }
}
