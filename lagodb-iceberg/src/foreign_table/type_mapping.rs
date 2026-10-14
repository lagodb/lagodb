//! Iceberg-first PostgreSQL declarations and the explicit foreign DDL contract.

use iceberg_lite::spec::{PrimitiveType, Type};
use lagodb_core::tuple::{PgType, numeric_precision_scale, numeric_typmod};
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use crate::error::IcebergError;
use crate::schema::arrow_type::SemanticArrowType;

pub(super) struct ForeignType<'a>(&'a Type);

impl<'a> ForeignType<'a> {
    pub(super) fn new(source: &'a Type) -> Self {
        Self(source)
    }

    pub(super) fn canonical(&self) -> Option<PgType> {
        let postgres = match self.0 {
            Type::Primitive(p) => match p {
                PrimitiveType::Boolean => PgType::new(pg_sys::BOOLOID, -1),
                PrimitiveType::Int => PgType::new(pg_sys::INT4OID, -1),
                PrimitiveType::Long => PgType::new(pg_sys::INT8OID, -1),
                PrimitiveType::Float => PgType::new(pg_sys::FLOAT4OID, -1),
                PrimitiveType::Double => PgType::new(pg_sys::FLOAT8OID, -1),
                PrimitiveType::Decimal { precision, scale } => {
                    return Some(PgType {
                        oid: pg_sys::NUMERICOID,
                        typmod: numeric_typmod(*precision, *scale as i32),
                    });
                }
                PrimitiveType::Date => PgType::new(pg_sys::DATEOID, -1),
                PrimitiveType::Time => PgType::new(pg_sys::TIMEOID, -1),
                // PG timestamps have microsecond resolution. Query admission
                // rejects operations requiring nanosecond equality or ordering;
                // projection and validity tests retain their existing mapping.
                // ForeignSchemaBinding::validate_update rejects PG row rewrites
                // that would lose the source precision. Nested JSONB preserves it.
                PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                    PgType::new(pg_sys::TIMESTAMPOID, -1)
                }
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                    PgType::new(pg_sys::TIMESTAMPTZOID, -1)
                }
                PrimitiveType::String => PgType::new(pg_sys::TEXTOID, -1),
                PrimitiveType::Uuid => PgType::new(pg_sys::UUIDOID, -1),
                // Binary and fixed values use PostgreSQL bytea.
                PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
                    PgType::new(pg_sys::BYTEAOID, -1)
                }
                PrimitiveType::Unknown => return None,
            },
            Type::List(list) => {
                if matches!(
                    list.element_field.field_type.as_ref(),
                    Type::Primitive(_)
                ) {
                    let element =
                        Self::new(&list.element_field.field_type).canonical()?;
                    let oid = match element.oid {
                        pg_sys::BOOLOID => pg_sys::BOOLARRAYOID,
                        pg_sys::INT4OID => pg_sys::INT4ARRAYOID,
                        pg_sys::INT8OID => pg_sys::INT8ARRAYOID,
                        pg_sys::FLOAT4OID => pg_sys::FLOAT4ARRAYOID,
                        pg_sys::FLOAT8OID => pg_sys::FLOAT8ARRAYOID,
                        pg_sys::TEXTOID => pg_sys::TEXTARRAYOID,
                        pg_sys::NUMERICOID => pg_sys::NUMERICARRAYOID,
                        pg_sys::DATEOID => pg_sys::DATEARRAYOID,
                        pg_sys::TIMEOID => pg_sys::TIMEARRAYOID,
                        pg_sys::TIMESTAMPOID => pg_sys::TIMESTAMPARRAYOID,
                        pg_sys::TIMESTAMPTZOID => pg_sys::TIMESTAMPTZARRAYOID,
                        pg_sys::UUIDOID => pg_sys::UUIDARRAYOID,
                        pg_sys::BYTEAOID => pg_sys::BYTEAARRAYOID,
                        _ => return None,
                    };
                    PgType {
                        oid,
                        typmod: element.typmod,
                    }
                } else {
                    PgType::new(pg_sys::JSONBOID, -1)
                }
            }
            Type::Struct(_) | Type::Map(_) => PgType::new(pg_sys::JSONBOID, -1),
            Type::Variant(_) => return None,
        };
        Some(postgres)
    }

    pub(super) fn accepts(&self, oid: pg_sys::Oid, typmod: i32) -> bool {
        self.canonical().is_some_and(|canonical| {
            (oid, typmod) == (canonical.oid, canonical.typmod)
        })
    }

    /// Share declaration admission with execution-time schema binding.
    pub(super) fn validate(
        &self,
        name: &str,
        local: PgType,
        writable: bool,
    ) -> Result<(), IcebergFdwError> {
        SemanticArrowType::new(self.0).validate_supported()?;
        let canonical = self
            .canonical()
            .ok_or_else(|| IcebergError::UnsupportedColumnType(self.0.to_string()))?;
        let accepted = if writable {
            self.accepts(local.oid, local.typmod)
        } else {
            self.accepts_read(local.oid, local.typmod)
        };
        if !accepted {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "column {:?} has unsupported PostgreSQL type (oid {}, typmod {}); the default Iceberg mapping is (oid {}, typmod {})",
                    name,
                    u32::from(local.oid),
                    local.typmod,
                    u32::from(canonical.oid),
                    canonical.typmod,
                ),
            });
        }
        Ok(())
    }

    /// Reading a decimal may constrain its precision through numeric_recv.
    /// Keep the scale unchanged: rounding is not part of schema evolution.
    pub(super) fn accepts_read(&self, oid: pg_sys::Oid, typmod: i32) -> bool {
        if let Type::Primitive(PrimitiveType::Decimal { scale, .. }) = self.0 {
            return oid == pg_sys::NUMERICOID
                && (typmod == -1
                    || numeric_precision_scale(typmod)
                        .is_some_and(|target| target.scale == *scale as i32));
        }
        self.accepts(oid, typmod)
    }
}
