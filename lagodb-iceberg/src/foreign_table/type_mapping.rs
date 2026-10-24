//! Iceberg-first PostgreSQL declarations and the explicit foreign DDL contract.

use iceberg_lite::spec::{PrimitiveType, Type};
use lagodb_core::datum::{PgType, numeric_precision_scale, numeric_typmod};
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use crate::error::IcebergError;
use crate::schema::arrow_type::SemanticArrowType;

pub(super) struct ForeignType<'a>(&'a Type);

pub(super) struct PostgresForeignType(PgType);

pub(super) enum CreatedForeignType {
    Primitive(PrimitiveType),
    List(PrimitiveType),
}

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
            let (access, requirement) = if writable {
                ("written from", "writes require")
            } else {
                ("read as", "the default mapping is")
            };
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "column {:?}: Iceberg type {} cannot be {} PostgreSQL type {}; {} {}",
                    name,
                    self.0,
                    access,
                    local.display_name(),
                    requirement,
                    canonical.display_name(),
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

impl PostgresForeignType {
    pub(super) fn new(source: PgType) -> Self {
        Self(source)
    }

    pub(super) fn to_iceberg(
        &self,
        column: &str,
    ) -> Result<CreatedForeignType, IcebergFdwError> {
        let element_oid = match self.0.oid {
            pg_sys::BOOLARRAYOID => Some(pg_sys::BOOLOID),
            pg_sys::INT4ARRAYOID => Some(pg_sys::INT4OID),
            pg_sys::INT8ARRAYOID => Some(pg_sys::INT8OID),
            pg_sys::FLOAT4ARRAYOID => Some(pg_sys::FLOAT4OID),
            pg_sys::FLOAT8ARRAYOID => Some(pg_sys::FLOAT8OID),
            pg_sys::NUMERICARRAYOID => Some(pg_sys::NUMERICOID),
            pg_sys::DATEARRAYOID => Some(pg_sys::DATEOID),
            pg_sys::TIMEARRAYOID => Some(pg_sys::TIMEOID),
            pg_sys::TIMESTAMPARRAYOID => Some(pg_sys::TIMESTAMPOID),
            pg_sys::TIMESTAMPTZARRAYOID => Some(pg_sys::TIMESTAMPTZOID),
            pg_sys::TEXTARRAYOID => Some(pg_sys::TEXTOID),
            pg_sys::UUIDARRAYOID => Some(pg_sys::UUIDOID),
            pg_sys::BYTEAARRAYOID => Some(pg_sys::BYTEAOID),
            _ => None,
        };
        match element_oid {
            Some(oid) => Self(PgType::new(oid, self.0.typmod))
                .primitive(column)
                .map(CreatedForeignType::List),
            None => self.primitive(column).map(CreatedForeignType::Primitive),
        }
    }

    fn primitive(&self, column: &str) -> Result<PrimitiveType, IcebergFdwError> {
        let primitive = match (self.0.oid, self.0.typmod) {
            (pg_sys::BOOLOID, -1) => PrimitiveType::Boolean,
            (pg_sys::INT4OID, -1) => PrimitiveType::Int,
            (pg_sys::INT8OID, -1) => PrimitiveType::Long,
            (pg_sys::FLOAT4OID, -1) => PrimitiveType::Float,
            (pg_sys::FLOAT8OID, -1) => PrimitiveType::Double,
            (pg_sys::DATEOID, -1) => PrimitiveType::Date,
            (pg_sys::TIMEOID, -1) => PrimitiveType::Time,
            (pg_sys::TIMESTAMPOID, -1) => PrimitiveType::Timestamp,
            (pg_sys::TIMESTAMPTZOID, -1) => PrimitiveType::Timestamptz,
            (pg_sys::TEXTOID, -1) => PrimitiveType::String,
            (pg_sys::UUIDOID, -1) => PrimitiveType::Uuid,
            (pg_sys::BYTEAOID, -1) => PrimitiveType::Binary,
            (pg_sys::NUMERICOID, typmod) => {
                let Some(value) = numeric_precision_scale(typmod) else {
                    return Err(self.unsupported(column));
                };
                let (Ok(precision), Ok(scale)) =
                    (u8::try_from(value.precision), u8::try_from(value.scale))
                else {
                    return Err(self.unsupported(column));
                };
                if precision > 38 || scale > precision {
                    return Err(self.unsupported(column));
                }
                PrimitiveType::Decimal {
                    precision: u32::from(precision),
                    scale: u32::from(scale),
                }
            }
            _ => return Err(self.unsupported(column)),
        };
        Ok(primitive)
    }

    fn unsupported(&self, column: &str) -> IcebergFdwError {
        IcebergFdwError::UnsupportedTableCreationType {
            column: column.to_owned(),
            pg_type: self.0.display_name(),
        }
    }
}
