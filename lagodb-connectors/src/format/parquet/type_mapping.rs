//! Parquet admission and PostgreSQL declarations; value codecs live in Arrow.

mod infer;
mod read;

use std::sync::Arc;

use arrow_schema::extension::Uuid;
use arrow_schema::{DataType, Field, TimeUnit};
use lagodb_core::datum::{PgCompositeField, PgType, numeric_precision_scale};
use lagodb_core::diag::PgReportError;
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

pub(crate) struct ParquetTypeMapping;

pub(crate) use read::ParquetReadMode;

/// How a declaration maps a domain, at any nesting depth.
#[derive(Clone, Copy)]
enum DomainPolicy {
    /// Written Datums already passed PostgreSQL's domain coercion.
    Base,
    /// Columnar reads hand values to the query engine without
    /// `CoerceToDomain`, so domain constraints would go unchecked.
    Reject,
}

/// A declaration's Parquet representation, or why it has none. Catalog and
/// lock failures travel separately as `ConnectorError`.
enum Mapping<T> {
    Supported(T),
    Unsupported(Box<str>),
}

impl<T> Mapping<T> {
    fn unsupported(reason: impl Into<Box<str>>) -> Self {
        Self::Unsupported(reason.into())
    }

    /// Paths that require the representation report its absence as a schema
    /// error.
    fn require(self) -> Result<T, ConnectorError> {
        match self {
            Self::Supported(value) => Ok(value),
            Self::Unsupported(reason) => Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                reason,
            )),
        }
    }

    fn supported(self) -> Option<T> {
        match self {
            Self::Supported(value) => Some(value),
            Self::Unsupported(_) => None,
        }
    }
}

impl ParquetTypeMapping {
    /// Both INSERT and COPY TO derive the entire schema from PG declarations.
    pub(super) fn write_field(
        name: &str,
        ty: PgType,
    ) -> Result<Field, ConnectorError> {
        Self::declared_field(name, ty, DomainPolicy::Base)?.require()
    }

    /// The canonical Arrow field a columnar read emits for a declaration, or
    /// `None` when the declaration has no columnar representation; catalog
    /// failures propagate. Composite members are read under the composite
    /// type's retained lock, after the domain check of their enclosing type.
    pub(super) fn declared_read_field(
        name: &str,
        ty: PgType,
    ) -> Result<Option<Field>, ConnectorError> {
        Ok(Self::declared_field(name, ty, DomainPolicy::Reject)?.supported())
    }

    fn declared_field(
        name: &str,
        ty: PgType,
        domains: DomainPolicy,
    ) -> Result<Mapping<Field>, ConnectorError> {
        let base = ty.base();
        if matches!(domains, DomainPolicy::Reject) && base.oid != ty.oid {
            return Ok(Mapping::unsupported(format!(
                "column {name:?}: domain types have no columnar read"
            )));
        }
        let ty = base;
        if let Mapping::Unsupported(reason) = Self::check_array_type(ty) {
            return Ok(Mapping::Unsupported(reason));
        }
        let data_type = if let Some(element) = ty.element() {
            match Self::declared_field("item", element, domains)? {
                Mapping::Supported(field) => DataType::List(Arc::new(field)),
                unsupported => return Ok(unsupported),
            }
        } else if ty.is_composite() {
            let attributes = match Self::composite_fields(ty)? {
                Mapping::Supported(attributes) => attributes,
                Mapping::Unsupported(reason) => {
                    return Ok(Mapping::Unsupported(reason));
                }
            };
            let mut fields = Vec::with_capacity(attributes.len());
            for attribute in attributes {
                match Self::declared_field(
                    &attribute.name,
                    attribute.pg_type,
                    domains,
                )? {
                    Mapping::Supported(field) => fields.push(field),
                    unsupported => return Ok(unsupported),
                }
            }
            DataType::Struct(fields.into())
        } else if let Some(primitive) = Self::primitive_storage_type(ty.oid) {
            primitive
        } else {
            match ty.oid {
                pg_sys::BOOLOID => DataType::Boolean,
                pg_sys::TEXTOID
                | pg_sys::VARCHAROID
                | pg_sys::BPCHAROID
                | pg_sys::NAMEOID
                | pg_sys::JSONOID
                | pg_sys::JSONBOID => DataType::Utf8,
                pg_sys::BYTEAOID => DataType::LargeBinary,
                pg_sys::UUIDOID => {
                    return Ok(Mapping::Supported(
                        Field::new(name, DataType::FixedSizeBinary(16), true)
                            .with_extension_type(Uuid),
                    ));
                }
                pg_sys::DATEOID => DataType::Date32,
                pg_sys::TIMEOID => DataType::Time64(TimeUnit::Microsecond),
                pg_sys::TIMESTAMPOID => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                pg_sys::TIMESTAMPTZOID => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
                }
                pg_sys::NUMERICOID => {
                    let Some(numeric) = numeric_precision_scale(ty.typmod) else {
                        return Ok(Mapping::unsupported(
                            "unconstrained numeric cannot be represented losslessly as Parquet Decimal128",
                        ));
                    };
                    if numeric.precision > 38
                        || numeric.scale < 0
                        || numeric.scale as u32 > numeric.precision
                    {
                        return Ok(Mapping::unsupported(
                            "numeric precision/scale is outside Parquet Decimal128 limits",
                        ));
                    }
                    DataType::Decimal128(numeric.precision as u8, numeric.scale as i8)
                }
                _ => {
                    return Ok(Mapping::unsupported(format!(
                        "PostgreSQL type OID {} is not supported by Parquet",
                        ty.oid
                    )));
                }
            }
        };
        Ok(Mapping::Supported(Field::new(name, data_type, true)))
    }

    /// The same fixed numeric representation governs writes and read admission.
    /// General Arrow codec conversions do not expand Parquet's type contract.
    fn primitive_storage_type(oid: pg_sys::Oid) -> Option<DataType> {
        match oid {
            pg_sys::INT2OID => Some(DataType::Int16),
            pg_sys::INT4OID => Some(DataType::Int32),
            pg_sys::INT8OID => Some(DataType::Int64),
            pg_sys::FLOAT4OID => Some(DataType::Float32),
            pg_sys::FLOAT8OID => Some(DataType::Float64),
            _ => None,
        }
    }

    fn check_array_type(ty: PgType) -> Mapping<()> {
        // These catalog types expose element OIDs but are not ordinary arrays.
        if matches!(ty.oid, pg_sys::INT2VECTOROID | pg_sys::OIDVECTOROID) {
            return Mapping::unsupported(
                "int2vector and oidvector are not supported as Parquet arrays",
            );
        }
        Mapping::Supported(())
    }

    fn composite_fields(
        ty: PgType,
    ) -> Result<Mapping<Vec<PgCompositeField>>, ConnectorError> {
        // The caller established that ty is a catalog composite. Hold its
        // definition stable until transaction end, as managed schema binding does.
        let relid = unsafe { pg_sys::get_typ_typrelid(ty.oid) };
        let relation = RelationGuard::open_relation_retain_lock(
            relid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )
        .map_err(PgReportError::from_pg_error)?;
        if relation.as_handle().relkind() as u8 != pg_sys::RELKIND_COMPOSITE_TYPE {
            return Ok(Mapping::unsupported(
                "Parquet composite columns require a named composite type, not a table row type",
            ));
        }
        Ok(Mapping::Supported(ty.composite_fields()))
    }

    fn incompatible(field: &Field, target: PgType) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Parquet,
            format!(
                "column {:?} cannot map {} to PostgreSQL type OID {}",
                field.name(),
                field.data_type(),
                target.oid
            ),
        )
    }

    fn unsupported(field: &Field) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Parquet,
            format!(
                "column {:?} uses unsupported source field {field}",
                field.name()
            ),
        )
    }
}
