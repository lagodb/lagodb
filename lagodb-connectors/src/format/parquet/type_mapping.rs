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

pub(super) struct ParquetTypeMapping;

pub(crate) use read::ParquetReadMode;

impl ParquetTypeMapping {
    /// Both INSERT and COPY TO derive the entire schema from PG declarations.
    pub(super) fn write_field(
        name: &str,
        ty: PgType,
    ) -> Result<Field, ConnectorError> {
        let ty = ty.base();
        Self::check_array_type(ty)?;
        let data_type = if let Some(element) = ty.element() {
            DataType::List(Arc::new(Self::write_field("item", element)?))
        } else if ty.is_composite() {
            let fields = Self::composite_fields(ty)?
                .into_iter()
                .map(|attribute| {
                    Self::write_field(&attribute.name, attribute.pg_type)
                })
                .collect::<Result<Vec<_>, _>>()?;
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
                    return Ok(Field::new(name, DataType::FixedSizeBinary(16), true)
                        .with_extension_type(Uuid));
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
                    let numeric = numeric_precision_scale(ty.typmod)
                        .ok_or_else(|| {
                            ConnectorError::invalid_object_schema(
                                FormatKind::Parquet,
                                "unconstrained numeric cannot be represented losslessly as Parquet Decimal128",
                            )
                        })?;
                    if numeric.precision > 38
                        || numeric.scale < 0
                        || numeric.scale as u32 > numeric.precision
                    {
                        return Err(ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            "numeric precision/scale is outside Parquet Decimal128 limits",
                        ));
                    }
                    DataType::Decimal128(numeric.precision as u8, numeric.scale as i8)
                }
                _ => {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "PostgreSQL type OID {} is not supported by Parquet",
                            ty.oid
                        ),
                    ));
                }
            }
        };
        Ok(Field::new(name, data_type, true))
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

    fn check_array_type(ty: PgType) -> Result<(), ConnectorError> {
        // These catalog types expose element OIDs but are not ordinary arrays.
        if matches!(ty.oid, pg_sys::INT2VECTOROID | pg_sys::OIDVECTOROID) {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "int2vector and oidvector are not supported as Parquet arrays",
            ));
        }
        Ok(())
    }

    fn composite_fields(ty: PgType) -> Result<Vec<PgCompositeField>, ConnectorError> {
        // The caller established that ty is a catalog composite. Hold its
        // definition stable until transaction end, as managed schema binding does.
        let relid = unsafe { pg_sys::get_typ_typrelid(ty.oid) };
        let relation = RelationGuard::open_relation_retain_lock(
            relid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )
        .map_err(PgReportError::from_pg_error)?;
        if relation.as_handle().relkind() as u8 != pg_sys::RELKIND_COMPOSITE_TYPE {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "Parquet composite columns require a named composite type, not a table row type",
            ));
        }
        Ok(ty.composite_fields())
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
