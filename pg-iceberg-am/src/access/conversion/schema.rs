//! Iceberg → Arrow schema bridging for the access-method layer.
//!
//! Single source of truth for the Iceberg → Arrow primitive type table is
//! `iceberg_lite::arrow`. We deliberately do *not* maintain a parallel table
//! here: in the past pg-iceberg-am had its own mapping that disagreed with
//! iceberg-lite (notably mapping `timestamp_ns`/`timestamptz_ns` to
//! microsecond Arrow types while the Parquet writer's view of the table
//! expected nanoseconds), which produced silent schema-mismatch failures on
//! the DML write path.
//!
//! What lives here is what is *AM-specific*:
//!
//! - thin error-mapping wrappers over the iceberg-lite converters, so callers
//!   stay on `IcebergError` / `IcebergResult`;
//! - [`ValidateSupported`], which rejects schemas/types containing column
//!   shapes pg-iceberg-am's per-column dispatch can't materialize. Both the
//!   DML write path ([`super::RowRecordBatchBuilder::new`]) and the scan
//!   path ([`super::RecordBatchRowReader::new`]) call it at construction
//!   time, so the same shape error surfaces at the same boundary regardless
//!   of which direction triggered it.

use arrow_schema::Schema;
use iceberg_lite::spec::{PrimitiveType, Schema as IcebergSchema, Type};

use super::complex::SupportedListElement;
use crate::error::{IcebergError, IcebergResult};

/// Convert an Iceberg schema to an Arrow schema.
///
/// Thin adapter over [`iceberg_lite::arrow::schema_to_arrow_schema`] that
/// re-routes its error type into [`IcebergError`]. The Arrow schema produced
/// here is what the DML write path's [`super::RowRecordBatchBuilder`] will
/// hand to the Parquet writer; making sure it is the same schema the Parquet
/// writer derives internally is the point of this module.
pub fn iceberg_schema_to_arrow_schema(
    schema: &IcebergSchema,
) -> IcebergResult<Schema> {
    Ok(iceberg_lite::arrow::schema_to_arrow_schema(schema)?)
}

/// Convert one Iceberg [`Type`] to an Arrow `DataType`.
///
/// Used by nested-type builders (e.g. List element fields). Same rationale as
/// [`iceberg_schema_to_arrow_schema`].
pub(crate) fn iceberg_type_to_arrow_type(
    iceberg_type: &Type,
) -> IcebergResult<arrow_schema::DataType> {
    Ok(iceberg_lite::arrow::type_to_arrow_type(iceberg_type)?)
}

/// Reject schemas/types that pg-iceberg-am's DML/scan paths cannot yet
/// handle.
///
/// Implemented for [`IcebergSchema`], [`Type`], and [`PrimitiveType`] so
/// callers express the check as `schema.validate_supported()?` rather than
/// threading values through a chain of similarly-named free functions.
///
/// Top-level columns are accepted for any primitive type or for a single
/// level of `List` whose element type [`SupportedListElement`] admits.
/// Nested lists, `Struct`, `Map`, and lists of unsupported element types are
/// rejected at the boundary so a later "build first batch" call doesn't
/// surface a generic `UnsupportedColumnType` from deep inside the per-row
/// dispatch loop.
pub(crate) trait ValidateSupported {
    fn validate_supported(&self) -> IcebergResult<()>;
}

impl ValidateSupported for IcebergSchema {
    fn validate_supported(&self) -> IcebergResult<()> {
        for field in self.as_struct().fields() {
            field.field_type.validate_supported()?;
        }
        Ok(())
    }
}

impl ValidateSupported for Type {
    fn validate_supported(&self) -> IcebergResult<()> {
        match self {
            Type::Primitive(p) => p.validate_supported(),
            Type::List(list) => match list.element_field.field_type.as_ref() {
                Type::Primitive(p) => {
                    if SupportedListElement::from_primitive(p).is_some() {
                        p.validate_supported()
                    } else {
                        Err(IcebergError::UnsupportedColumnType(format!(
                            "list element type {p:?} is not supported"
                        )))
                    }
                }
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
        }
    }
}

/// Reject primitive shapes pg-iceberg-am cannot encode into Arrow even though
/// `iceberg_lite::arrow::type_to_arrow_type` is willing to map them.
///
/// Currently the only such case is `Fixed(len)` with `len > i32::MAX`:
/// iceberg-lite falls through to Arrow `LargeBinary` for that range
/// (see `iceberg-lite/src/arrow/schema.rs`'s `Fixed` arm), but our writer
/// path uses `FixedSizeBinaryBuilder`, which is bounded by `i32`. Catching
/// it here makes the invariant in the write path (`*len as i32` becomes a
/// `try_from` once this validator has run) load-bearing.
///
/// PG-side schema construction (`schema_builder.rs`) maps PostgreSQL `uuid`
/// to Iceberg `Uuid`, not `Fixed(16)`, and does not otherwise create
/// `Fixed` columns. This only fires for externally-defined Iceberg tables
/// imported into pg-iceberg-am.
impl ValidateSupported for PrimitiveType {
    fn validate_supported(&self) -> IcebergResult<()> {
        match self {
            PrimitiveType::Fixed(len) if *len > i32::MAX as u64 => {
                Err(IcebergError::UnsupportedColumnType(format!(
                    "fixed[{len}] exceeds Arrow FixedSizeBinary i32 width limit"
                )))
            }
            _ => Ok(()),
        }
    }
}
