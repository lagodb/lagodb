//! Errors owned by the Iceberg planned-filter facet.

use lagodb_core::diag::SqlStateError;
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::tuple::DecimalCodecError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum IcebergFilterError {
    #[error("Iceberg planned filter contains unknown boolean test tag {0}")]
    UnknownBooleanTest(i32),
    #[error(transparent)]
    Iceberg(#[from] IcebergError),

    #[error("Iceberg planned-filter codec failed: {0}")]
    PlanData(#[from] PlanDataError),

    #[error("Iceberg planned filter contains unknown node tag {0}")]
    UnknownNodeTag(i32),

    #[error("Iceberg planned filter contains unknown operator tag {0}")]
    UnknownOperatorTag(i32),

    #[error("Iceberg planned filter contains unknown value type tag {0}")]
    UnknownValueTypeTag(i32),

    #[error("Iceberg planned filter contains an invalid Decimal128 shape")]
    InvalidDecimalShape,

    #[error("Iceberg planned filter contains an empty {kind} node")]
    EmptyLogicalNode { kind: &'static str },

    #[error(
        "Iceberg planned filter slot {index} is outside its {binding_count} bindings"
    )]
    BindingSlotOutOfBounds { index: usize, binding_count: usize },

    #[error(
        "Iceberg planned filter schema id {planned} does not match execution schema id {execution}"
    )]
    SchemaMismatch { planned: i32, execution: i32 },

    #[error("failed to decode PostgreSQL Datum with type OID {}", u32::from(*type_oid))]
    DatumDecode { type_oid: pg_sys::Oid },

    #[error("Iceberg Decimal128 predicate binding failed: {0}")]
    DecimalCodec(#[from] DecimalCodecError),
}

impl SqlStateError for IcebergFilterError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::UnknownBooleanTest(_)
            | Self::PlanData(_)
            | Self::UnknownNodeTag(_)
            | Self::UnknownOperatorTag(_)
            | Self::UnknownValueTypeTag(_)
            | Self::InvalidDecimalShape
            | Self::EmptyLogicalNode { .. }
            | Self::BindingSlotOutOfBounds { .. }
            | Self::SchemaMismatch { .. }
            | Self::DatumDecode { .. }
            | Self::DecimalCodec(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
