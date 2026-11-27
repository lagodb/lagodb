//! Errors owned by Iceberg planned-filter serialization and runtime binding.

use lagodb_core::datum::DecimalCodecError;
use lagodb_core::diag::SqlStateError;
use lagodb_core::plan_data::PlanDataError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum IcebergFilterError {
    #[error("Iceberg planned filter contains unknown boolean test tag {0}")]
    UnknownBooleanTest(i32),

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

    #[error("Iceberg planned filter contains an invalid character(n) typmod {0}")]
    InvalidBpcharTypmod(i32),

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

    #[error("Iceberg Decimal128 predicate binding failed: {0}")]
    DecimalCodec(#[from] DecimalCodecError),
}

impl SqlStateError for IcebergFilterError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::UnknownBooleanTest(_)
            | Self::PlanData(_)
            | Self::UnknownNodeTag(_)
            | Self::UnknownOperatorTag(_)
            | Self::UnknownValueTypeTag(_)
            | Self::InvalidDecimalShape
            | Self::InvalidBpcharTypmod(_)
            | Self::EmptyLogicalNode { .. }
            | Self::BindingSlotOutOfBounds { .. }
            | Self::SchemaMismatch { .. }
            | Self::DecimalCodec(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
