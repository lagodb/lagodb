//! Errors owned by the Iceberg planned-filter facet.

use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::plan_data::PlanDataError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum IcebergFilterError {
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

    #[error("Iceberg planned filter contains an empty {kind} node")]
    EmptyLogicalNode { kind: &'static str },

    #[error(
        "Iceberg planned filter slot {index} is outside its {binding_count} bindings"
    )]
    BindingSlotOutOfBounds { index: usize, binding_count: usize },

    #[error("Iceberg filter planner has no field binding for attno {0}")]
    MissingFieldBinding(pg_sys::AttrNumber),

    #[error(
        "Iceberg planned filter schema id {planned} does not match execution schema id {execution}"
    )]
    SchemaMismatch { planned: i32, execution: i32 },

    #[error("failed to decode PostgreSQL Datum with type OID {}", u32::from(*type_oid))]
    DatumDecode { type_oid: pg_sys::Oid },
}

impl SqlStateError for IcebergFilterError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::PlanData(_)
            | Self::UnknownNodeTag(_)
            | Self::UnknownOperatorTag(_)
            | Self::UnknownValueTypeTag(_)
            | Self::EmptyLogicalNode { .. }
            | Self::BindingSlotOutOfBounds { .. }
            | Self::MissingFieldBinding(_)
            | Self::SchemaMismatch { .. }
            | Self::DatumDecode { .. } => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
