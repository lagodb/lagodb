//! Iceberg FDW domain errors and framework-boundary conversions.

use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::fdw::{
    ForeignImportError, ForeignModifyError, ForeignScanError,
    ForeignTableMaintenanceError, ForeignValidationError,
};
use pg_lakebase_core::plan_data::PlanDataError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::engine::predicate::IcebergFilterError;
use crate::error::IcebergError;

#[derive(Debug, Error)]
pub(crate) enum IcebergFdwError {
    #[error("foreign option {name:?} is not supported at this catalog level")]
    UnsupportedOption { name: String },

    #[error("invalid foreign option {name:?}: {reason}")]
    InvalidOption { name: String, reason: &'static str },

    #[error("required foreign option {name:?} is missing")]
    MissingOption { name: &'static str },

    #[error("{subject} must be valid UTF-8")]
    InvalidUtf8 { subject: &'static str },

    #[error("{subject} contains a NUL byte")]
    InteriorNul { subject: &'static str },

    #[error("invalid {kind} {name:?}: {reason}")]
    InvalidIdentifier {
        kind: &'static str,
        name: String,
        reason: &'static str,
    },

    #[error("the foreign table's REST catalog identity changed after planning")]
    PlanIdentityChanged,

    #[error("the Iceberg table generation or schema changed after planning")]
    PlanSourceChanged,

    #[error(
        "foreign table schema does not match the current Iceberg schema: {detail}"
    )]
    SchemaContractMismatch { detail: String },

    #[error("Iceberg FDW requires foreign server TYPE 'rest', found {actual:?}")]
    InvalidCatalogType { actual: String },

    #[error("permission denied for foreign server {server:?}")]
    ServerUsageDenied { server: String },

    #[error("foreign table is read-only")]
    ReadOnlyTable,

    #[error(
        "foreign server or user-mapping configuration changed after this writable transaction bound it"
    )]
    CatalogBindingChanged,

    #[error("Iceberg foreign tables do not support {operation}")]
    UnsupportedOperation { operation: &'static str },

    #[error("Iceberg FDW plan codec failed: {0}")]
    PlanData(#[from] PlanDataError),

    #[error("invalid Iceberg FDW plan: {detail}")]
    InvalidPlan { detail: &'static str },

    #[error(transparent)]
    Iceberg(#[from] IcebergError),

    #[error(transparent)]
    Filter(#[from] IcebergFilterError),
}

impl IcebergFdwError {
    pub(crate) fn unsupported_option(name: impl Into<String>) -> Self {
        Self::UnsupportedOption { name: name.into() }
    }

    pub(crate) fn invalid_option(
        name: impl Into<String>,
        reason: &'static str,
    ) -> Self {
        Self::InvalidOption {
            name: name.into(),
            reason,
        }
    }
}

impl SqlStateError for IcebergFdwError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::UnsupportedOption { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME
            }
            Self::InvalidOption { .. }
            | Self::InvalidUtf8 { .. }
            | Self::InteriorNul { .. }
            | Self::InvalidIdentifier { .. }
            | Self::InvalidCatalogType { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_INVALID_STRING_FORMAT
            }
            Self::MissingOption { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND
            }
            Self::PlanIdentityChanged
            | Self::PlanSourceChanged
            | Self::CatalogBindingChanged => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::ReadOnlyTable | Self::UnsupportedOperation { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::SchemaContractMismatch { .. } => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            Self::ServerUsageDenied { .. } => {
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
            }
            Self::PlanData(_) | Self::InvalidPlan { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            Self::Iceberg(error) => error.sql_error_code(),
            Self::Filter(error) => error.sql_error_code(),
        }
    }
}

impl From<iceberg_lite::Error> for IcebergFdwError {
    fn from(error: iceberg_lite::Error) -> Self {
        IcebergError::from(error).into()
    }
}

impl From<IcebergFdwError> for ForeignValidationError {
    fn from(error: IcebergFdwError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergFdwError> for ForeignImportError {
    fn from(error: IcebergFdwError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergFdwError> for ForeignScanError {
    fn from(error: IcebergFdwError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergFdwError> for ForeignModifyError {
    fn from(error: IcebergFdwError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergError> for ForeignModifyError {
    fn from(error: IcebergError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergError> for ForeignScanError {
    fn from(error: IcebergError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergFdwError> for ForeignTableMaintenanceError {
    fn from(error: IcebergFdwError) -> Self {
        Self::provider(error)
    }
}

impl From<IcebergError> for ForeignTableMaintenanceError {
    fn from(error: IcebergError) -> Self {
        Self::provider(error)
    }
}
