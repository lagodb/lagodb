//! Iceberg FDW domain errors and framework-boundary conversions.

use lagodb_core::diag::SqlStateError;
use lagodb_core::fdw::{
    ForeignImportError, ForeignModifyError, ForeignScanError,
    ForeignTableMaintenanceError, ForeignValidationError,
};
use lagodb_core::plan_data::PlanDataError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::error::IcebergError;
use crate::predicate::IcebergFilterError;

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

    #[error("the foreign table's catalog identity changed after planning")]
    PlanIdentityChanged,

    #[error("the Iceberg table generation or schema changed after planning")]
    PlanSourceChanged,

    #[error(
        "foreign table schema does not match the selected Iceberg schema: {detail}"
    )]
    SchemaContractMismatch { detail: String },

    #[error(
        "cannot create Iceberg table: column {column:?} has unsupported PostgreSQL type {pg_type}"
    )]
    UnsupportedTableCreationType { column: String, pg_type: String },

    #[error(
        "Iceberg schema ID {schema_id} does not exist in the selected table metadata"
    )]
    UnknownSchema { schema_id: i32 },

    #[error(
        "Iceberg table {table} does not exist and foreign option \"create_remote_if_missing\" is false"
    )]
    TableNotFound { table: String },

    #[error(
        "Iceberg table {table} is already being provisioned by another foreign table in this transaction; drop the provisioning relation or commit before creating another mapping"
    )]
    StagedTableAlreadyProvisioned { table: String },

    #[error(
        "Iceberg table {table} no longer matches the generation bound for writing by this transaction"
    )]
    TableGenerationConflict { table: String },

    #[error(
        "Iceberg FDW requires foreign server TYPE 'rest' or 'sqlite', found {actual:?}"
    )]
    InvalidCatalogType { actual: String },

    #[error("permission denied for foreign server {server:?}")]
    ServerUsageDenied { server: String },

    #[error(
        "configuring a SQLite catalog requires pg_read_server_files and pg_write_server_files privileges"
    )]
    LocalFileAccessDenied,

    #[error(
        "cannot UPDATE Iceberg foreign table: column {column:?} maps nanosecond timestamps to PostgreSQL microseconds; rewriting the row would lose precision even if the column is not modified"
    )]
    LossyTimestampUpdate { column: String },

    #[error(
        "foreign server or user-mapping configuration changed while the Iceberg catalog was in use"
    )]
    CatalogConfigurationChanged,

    #[error(
        "cannot change immutable Iceberg catalog identity option {name:?}; create a new foreign server instead"
    )]
    ImmutableCatalogIdentityOption { name: String },

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
            | Self::CatalogConfigurationChanged
            | Self::StagedTableAlreadyProvisioned { .. }
            | Self::TableGenerationConflict { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::TableNotFound { .. } => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
            Self::LossyTimestampUpdate { .. }
            | Self::ImmutableCatalogIdentityOption { .. }
            | Self::UnsupportedOperation { .. }
            | Self::UnsupportedTableCreationType { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::UnknownSchema { .. } | Self::SchemaContractMismatch { .. } => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            Self::ServerUsageDenied { .. } | Self::LocalFileAccessDenied => {
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
