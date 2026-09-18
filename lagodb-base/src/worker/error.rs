use std::ffi::CString;
use std::fmt;

use lagodb_core::diag::{PgError, PgReportError, SqlStateError};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;

pub(crate) trait WorkerCatalogResultExt<T> {
    fn map_worker_catalog_err(
        self,
        operation: WorkerCatalogOperation,
    ) -> Result<T, WorkerError>;
}

impl<T> WorkerCatalogResultExt<T> for Result<T, PgError> {
    #[inline]
    fn map_worker_catalog_err(
        self,
        operation: WorkerCatalogOperation,
    ) -> Result<T, WorkerError> {
        self.map_err(|source| WorkerError::WorkerCatalog { operation, source })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerCatalogOperation {
    ResolveSchema,
    ResolveRelation,
    ResolveIndex,
    ResolveSequence,
    AllocateId,
    ResolveEntrypoint,
    Open,
    Scan,
    Insert,
    Delete,
}

impl fmt::Display for WorkerCatalogOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResolveSchema => "resolve lagodb schema",
            Self::ResolveRelation => "resolve lagodb.workers",
            Self::ResolveIndex => "resolve lagodb.workers index",
            Self::ResolveSequence => "resolve lagodb.worker_id_seq",
            Self::AllocateId => "allocate a LagoDB worker ID",
            Self::ResolveEntrypoint => "resolve worker entry point",
            Self::Open => "open lagodb.workers",
            Self::Scan => "scan lagodb.workers",
            Self::Insert => "insert into lagodb.workers",
            Self::Delete => "delete from lagodb.workers",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerError {
    #[error("cannot PREPARE a transaction with pending LagoDB actions")]
    PreparedTransactionWithRuntimeActions,

    #[error("workers can only be registered by an extension script")]
    WorkerRegistrationRequiresExtensionScript,

    #[error("worker name must contain between 1 and 255 bytes")]
    InvalidWorkerName,

    #[error("registering extension does not exist")]
    RegisteringExtensionMissing,

    #[error("worker entry point does not exist")]
    EntryPointMissing,

    #[error(
        "worker entry point must be a non-set-returning function with signature (internal) RETURNS bigint"
    )]
    InvalidEntryPointSignature,

    #[error("worker entry point schema does not exist")]
    EntryPointSchemaMissing,

    #[error("lagodb.workers does not exist")]
    WorkersTableMissing,

    #[error("lagodb.workers primary key index does not exist")]
    WorkersPrimaryKeyMissing,

    #[error("lagodb.workers name key index does not exist")]
    WorkersNameIndexMissing,

    #[error("worker locator ({extension_name:?}, {worker_name:?}) is not registered")]
    WorkerNotRegistered {
        extension_name: CString,
        worker_name: String,
    },

    #[error("worker '{worker_name}' is not registered")]
    WorkerNameNotRegistered { worker_name: String },

    #[error("worker id {worker_id} is not registered")]
    WorkerIdNotRegistered { worker_id: i32 },

    #[error("failed to {operation}: {source}")]
    WorkerCatalog {
        operation: WorkerCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("lagodb.worker_id_seq does not exist")]
    WorkerIdSequenceMissing,

    #[error("failed to prepare LagoDB worker entry point: {source}")]
    WorkerEntrypointPreparation {
        #[source]
        source: pgrx::spi::Error,
    },
}

impl WorkerError {
    fn into_report(self) -> PgReportError {
        PgReportError::from_domain_error(self)
    }

    pub(crate) fn report(self) -> ! {
        self.into_report().report()
    }
}

impl SqlStateError for WorkerError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::WorkerRegistrationRequiresExtensionScript
            | Self::RegisteringExtensionMissing => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }

            Self::PreparedTransactionWithRuntimeActions => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }

            Self::InvalidWorkerName => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            Self::EntryPointMissing => PgSqlErrorCode::ERRCODE_UNDEFINED_FUNCTION,

            Self::InvalidEntryPointSignature => {
                PgSqlErrorCode::ERRCODE_INVALID_FUNCTION_DEFINITION
            }

            Self::EntryPointSchemaMissing
            | Self::WorkerNotRegistered { .. }
            | Self::WorkerNameNotRegistered { .. }
            | Self::WorkerIdNotRegistered { .. } => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }

            Self::WorkersTableMissing
            | Self::WorkersPrimaryKeyMissing
            | Self::WorkersNameIndexMissing
            | Self::WorkerIdSequenceMissing => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE
            }

            Self::WorkerCatalog { source, .. } => source.sql_error_code(),

            Self::WorkerEntrypointPreparation { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<WorkerError> for ErrorReport {
    fn from(value: WorkerError) -> Self {
        value.into_report().into_report()
    }
}
