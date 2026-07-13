use std::fmt;

use pg_lakebase_core::diag::{
    PgError, PgReportError, SqlStateError, domain_error_report,
};
use pg_lakebase_core::extension_worker::WorkerContextError;
use pg_lakebase_core::maintenance::MaintenanceError;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;

use crate::state::RuntimeStateTransitionError;

pub(crate) type LakebaseResult<T> = Result<T, LakebaseError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerCatalogOperation {
    ResolveSchema,
    ResolveRelation,
    ResolveEntrypoint,
    Open,
    Scan,
    Insert,
    Delete,
}

impl fmt::Display for WorkerCatalogOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResolveSchema => "resolve lakebase schema",
            Self::ResolveRelation => "resolve lakebase.workers",
            Self::ResolveEntrypoint => "resolve worker entry point",
            Self::Open => "open lakebase.workers",
            Self::Scan => "scan lakebase.workers",
            Self::Insert => "insert into lakebase.workers",
            Self::Delete => "delete from lakebase.workers",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LakebaseError {
    #[error(
        "pg_lakebase_runtime must be loaded with shared_preload_libraries before use; add pg_lakebase_runtime to shared_preload_libraries and restart PostgreSQL"
    )]
    RuntimeNotPreloaded,

    #[error("pg_lakebase_runtime shared-memory layout mismatch")]
    SharedMemoryLayoutMismatch,

    #[error("pg_lakebase_runtime state transition failed: {source}")]
    RuntimeStateTransition {
        #[source]
        source: RuntimeStateTransitionError,
    },

    #[error("pg_lakebase.max_worker_registrations is exhausted")]
    MaxWorkerRegistrationsExhausted,

    #[error("Lakebase database runtime state is exhausted")]
    DatabaseRuntimeStateExhausted,

    #[error("Lakebase worker registration state is exhausted")]
    WorkerRegistrationStateExhausted,

    #[error("timed out waiting for Lakebase workers to stop")]
    StopDatabaseTimeout,

    #[error("timed out waiting for extension workers to stop")]
    StopExtensionTimeout,

    #[error("timed out waiting for Lakebase reconciler to stop")]
    StopReconcilerTimeout,

    #[error("timed out waiting for Lakebase worker to stop")]
    StopWorkerTimeout,

    #[error("worker registration requires superuser")]
    WorkerRegistrationRequiresSuperuser,

    #[error("worker deregistration requires superuser")]
    WorkerDeregistrationRequiresSuperuser,

    #[error("workers can only be registered by an extension script")]
    WorkerRegistrationRequiresExtensionScript,

    #[error("workers can only be deregistered by an extension script")]
    WorkerDeregistrationRequiresExtensionScript,

    #[error("worker name must contain between 1 and 255 bytes")]
    InvalidWorkerName,

    #[error("worker entry point must belong to the registering extension")]
    EntryPointNotOwnedByExtension,

    #[error("registering extension does not exist")]
    RegisteringExtensionMissing,

    #[error("worker entry point does not exist")]
    EntryPointMissing,

    #[error("worker entry point must have signature (internal) RETURNS bigint")]
    InvalidEntryPointSignature,

    #[error("worker entry point schema does not exist")]
    EntryPointSchemaMissing,

    #[error("lakebase.workers does not exist")]
    WorkersTableMissing,

    #[error("worker '{extension_name}.{worker_name}' is not registered")]
    WorkerNotRegistered {
        extension_name: String,
        worker_name: String,
    },

    #[error("failed to {operation}: {source}")]
    WorkerCatalog {
        operation: WorkerCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("failed to prepare Lakebase worker entry point: {source}")]
    WorkerEntrypointPreparation {
        #[source]
        source: pgrx::spi::Error,
    },

    #[error("failed to inspect maintenance queue before DROP EXTENSION: {source}")]
    MaintenanceQueueInspection {
        #[source]
        source: pgrx::spi::Error,
    },

    #[error("maintenance queue count returned no row before DROP EXTENSION")]
    MaintenanceQueueCountMissing,

    #[error(
        "cannot drop pg_lakebase_runtime while {pending} maintenance items are pending"
    )]
    MaintenanceQueueNotEmpty { pending: i64 },

    #[error("Lakebase worker context error: {source}")]
    WorkerContext {
        #[source]
        source: WorkerContextError,
    },

    #[error("failed to retry maintenance item: {source}")]
    RetryMaintenanceItem {
        #[source]
        source: MaintenanceError,
    },
}

impl LakebaseError {
    pub(crate) fn report(self) -> ! {
        PgReportError::from_domain_error(self).report()
    }
}

impl SqlStateError for LakebaseError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::RuntimeNotPreloaded
            | Self::WorkerRegistrationRequiresExtensionScript
            | Self::WorkerDeregistrationRequiresExtensionScript
            | Self::EntryPointNotOwnedByExtension
            | Self::RegisteringExtensionMissing => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }

            Self::SharedMemoryLayoutMismatch
            | Self::RuntimeStateTransition { .. }
            | Self::WorkerContext { .. } => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            Self::MaxWorkerRegistrationsExhausted
            | Self::DatabaseRuntimeStateExhausted
            | Self::WorkerRegistrationStateExhausted => {
                PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
            }

            Self::StopDatabaseTimeout
            | Self::StopExtensionTimeout
            | Self::StopReconcilerTimeout
            | Self::StopWorkerTimeout => PgSqlErrorCode::ERRCODE_QUERY_CANCELED,

            Self::WorkerRegistrationRequiresSuperuser
            | Self::WorkerDeregistrationRequiresSuperuser => {
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
            }

            Self::InvalidWorkerName => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            Self::EntryPointMissing => PgSqlErrorCode::ERRCODE_UNDEFINED_FUNCTION,

            Self::InvalidEntryPointSignature => {
                PgSqlErrorCode::ERRCODE_INVALID_FUNCTION_DEFINITION
            }

            Self::EntryPointSchemaMissing | Self::WorkerNotRegistered { .. } => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }

            Self::MaintenanceQueueCountMissing => {
                PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
            }

            Self::WorkersTableMissing => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,

            Self::WorkerCatalog { source, .. } => source.sql_error_code(),

            Self::WorkerEntrypointPreparation { .. }
            | Self::MaintenanceQueueInspection { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            Self::MaintenanceQueueNotEmpty { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE
            }

            Self::RetryMaintenanceItem { source } => source.sql_error_code(),
        }
    }
}

impl From<LakebaseError> for ErrorReport {
    fn from(value: LakebaseError) -> Self {
        domain_error_report(value)
    }
}

impl From<RuntimeStateTransitionError> for LakebaseError {
    fn from(source: RuntimeStateTransitionError) -> Self {
        Self::RuntimeStateTransition { source }
    }
}
