use std::fmt;

use pg_lakebase_core::diag::{PgError, PgReportError, SqlStateError};
use pg_lakebase_core::extension_worker::WorkerContextError;
use pg_lakebase_core::object_cleanup::ObjectCleanupError;
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

    #[error("Lakebase worker registration state is exhausted")]
    WorkerRegistrationStateExhausted,

    #[error(
        "an existing Lakebase worker must be stopped before its registration can be replaced"
    )]
    WorkerReplacementNotQuiescent,

    #[error("cannot PREPARE a transaction with pending Lakebase runtime actions")]
    PreparedTransactionWithRuntimeActions,

    #[error("timed out waiting for Lakebase workers to stop: {details}")]
    StopDatabaseTimeout { details: String },

    #[error("timed out waiting for extension workers to stop: {details}")]
    StopExtensionTimeout { details: String },

    #[error("timed out waiting for Lakebase worker to stop: {details}")]
    StopWorkerTimeout { details: String },

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

    #[error(
        "worker entry point must be a non-set-returning function with signature (internal) RETURNS bigint"
    )]
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

    #[error("failed to inspect pg_lakebase_runtime dependencies: {source}")]
    RuntimeDependencyInspection {
        #[source]
        source: PgError,
    },

    #[error("pg_lakebase_runtime dependency catalog objects are missing")]
    RuntimeDependencyCatalogMissing,

    #[error(
        "cannot drop pg_lakebase_runtime while dependent extension \"{extension_name}\" is installed"
    )]
    RuntimeHasDependentExtension { extension_name: String },

    #[error("failed to inspect maintenance queue before DROP EXTENSION: {source}")]
    MaintenanceQueueInspection {
        #[source]
        source: ObjectCleanupError,
    },

    #[error(
        "cannot drop pg_lakebase_runtime while the maintenance queue contains unresolved work"
    )]
    MaintenanceQueueNotEmpty,

    #[error("Lakebase worker context error: {source}")]
    WorkerContext {
        #[source]
        source: WorkerContextError,
    },

    #[error("failed to retry maintenance item: {source}")]
    RetryMaintenanceItem {
        #[source]
        source: ObjectCleanupError,
    },
}

impl LakebaseError {
    fn into_report(self) -> PgReportError {
        let sql_error_code = self.sql_error_code();
        match self {
            error @ Self::RuntimeHasDependentExtension { .. } => {
                PgReportError::from_parts(
                    sql_error_code,
                    error.to_string(),
                    Some(
                        "PostgreSQL has not removed any extension when this pre-drop check runs, so a dependent extension listed in the same DROP EXTENSION statement is still installed."
                            .to_owned(),
                    ),
                    Some(
                        "Drop the dependent extension in a separate DROP EXTENSION statement and commit it first (use CASCADE only if you intend to drop its dependent objects). Wait for lakebase.maintenance_status to contain no rows, then drop pg_lakebase_runtime."
                            .to_owned(),
                    ),
                )
            }
            error @ Self::MaintenanceQueueNotEmpty => PgReportError::from_parts(
                sql_error_code,
                error.to_string(),
                Some(
                    "Ready, retry-wait, and failed queue rows are unresolved external cleanup obligations. Dropping the runtime would remove the durable queue and its worker before those obligations are resolved."
                        .to_owned(),
                ),
                Some(
                    "Inspect lakebase.maintenance_status. Let ready or retry-wait items finish. For failed items, repair the underlying cause and call lakebase.retry_maintenance_item(item_id). Manually delete a row only if you accept that its external objects may remain."
                        .to_owned(),
                ),
            ),
            error => PgReportError::from_domain_error(error),
        }
    }

    pub(crate) fn report(self) -> ! {
        self.into_report().report()
    }
}

impl SqlStateError for LakebaseError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::RuntimeNotPreloaded
            | Self::WorkerRegistrationRequiresExtensionScript
            | Self::WorkerDeregistrationRequiresExtensionScript
            | Self::WorkerReplacementNotQuiescent
            | Self::EntryPointNotOwnedByExtension
            | Self::RegisteringExtensionMissing => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }

            Self::SharedMemoryLayoutMismatch
            | Self::RuntimeStateTransition { .. }
            | Self::WorkerContext { .. }
            | Self::RuntimeDependencyCatalogMissing => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            Self::MaxWorkerRegistrationsExhausted
            | Self::WorkerRegistrationStateExhausted => {
                PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
            }

            Self::PreparedTransactionWithRuntimeActions => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }

            Self::StopDatabaseTimeout { .. }
            | Self::StopExtensionTimeout { .. }
            | Self::StopWorkerTimeout { .. } => {
                PgSqlErrorCode::ERRCODE_QUERY_CANCELED
            }

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

            Self::WorkersTableMissing => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,

            Self::WorkerCatalog { source, .. }
            | Self::RuntimeDependencyInspection { source } => source.sql_error_code(),

            Self::WorkerEntrypointPreparation { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            Self::RuntimeHasDependentExtension { .. } => {
                PgSqlErrorCode::ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST
            }

            Self::MaintenanceQueueNotEmpty => PgSqlErrorCode::ERRCODE_OBJECT_IN_USE,

            Self::MaintenanceQueueInspection { source }
            | Self::RetryMaintenanceItem { source } => source.sql_error_code(),
        }
    }
}

impl From<LakebaseError> for ErrorReport {
    fn from(value: LakebaseError) -> Self {
        value.into_report().into_report()
    }
}

impl From<RuntimeStateTransitionError> for LakebaseError {
    fn from(source: RuntimeStateTransitionError) -> Self {
        Self::RuntimeStateTransition { source }
    }
}
