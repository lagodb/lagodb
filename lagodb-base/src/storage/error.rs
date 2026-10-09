//! Worker failures, shutdown policy and PostgreSQL report conversion.

use std::io;

use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;
use tokio::task::JoinError;

use lagodb_core::diag::{PgReportError, SqlStateError, error_source_chain_detail};
use lagodb_storage::StorageError;

use super::reconciler::ReconcileError;
use super::reload::ReconcilePhase;
use super::volume_config::StorageVolumeError;

#[derive(Debug, Error)]
pub(super) enum ServerTaskError {
    #[error("storage server startup failed")]
    Startup(#[source] StorageError),
    #[error("storage server failed")]
    Serve(#[source] StorageError),
}

#[derive(Debug, Error)]
pub(super) enum StorageWorkerError {
    #[error(transparent)]
    Postgres(PgReportError),
    #[error("storage volume config initialization failed")]
    ConfigInitialization(#[source] StorageVolumeError),
    #[error("storage runtime config invalid, worker cannot start")]
    RuntimeConfig(#[source] StorageError),
    #[error("failed to create lagodb-storage tokio runtime")]
    RuntimeBuild(#[source] io::Error),
    #[error("storage worker {phase} reconcile failed")]
    Reconcile {
        phase: ReconcilePhase,
        #[source]
        source: ReconcileError,
    },
    #[error("{task} task failed")]
    Task {
        task: &'static str,
        #[source]
        source: JoinError,
    },
    #[error(transparent)]
    Server(#[from] ServerTaskError),
    #[error("storage server exited unexpectedly")]
    ServerExited,
}

impl StorageWorkerError {
    /// Convert after worker-owned resources have been shut down. Captured PG
    /// errors already own their SQLSTATE, DETAIL and HINT and pass through intact.
    pub(super) fn into_report(self) -> PgReportError {
        match self {
            Self::Postgres(error) => error,
            error => PgReportError::from_domain_error(error),
        }
    }

    /// Snapshot failures during ordinary reload keep the current registry.
    /// Startup failures and preparation-task panics require worker shutdown.
    pub(super) fn is_fatal(&self) -> bool {
        !matches!(
            self,
            Self::Reconcile { phase, source }
                if *phase != ReconcilePhase::Startup
                    && !matches!(source, ReconcileError::PrepareTask(_))
        )
    }

    pub(super) fn is_cancelled(&self) -> bool {
        matches!(self, Self::Task { source, .. } if source.is_cancelled())
    }

    pub(super) fn diagnostic_message(&self) -> String {
        match error_source_chain_detail(self) {
            Some(detail) => format!("{self}\n{detail}"),
            None => self.to_string(),
        }
    }
}

impl SqlStateError for StorageWorkerError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Postgres(error) => error.sql_error_code(),
            Self::ConfigInitialization(error) => error.sql_error_code(),
            Self::RuntimeConfig(error)
            | Self::Server(ServerTaskError::Startup(error))
            | Self::Server(ServerTaskError::Serve(error)) => error.sql_error_code(),
            Self::RuntimeBuild(_) => PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
            Self::Reconcile {
                source: ReconcileError::Source(error),
                ..
            } => error.sql_error_code(),
            Self::Reconcile {
                source: ReconcileError::DuplicateVolumeId(_),
                ..
            } => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
            Self::Reconcile {
                source: ReconcileError::PrepareTask(_),
                ..
            }
            | Self::Task { .. }
            | Self::ServerExited => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
