//! DataFusion query-domain errors and PostgreSQL error-boundary conversion.

use std::error::Error;
use std::io;

use datafusion::common::DataFusionError;
use lagodb_core::customscan::custom_exprs::PgExpressionSectionsError;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::RuntimeValueStateError;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("query execution limits must all be non-zero")]
    InvalidLimits,
    #[error("failed to create current-thread query runtime: {0}")]
    Runtime(#[source] io::Error),
    #[error("DataFusion query execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("table scan binding failed: {0}")]
    ScanBind(#[source] PgReportError),
    #[error("failed to initialize query runtime values: {0}")]
    RuntimeValues(#[source] RuntimeValueStateError),
    #[error("invalid PostgreSQL expression sections: {0}")]
    ExpressionSections(#[source] PgExpressionSectionsError),
    #[error("selected query plan has {scans} scans but {callbacks} callback tables")]
    ScanCallbackCount { scans: usize, callbacks: usize },
    #[error("DataFusion query output has {columns} columns and {rows} rows")]
    InvalidQueryOutput { columns: usize, rows: usize },
    #[error("failed to convert the DataFusion result batch: {0}")]
    OutputConversion(#[from] PgReportError),
    #[error("bound table scan {scan} remained shared while closing execution")]
    BoundScanStillShared { scan: usize },
    #[error("query fragment is missing metadata for table scan {scan}")]
    MissingScanMetadata { scan: usize },
    #[error("table scan release failed: {0}")]
    ScanRelease(#[source] PgReportError),
    #[error("parallel query host failed: {0}")]
    ParallelHost(#[source] PgReportError),
    #[error("query initialization failed: {primary}; cleanup failure: {cleanup:?}")]
    Initialization {
        #[source]
        primary: Box<QueryExecutionError>,
        cleanup: Option<Box<QueryExecutionError>>,
    },
}

impl SqlStateError for QueryExecutionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::DataFusion(error) => Self::datafusion_sqlstate(error),
            Self::ScanBind(error)
            | Self::ScanRelease(error)
            | Self::ParallelHost(error)
            | Self::OutputConversion(error) => error.sql_error_code(),
            Self::Initialization { primary, .. } => primary.sql_error_code(),
            Self::InvalidQueryOutput { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            Self::InvalidLimits
            | Self::Runtime(_)
            | Self::RuntimeValues(_)
            | Self::ExpressionSections(_)
            | Self::ScanCallbackCount { .. }
            | Self::MissingScanMetadata { .. }
            | Self::BoundScanStillShared { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl QueryExecutionError {
    /// Convert at the query-offload boundary while preserving a provider's
    /// SQLSTATE, DETAIL, and HINT.
    pub fn into_report(self) -> PgReportError {
        match self {
            Self::DataFusion(error) => Self::datafusion_report(error),
            Self::ScanBind(error)
            | Self::OutputConversion(error)
            | Self::ParallelHost(error)
            | Self::ScanRelease(error) => error,
            Self::Initialization { primary, cleanup } => {
                let cleanup = cleanup.map(|error| {
                    format!("query initialization cleanup failed: {error}")
                });
                (*primary)
                    .into_report()
                    .contextualize("query initialization failed", cleanup)
            }
            error => PgReportError::from_domain_error(error),
        }
    }

    fn datafusion_sqlstate(error: &DataFusionError) -> PgSqlErrorCode {
        if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
            return PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY;
        }
        Self::provider_error(error)
            .map_or(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, |error| {
                error.sql_error_code()
            })
    }

    fn datafusion_report(error: DataFusionError) -> PgReportError {
        if let Some(provider) = Self::provider_error(&error) {
            return PgReportError::from_parts(
                provider.sql_error_code(),
                provider.message(),
                provider.detail().map(str::to_owned),
                provider.hint().map(str::to_owned),
            );
        }
        PgReportError::from_domain_error(Self::DataFusion(error))
    }

    fn provider_error(error: &DataFusionError) -> Option<&PgReportError> {
        let mut current: Option<&(dyn Error + 'static)> = Some(error);
        while let Some(error) = current {
            if let Some(provider) = error.downcast_ref::<PgReportError>() {
                return Some(provider);
            }
            current = error.source();
        }
        None
    }
}
