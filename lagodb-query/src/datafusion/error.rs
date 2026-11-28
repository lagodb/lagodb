//! DataFusion query-domain errors and PostgreSQL error-boundary conversion.

use std::error::Error;
use std::io;

use arrow_schema::ArrowError;
use datafusion::common::DataFusionError;
use lagodb_arrow::ArrowConversionError;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::RuntimeValueStateError;
use pgrx::prelude::PgSqlErrorCode;

/// Hash tables and a few other operators cannot spill, so they can still fail
/// when PostgreSQL's estimate led to a plan that needs more memory.
const MEMORY_BUDGET_HINT: &str =
    "Increase work_mem or hash_mem_multiplier for this query.";

/// The message DataFusion's native decimal AVG raises when an average leaves
/// its result type (`DecimalAverager::avg`).
const NATIVE_AVERAGE_OVERFLOW: &str = "Arithmetic Overflow in AvgAccumulator";

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("query execution limits must all be non-zero")]
    InvalidLimits,
    #[error("failed to create current-thread query runtime: {0}")]
    Runtime(#[source] io::Error),
    #[error("failed to register a parallel transport descriptor: {0}")]
    Transport(#[source] io::Error),
    #[error("DataFusion query execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("table scan binding failed: {0}")]
    ScanBind(#[source] PgReportError),
    #[error("table scan task planning failed: {0}")]
    ScanPlan(#[source] PgReportError),
    #[error("failed to initialize query runtime values: {0}")]
    RuntimeValues(#[source] RuntimeValueStateError),
    #[error("selected query plan has {scans} scans but {callbacks} callback tables")]
    ScanCallbackCount { scans: usize, callbacks: usize },
    #[error("DataFusion query output has {columns} columns and {rows} rows")]
    InvalidQueryOutput { columns: usize, rows: usize },
    #[error("failed to convert the DataFusion result batch: {0}")]
    OutputConversion(#[from] ArrowConversionError),
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
            | Self::ScanPlan(error)
            | Self::ScanRelease(error)
            | Self::ParallelHost(error) => error.sql_error_code(),
            Self::OutputConversion(error) => error.sql_error_code(),
            Self::Initialization { primary, .. } => primary.sql_error_code(),
            Self::InvalidQueryOutput { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            Self::InvalidLimits
            | Self::Runtime(_)
            | Self::Transport(_)
            | Self::RuntimeValues(_)
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
            | Self::ScanPlan(error)
            | Self::ParallelHost(error)
            | Self::ScanRelease(error) => error,
            Self::OutputConversion(error) => error.into(),
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

    /// A cancel or termination that a parallel participant must defer: the
    /// error ends the stream, and the guard's exit services the interrupt.
    pub(in crate::datafusion) fn interrupted() -> DataFusionError {
        DataFusionError::Execution("parallel query interrupted".to_owned())
    }

    fn datafusion_sqlstate(error: &DataFusionError) -> PgSqlErrorCode {
        if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
            return PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY;
        }
        if let Some(provider) = Self::provider_error(error) {
            return provider.sql_error_code();
        }
        Self::native_numeric_error(error)
            .map_or(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, |error| {
                error.sql_error_code()
            })
    }

    /// PostgreSQL's error for a zero divisor (`numeric_div`, `int4div`).
    pub(in crate::datafusion) fn division_by_zero() -> PgReportError {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_DIVISION_BY_ZERO,
            "division by zero",
        )
    }

    /// PostgreSQL's error for a NUMERIC result it cannot represent
    /// (`numeric.c` `make_result_opt_error`). The engine raises it where a
    /// fixed decimal value leaves its Arrow type.
    pub(in crate::datafusion) fn numeric_overflow() -> PgReportError {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            "value overflows numeric format",
        )
    }

    /// PostgreSQL's error for one raised by a native DataFusion operator over
    /// fixed decimals: Arrow's checked decimal arithmetic, casts and division,
    /// and native AVG, whose overflow is only an execution message
    /// (`datafusion-functions-aggregate-common` `DecimalAverager`, pinned by
    /// the wide `avg(numeric)` regress case). The mapping by error kind holds
    /// because no other native operator that raises these is pushed down:
    /// integer arithmetic and `abs` report their own errors, and DataFusion's
    /// unary minus wraps (`neg_wrapping`).
    fn native_numeric_error(error: &DataFusionError) -> Option<PgReportError> {
        match error.find_root() {
            DataFusionError::ArrowError(error, _) => match **error {
                ArrowError::ArithmeticOverflow(_) => Some(Self::numeric_overflow()),
                ArrowError::DivideByZero => Some(Self::division_by_zero()),
                _ => None,
            },
            DataFusionError::Execution(message)
                if message.starts_with(NATIVE_AVERAGE_OVERFLOW) =>
            {
                Some(Self::numeric_overflow())
            }
            _ => None,
        }
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
        if let Some(numeric) = Self::native_numeric_error(&error) {
            return numeric;
        }
        let exhausted =
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_));
        let report = PgReportError::from_domain_error(Self::DataFusion(error));
        if !exhausted {
            return report;
        }
        PgReportError::from_parts(
            report.sql_error_code(),
            report.message(),
            report.detail().map(str::to_owned),
            Some(MEMORY_BUDGET_HINT.to_owned()),
        )
    }

    fn provider_error(error: &DataFusionError) -> Option<&PgReportError> {
        let mut current: Option<&(dyn Error + 'static)> = Some(error);
        while let Some(error) = current {
            if let Some(provider) = error.downcast_ref::<PgReportError>() {
                return Some(provider);
            }
            // `io::Error::source` skips the error it wraps. Spill writes reach
            // Arrow through `io::Write`, so their cause sits one level inside.
            let wrapped = error
                .downcast_ref::<io::Error>()
                .and_then(io::Error::get_ref);
            current = match wrapped {
                Some(wrapped) => Some(wrapped as &(dyn Error + 'static)),
                None => error.source(),
            };
        }
        None
    }
}
