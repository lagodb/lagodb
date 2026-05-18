use pgrx::{
    pg_sys::panic::{
        CaughtError, ErrorReport, ErrorReportWithLevel, ErrorReportable,
    },
    prelude::*,
};
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Small owning handle for a PostgreSQL [`ErrorReport`].
///
/// `ErrorReport` carries the full PostgreSQL diagnostic payload and is too large
/// to use directly as the `Err` variant of public callback results. Keeping it
/// boxed preserves the original SQLSTATE/detail/hint/location while keeping
/// success-path `Result` values compact.
#[derive(Debug)]
pub struct PgReportError(Box<ErrorReport>);

impl PgReportError {
    #[inline]
    pub fn new(report: ErrorReport) -> Self {
        Self(Box::new(report))
    }

    #[inline]
    pub fn into_report(self) -> ErrorReport {
        *self.0
    }

    #[inline]
    pub fn report(self) -> ! {
        self.into_report().report(PgLogLevel::ERROR);
        unreachable!()
    }
}

impl<E> From<E> for PgReportError
where
    E: Into<ErrorReport>,
{
    #[inline]
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl Display for PgReportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for PgReportError {}

/// Error types that can choose the PostgreSQL SQLSTATE used when reporting.
pub trait SqlStateError: std::error::Error + Send + Sync + 'static {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    }
}

#[derive(Debug)]
pub enum PgErrorSource {
    Postgres,
    ErrorReport,
    RustPanic,
}

impl Display for PgErrorSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres => f.write_str("postgres"),
            Self::ErrorReport => f.write_str("error_report"),
            Self::RustPanic => f.write_str("rust_panic"),
        }
    }
}

/// Owned PostgreSQL error data captured at the FFI boundary.
#[derive(Debug)]
pub struct PgErrorReport {
    source: PgErrorSource,
    level: PgLogLevel,
    sql_error_code: PgSqlErrorCode,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    file: String,
    line: u32,
    function_name: Option<String>,
}

impl PgErrorReport {
    pub(crate) fn from_caught(err: CaughtError) -> Self {
        match err {
            CaughtError::PostgresError(report) => {
                Self::from_error_report(PgErrorSource::Postgres, report)
            }
            CaughtError::ErrorReport(report) => {
                Self::from_error_report(PgErrorSource::ErrorReport, report)
            }
            CaughtError::RustPanic { ereport, .. } => {
                Self::from_error_report(PgErrorSource::RustPanic, ereport)
            }
        }
    }

    fn from_error_report(
        source: PgErrorSource,
        report: ErrorReportWithLevel,
    ) -> Self {
        Self {
            source,
            level: report.level(),
            sql_error_code: report.sql_error_code(),
            message: report.message().to_string(),
            detail: report.detail().map(str::to_string),
            hint: report.hint().map(str::to_string),
            file: report.file().to_string(),
            line: report.line_number(),
            function_name: report.function_name().map(str::to_string),
        }
    }

    #[inline]
    pub fn sql_error_code(&self) -> PgSqlErrorCode {
        self.sql_error_code
    }

    #[inline]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[inline]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    #[inline]
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    #[inline]
    pub(crate) fn is_tuple_concurrency_conflict(&self) -> bool {
        // PostgreSQL's `simple_heap_update` reports TM_Updated/TM_Deleted from
        // the `CatalogTupleUpdate` path as generic ERRORs without a dedicated
        // SQLSTATE. While we keep the structured error report, the message is
        // still the only discriminator available unless we replace this path
        // with a lower-level catalog update implementation that preserves
        // constraints and index maintenance.
        matches!(
            self.message.as_str(),
            "tuple concurrently updated" | "tuple concurrently deleted"
        )
    }
}

impl Display for PgErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?} {}: {}",
            self.source, self.level, self.sql_error_code, self.message
        )?;

        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {hint}")?;
        }

        write!(f, "\nLOCATION: {}", self.file)?;
        if self.line != 0 {
            write!(f, ":{}", self.line)?;
        }
        if let Some(function_name) = &self.function_name {
            write!(f, " {function_name}")?;
        }

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum PgError {
    #[error("Invalid string (contains null byte): {0}")]
    NulError(#[from] std::ffi::NulError),

    #[error("Postgres error: {0}")]
    PostgresError(Box<PgErrorReport>),
}

impl PgError {
    #[inline]
    pub(crate) fn from_caught(err: CaughtError) -> Self {
        Self::PostgresError(Box::new(PgErrorReport::from_caught(err)))
    }

    #[inline]
    pub(crate) fn is_tuple_concurrency_conflict(&self) -> bool {
        match self {
            Self::PostgresError(report) => report.is_tuple_concurrency_conflict(),
            Self::NulError(_) => false,
        }
    }
}

impl SqlStateError for PgError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::PostgresError(report) => report.sql_error_code(),
            Self::NulError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

pub trait ReportableError {
    type Output;

    fn report_unwrap(self) -> Self::Output;
}

impl<T, E: Into<ErrorReport>> ReportableError for Result<T, E> {
    type Output = T;

    fn report_unwrap(self) -> Self::Output {
        self.map_err(|e| e.into()).unwrap_or_report()
    }
}

impl<T> ReportableError for Result<T, PgReportError> {
    type Output = T;

    fn report_unwrap(self) -> Self::Output {
        match self {
            Ok(value) => value,
            Err(error) => error.report(),
        }
    }
}
