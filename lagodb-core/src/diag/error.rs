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
/// `ErrorReport` is too large to use directly as the `Err` variant of public
/// callback results. This wrapper boxes the report and keeps a single outer
/// [`PgSqlErrorCode`] in sync with [`Self::into_report`] / [`Self::report`].
///
/// The boxed report retains message, DETAIL, and HINT. File/line location from
/// the original construction is not preserved across [`Self::into_report`];
/// reporting uses a fresh caller location from the rebuild.
#[derive(Debug)]
pub struct PgReportError {
    sqlerrcode: PgSqlErrorCode,
    report: Box<ErrorReport>,
}

impl PgReportError {
    /// Preserve an error caught by a provider-owned `PgTryBuilder` so it can
    /// travel through the provider Result path to the single outer report.
    pub fn from_caught(err: CaughtError) -> Self {
        Self::from_pg_error(PgError::from_caught(err))
    }

    /// Build from a domain error with a single SQLSTATE and full `source` chain in DETAIL.
    #[inline]
    pub fn from_domain_error<E>(err: E) -> Self
    where
        E: SqlStateError + std::error::Error + std::fmt::Display,
    {
        let sqlerrcode = err.sql_error_code();
        Self {
            sqlerrcode,
            report: Box::new(domain_error_report(err)),
        }
    }

    /// Build from a primary message only (no DETAIL).
    #[inline]
    pub fn from_message(
        sqlerrcode: PgSqlErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self::from_parts(sqlerrcode, message, None, None)
    }

    /// Build with optional PostgreSQL DETAIL/HINT lines.
    #[inline]
    pub fn from_parts(
        sqlerrcode: PgSqlErrorCode,
        message: impl Into<String>,
        detail: Option<String>,
        hint: Option<String>,
    ) -> Self {
        let mut report = ErrorReport::new(sqlerrcode, message.into(), "");
        if let Some(detail) = detail {
            report = report.set_detail(detail);
        }
        if let Some(hint) = hint {
            report = report.set_hint(hint);
        }
        Self {
            sqlerrcode,
            report: Box::new(report),
        }
    }

    #[inline]
    pub fn sql_error_code(&self) -> PgSqlErrorCode {
        self.sqlerrcode
    }

    /// Rebuild the report using the outer SQLSTATE so reporting and hook conversion stay consistent.
    #[inline]
    pub fn into_report(self) -> ErrorReport {
        let Self { sqlerrcode, report } = self;
        let mut out = ErrorReport::new(sqlerrcode, report.message().to_string(), "");
        if let Some(detail) = report.detail() {
            out = out.set_detail(detail.to_string());
        }
        if let Some(hint) = report.hint() {
            out = out.set_hint(hint.to_string());
        }
        out
    }

    #[inline]
    pub fn report(self) -> ! {
        Self::raise(self.into_report())
    }

    /// Raise an already-structured report at a core-owned PostgreSQL boundary.
    #[inline]
    pub(crate) fn raise(report: ErrorReport) -> ! {
        report.report(PgLogLevel::ERROR);
        // pgrx guarantees that ERROR reporting does not return, but its
        // `ErrorReport::report` signature is `()` rather than `!`.
        unreachable!()
    }

    /// Preserve a PostgreSQL error's structured SQLSTATE, DETAIL, and HINT.
    ///
    /// This is deliberately a named conversion rather than `From<PgError>`:
    /// the blanket domain-error conversion below also applies to `PgError`,
    /// and Rust does not permit overlapping `From` implementations.
    pub fn from_pg_error(error: PgError) -> Self {
        match error {
            PgError::PostgresError(report) => Self::from_parts(
                report.sql_error_code(),
                report.message().to_owned(),
                report.detail().map(str::to_owned),
                report.hint().map(str::to_owned),
            ),
            PgError::NulError(error) => Self::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                error.to_string(),
            ),
        }
    }
}

impl<E> From<E> for PgReportError
where
    E: SqlStateError + std::error::Error + std::fmt::Display,
{
    #[inline]
    fn from(value: E) -> Self {
        Self::from_domain_error(value)
    }
}

impl Display for PgReportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.report.as_ref(), f)
    }
}

impl std::error::Error for PgReportError {}

/// Format the full `std::error::Error::source` chain (excluding the top-level error).
pub fn error_source_chain_detail(err: &dyn std::error::Error) -> Option<String> {
    let mut current = err.source();
    let mut lines = Vec::new();
    while let Some(source) = current {
        lines.push(format!("{source}"));
        current = source.source();
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

/// Join optional DETAIL fragments with newlines, skipping `None` and empty strings.
pub fn join_error_details(
    parts: impl IntoIterator<Item = Option<String>>,
) -> Option<String> {
    let lines: Vec<String> = parts
        .into_iter()
        .flatten()
        .filter(|line| !line.is_empty())
        .collect();
    (!lines.is_empty()).then(|| lines.join("\n"))
}

/// Build a PostgreSQL [`ErrorReport`] from a domain error and its `source` chain.
pub fn domain_error_report<E>(err: E) -> ErrorReport
where
    E: SqlStateError + std::error::Error + std::fmt::Display,
{
    let mut report = ErrorReport::new(err.sql_error_code(), format!("{err}"), "");
    if let Some(detail) = error_source_chain_detail(&err) {
        report = report.set_detail(detail);
    }
    report
}

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

/// Convert a PostgreSQL error captured by `PgTryBuilder` into the structured
/// domain error used below PostgreSQL callback boundaries.
///
/// The report constructor remains private; dependent crates only need this
/// stable diagnostic bridge and must not construct or reclassify reports.
impl From<CaughtError> for PgError {
    #[inline]
    fn from(err: CaughtError) -> Self {
        Self::from_caught(err)
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
