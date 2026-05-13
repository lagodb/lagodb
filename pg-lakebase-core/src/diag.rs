//! Helper functions for working with pg_lakebase_core
//!

use pgrx::{
    pg_sys::panic::{ErrorReport, ErrorReportable},
    *,
};
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};

/// Log debug message to Postgres log.
///
/// A helper function to emit `DEBUG1` level message to Postgres's log.
/// Set `log_min_messages = DEBUG1` in `postgresql.conf` to show the debug
/// messages.
///
/// See more details in [Postgres documents](https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-WHEN).
#[inline]
pub fn log_debug1(msg: &str) {
    debug1!("pg_lakebase_core: {}", msg);
}

/// Report info to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit info message.
///
/// For example,
///
/// ```rust,no_run
/// # use pg_lakebase_core::diag::report_info;
/// report_info(&format!("this is an info"));
/// ```
#[inline]
pub fn report_info(msg: &str) {
    ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg,
        "pg_lakebase_core"
    );
}

/// Report notice to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit notice message.
///
/// For example,
///
/// ```rust,no_run
/// # use pg_lakebase_core::diag::report_notice;
/// report_notice(&format!("this is a notice"));
/// ```
#[inline]
pub fn report_notice(msg: &str) {
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg,
        "pg_lakebase_core"
    );
}

/// Report warning to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit warning message.
///
/// For example,
///
/// ```rust,no_run
/// # use pg_lakebase_core::diag::report_warning;
/// report_warning(&format!("this is a warning"));
/// ```
#[inline]
pub fn report_warning(msg: &str) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        msg,
        "pg_lakebase_core"
    );
}

/// Report error to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit error message and
/// aborts the current transaction.
///
/// For example,
///
/// ```rust,no_run
/// # use pg_lakebase_core::diag::report_error;
/// use pgrx::prelude::PgSqlErrorCode;
///
/// report_error(
///     PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
///     &format!("target column number not match"),
/// );
/// ```
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: &str) {
    ereport!(PgLogLevel::ERROR, code, msg, "pg_lakebase_core");
}

/// Report panic to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit panic message and
/// cause the database server to crash. This is typically used for testing or
/// catastrophic failures.
#[inline]
pub fn report_panic(code: PgSqlErrorCode, msg: &str) {
    ereport!(PgLogLevel::PANIC, code, msg, "pg_lakebase_core");
}

#[derive(Error, Debug)]
pub enum CreateRuntimeError {
    #[error("failed to create async runtime: {0}")]
    FailedToCreateAsyncRuntime(#[from] std::io::Error),
}

impl From<CreateRuntimeError> for ErrorReport {
    fn from(value: CreateRuntimeError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, error_message, "")
    }
}

/// Error types that can choose the PostgreSQL SQLSTATE used when reporting.
pub trait SqlStateError: std::error::Error + Send + Sync + 'static {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    }
}

impl SqlStateError for CreateRuntimeError {}

/// Create a Tokio async runtime
///
/// Use this runtime to run async code in `block` mode. Run blocked code is
/// required by Postgres callback functions which is fine because Postgres
/// process is single-threaded.
///
/// For example,
///
/// ```rust,no_run
/// # use pg_lakebase_core::diag::{CreateRuntimeError, create_async_runtime};
/// # fn main() -> Result<(), CreateRuntimeError> {
/// # struct Client {
/// # }
/// # impl Client {
/// #     async fn query(&self, _sql: &str) -> Result<(), ()> { Ok(()) }
/// # }
/// # let client = Client {};
/// # let sql = "";
/// let rt = create_async_runtime()?;
///
/// // client.query() is an async function returning a Result
/// match rt.block_on(client.query(&sql)) {
///     Ok(result) => { }
///     Err(err) => { }
/// }
/// # Ok(())
/// # }
/// ```
#[inline]
pub fn create_async_runtime() -> Result<Runtime, CreateRuntimeError> {
    Ok(Builder::new_current_thread().enable_all().build()?)
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
