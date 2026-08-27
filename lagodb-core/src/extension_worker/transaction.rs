use std::panic::AssertUnwindSafe;

use pgrx::PgTryBuilder;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;

use crate::diag::PgReportError;

/// One recoverable PostgreSQL transaction executed by an extension worker.
///
/// `BackgroundWorker` is only pgrx's namespace for its worker transaction
/// helper; PostgreSQL owns the commit lifecycle. Unlike that helper, this
/// wrapper treats a Rust `Err` as a transaction abort and also catches a
/// PostgreSQL `ERROR` raised when `CommitTransactionCommand` invokes
/// pre-commit callbacks. Rust panics are never converted into per-relation
/// failures: the transaction is aborted and the panic is resumed so the
/// runtime can restart the worker.
pub struct WorkerTransaction;

impl WorkerTransaction {
    pub fn run<T, E, F>(body: F) -> Result<T, PgReportError>
    where
        E: Into<PgReportError>,
        F: FnOnce() -> Result<T, E>,
    {
        unsafe {
            assert!(
                !pg_sys::MyBgworkerEntry.is_null(),
                "WorkerTransaction can only run in a registered background worker"
            );
        }
        PgTryBuilder::new(AssertUnwindSafe(move || {
            unsafe {
                pg_sys::SetCurrentStatementStartTimestamp();
                pg_sys::StartTransactionCommand();
                pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
            }
            match body() {
                Ok(value) => {
                    unsafe {
                        pg_sys::PopActiveSnapshot();
                        // This may invoke extension pre-commit callbacks. A
                        // PostgreSQL ERROR is caught below and aborts the
                        // entire transaction before the worker continues.
                        pg_sys::CommitTransactionCommand();
                    }
                    Ok(value)
                }
                Err(error) => {
                    unsafe { pg_sys::AbortCurrentTransaction() };
                    Err(error.into())
                }
            }
        }))
        .catch_others(|caught| match caught {
            caught @ CaughtError::RustPanic { .. } => {
                unsafe { pg_sys::AbortCurrentTransaction() };
                caught.rethrow()
            }
            CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => {
                let error = PgReportError::from_parts(
                    report.sql_error_code(),
                    report.message(),
                    report.detail().map(str::to_owned),
                    report.hint().map(str::to_owned),
                );
                unsafe { pg_sys::AbortCurrentTransaction() };
                Err(error)
            }
        })
        .execute()
    }
}
