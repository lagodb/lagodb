use std::fmt;
use std::panic::AssertUnwindSafe;
use std::thread;

use pgrx::{PgSqlErrorCode, PgTryBuilder, debug1, ereport};

const COMPONENT: &str = "lagodb";

/// Log debug message to Postgres log.
///
/// A helper function to emit `DEBUG1` level message to Postgres's log.
/// Set `log_min_messages = DEBUG1` in `postgresql.conf` to show the debug
/// messages.
///
/// See more details in [Postgres documents](https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-WHEN).
#[inline]
pub fn log_debug1(msg: impl fmt::Display) {
    debug1!("{COMPONENT}: {msg}");
}

/// Log informational message to the Postgres server log.
///
/// Unlike `INFO`, `LOG` preserves the old server-log-only behavior for
/// operational messages that should not pollute SQL regression output.
#[inline]
pub fn log_info(msg: impl fmt::Display) {
    ereport!(
        LOG,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format_args!("{msg}"),
        COMPONENT
    );
}

/// Report info through PostgreSQL's diagnostics system.
#[inline]
pub fn report_info(msg: impl fmt::Display) {
    ereport!(
        INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format_args!("{msg}"),
        COMPONENT
    );
}

/// Report notice through PostgreSQL's diagnostics system.
#[inline]
pub fn report_notice(msg: impl fmt::Display) {
    ereport!(
        NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format_args!("{msg}"),
        COMPONENT
    );
}

/// Report warning through PostgreSQL's diagnostics system.
///
/// While unwinding (for example from a `Drop` cleanup path), an error raised
/// by a PostgreSQL error-context callback during `ereport` is caught so it
/// cannot abort the backend via a double panic.
#[inline]
pub fn report_warning(msg: impl fmt::Display) {
    if thread::panicking() {
        PgTryBuilder::new(AssertUnwindSafe(|| {
            ereport!(
                WARNING,
                PgSqlErrorCode::ERRCODE_WARNING,
                format_args!("{msg}"),
                COMPONENT
            );
        }))
        .catch_others(|_| ())
        .execute();
    } else {
        ereport!(
            WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            format_args!("{msg}"),
            COMPONENT
        );
    }
}

/// Report an error through PostgreSQL's diagnostics system.
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: impl fmt::Display) {
    ereport!(ERROR, code, format_args!("{msg}"), COMPONENT);
}

/// Report a panic through PostgreSQL's diagnostics system.
#[inline]
pub fn report_panic(code: PgSqlErrorCode, msg: impl fmt::Display) {
    ereport!(PANIC, code, format_args!("{msg}"), COMPONENT);
}
