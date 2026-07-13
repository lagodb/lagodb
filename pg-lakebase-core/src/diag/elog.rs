use std::fmt;

use pgrx::*;

const COMPONENT: &str = "pg-lakebase";

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

/// Log informational message to the Postgres server log using `ereport!`.
///
/// Unlike `INFO`, `LOG` preserves the old server-log-only behavior for
/// operational messages that should not pollute SQL regression output.
#[inline]
pub fn log_info(msg: impl fmt::Display) {
    ereport!(
        PgLogLevel::LOG,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg.to_string(),
        COMPONENT
    );
}

/// Report info to Postgres using `ereport!`
#[inline]
pub fn report_info(msg: impl fmt::Display) {
    ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg.to_string(),
        COMPONENT
    );
}

/// Report notice to Postgres using `ereport!`
#[inline]
pub fn report_notice(msg: impl fmt::Display) {
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg.to_string(),
        COMPONENT
    );
}

/// Report warning to Postgres using `ereport!`
#[inline]
pub fn report_warning(msg: impl fmt::Display) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        msg.to_string(),
        COMPONENT
    );
}

/// Report error to Postgres using `ereport!`
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: impl fmt::Display) {
    ereport!(PgLogLevel::ERROR, code, msg.to_string(), COMPONENT);
}

/// Report panic to Postgres using `ereport!`
#[inline]
pub fn report_panic(code: PgSqlErrorCode, msg: impl fmt::Display) {
    ereport!(PgLogLevel::PANIC, code, msg.to_string(), COMPONENT);
}
