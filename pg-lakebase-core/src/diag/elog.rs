use pgrx::*;

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
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: &str) {
    ereport!(PgLogLevel::ERROR, code, msg, "pg_lakebase_core");
}

/// Report panic to Postgres using `ereport!`
#[inline]
pub fn report_panic(code: PgSqlErrorCode, msg: &str) {
    ereport!(PgLogLevel::PANIC, code, msg, "pg_lakebase_core");
}
