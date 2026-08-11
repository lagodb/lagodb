//! Fail-closed boundary between provider-owned URI COPY and local file COPY.

use std::ffi::CStr;

use pg_lakebase_core::diag::PgReportError;
use pgrx::{PgSqlErrorCode, pg_sys};

/// Build the fail-closed error for a URI-form COPY filename after every
/// registered provider declined it.
///
/// # Safety
///
/// `node` must be the live `T_CopyStmt` node for the current ProcessUtility
/// invocation.
pub(super) unsafe fn unclaimed_uri_error(
    node: *mut pg_sys::Node,
) -> Option<PgReportError> {
    // SAFETY: the caller guarantees the node tag and lifetime.
    let statement = unsafe { &*node.cast::<pg_sys::CopyStmt>() };
    if statement.is_program || statement.filename.is_null() {
        return None;
    }
    // SAFETY: PostgreSQL COPY parse nodes store `filename` as a live
    // NUL-terminated string for the utility statement lifetime.
    let filename = unsafe { CStr::from_ptr(statement.filename) };
    if !has_uri_scheme(filename.to_bytes()) {
        return None;
    }
    Some(PgReportError::from_parts(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        "no Lakebase provider claimed the COPY URI",
        None,
        Some(
            "configure the required provider in pg_lakebase.provider_libraries and restart PostgreSQL"
                .to_owned(),
        ),
    ))
}

fn has_uri_scheme(filename: &[u8]) -> bool {
    let Some(separator) = filename.windows(3).position(|window| window == b"://")
    else {
        return false;
    };
    let scheme = &filename[..separator];
    scheme.first().is_some_and(u8::is_ascii_alphabetic)
        && scheme.iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'-' | b'.')
        })
}
