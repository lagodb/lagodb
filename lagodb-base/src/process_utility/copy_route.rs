//! Fail-closed boundary between provider-owned URI COPY and local file COPY.

use std::ffi::CStr;

use lagodb_core::copy::CopyEndpoint;
use lagodb_core::diag::PgReportError;
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
    if CopyEndpoint::from_filename(Some(filename), statement.is_program)
        != CopyEndpoint::ExternalUri
    {
        return None;
    }
    Some(PgReportError::from_parts(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        "no LagoDB provider claimed the COPY URI",
        None,
        Some(
            "configure the required provider in lagodb.provider_libraries and restart PostgreSQL"
                .to_owned(),
        ),
    ))
}
