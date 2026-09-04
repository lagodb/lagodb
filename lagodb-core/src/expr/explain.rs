//! Shared PostgreSQL expression rendering for scan-filter diagnostics.

use std::ffi::{CStr, CString};

use pgrx::pg_sys;

/// Shared EXPLAIN vocabulary for the exact executor predicate.
pub const FILTER: &CStr = c"Filter";
/// Shared EXPLAIN vocabulary for a pushed predicate in non-verbose output.
pub const PUSHED_FILTER: &CStr = c"Pushed Filter";
/// Shared EXPLAIN vocabulary for an exact pushed predicate.
pub(crate) const PUSHED_FILTER_EXACT: &CStr = c"Pushed Filter Exact";
/// Shared EXPLAIN vocabulary for a conservative pruning predicate.
pub const PUSHED_FILTER_CONSERVATIVE: &CStr = c"Pushed Filter Conservative";
/// Shared EXPLAIN vocabulary for PostgreSQL executor recheck.
pub(crate) const RECHECK: &CStr = c"Recheck";

/// Deparse independently owned predicate clauses and render their implicit
/// conjunction exactly as PostgreSQL scan EXPLAIN does.
///
/// # Safety
///
/// Every expression and `deparse_context` must remain live for the duration of
/// the call. A null context is valid only when none of the expressions needs a
/// namespace.
pub unsafe fn deparse_and_join<I>(
    deparse_context: *mut pg_sys::List,
    expressions: I,
) -> Option<CString>
where
    I: IntoIterator<Item = *mut pg_sys::Expr>,
{
    let mut parts = Vec::new();
    for expression in expressions {
        if expression.is_null() {
            continue;
        }
        let text = unsafe {
            pg_sys::deparse_expression(
                expression.cast(),
                deparse_context,
                false,
                false,
            )
        };
        if text.is_null() {
            continue;
        }
        parts.push(
            unsafe { CStr::from_ptr(text) }
                .to_string_lossy()
                .into_owned(),
        );
    }
    (!parts.is_empty()).then(|| {
        CString::new(parts.join(" AND "))
            .expect("deparsed predicate text contains no interior NUL")
    })
}
