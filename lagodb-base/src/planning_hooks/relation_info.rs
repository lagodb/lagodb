//! Previous-first catalog information routing, before PostgreSQL baserel sizing.

use lagodb_core::diag::ReportableError;
use lagodb_core::runtime_api::CallbackErrorReport;
use pgrx::{pg_guard, pg_sys};

use super::{PREV_GET_RELATION_INFO, callback_result, registry};

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn get_relation_info(
    root: *mut pg_sys::PlannerInfo,
    relation_oid: pg_sys::Oid,
    inhparent: bool,
    rel: *mut pg_sys::RelOptInfo,
) {
    if let Some(Some(previous)) = PREV_GET_RELATION_INFO.get() {
        // SAFETY: these are the live arguments supplied by PostgreSQL's hook.
        unsafe { previous(root, relation_oid, inhparent, rel) };
    }
    registry::relation_scan_snapshot()
        .try_for_each(|descriptor| {
            let mut error = CallbackErrorReport::default();
            // SAFETY: registration validated this exact-build callback. The
            // partially initialized RelOptInfo and error record are borrowed
            // only for this call; no path or restriction estimates exist yet.
            let status = unsafe {
                (descriptor.relation_info)(
                    descriptor.context,
                    root,
                    relation_oid,
                    inhparent,
                    rel,
                    &mut error,
                )
            };
            callback_result(status, &error, "relation information callback")
        })
        .report_unwrap();
}
