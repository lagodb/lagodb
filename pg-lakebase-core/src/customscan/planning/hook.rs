//! Planning entry point for `set_rel_pathlist_hook`. Rejections fall through to PG default paths.

use std::sync::OnceLock;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::gucs;
use crate::customscan::paths::CustomScanPathPlanner;
use crate::customscan::provider::find_matching_provider;
use crate::diag::ReportableError;

pub use crate::customscan::candidate::{CustomScanCandidate, CustomScanRejection};
pub use crate::customscan::paths::join_parameterized_variant_pushes_nothing;

/// Previous hook captured at install; `None` if none was installed.
static PREV_SET_REL_PATHLIST_HOOK: OnceLock<pg_sys::set_rel_pathlist_hook_type> =
    OnceLock::new();

/// Install pathlist hook at `_PG_init`; single-threaded startup only.
///
/// # Safety
///
/// Must be called during extension initialization before concurrent planner
/// activity can mutate PostgreSQL's global hook pointer.
pub unsafe fn install_set_rel_pathlist_hook() {
    PREV_SET_REL_PATHLIST_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(set_rel_pathlist_callback);
        prev
    });
}

/// Framework pathlist callback: chain prev hook, gates, match provider, emit paths.
#[pg_guard]
unsafe extern "C-unwind" fn set_rel_pathlist_callback(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    // Chain previous hook first.
    if let Some(Some(prev)) = PREV_SET_REL_PATHLIST_HOOK.get() {
        unsafe { prev(root, rel, rti, rte) };
    }

    if !gucs::enabled() {
        return;
    }

    let candidate = match unsafe { CustomScanCandidate::inspect(root, rel, rte) } {
        Ok(candidate) => candidate,
        Err(_) => return,
    };
    let ctx = unsafe { candidate.provider_context() };

    let provider = match find_matching_provider(&ctx).report_unwrap() {
        Some(p) => p,
        None => return,
    };

    let planner = unsafe { CustomScanPathPlanner::new(candidate, provider) };
    unsafe { planner.emit() };
}

/// Assert [`set_rel_pathlist_callback`] matches `set_rel_pathlist_hook_type`.
#[cfg(feature = "pg_test")]
#[doc(hidden)]
pub fn pg_test_assert_set_rel_pathlist_callback_signature() {
    let _slot: pg_sys::set_rel_pathlist_hook_type = Some(set_rel_pathlist_callback);
}

#[cfg(test)]
mod router_tests {
    use super::*;

    #[test]
    fn prev_hook_storage_is_oncelock_typed() {
        fn assert_same_type<T>(_: &OnceLock<T>) {}
        assert_same_type::<pg_sys::set_rel_pathlist_hook_type>(
            &PREV_SET_REL_PATHLIST_HOOK,
        );
    }
}
