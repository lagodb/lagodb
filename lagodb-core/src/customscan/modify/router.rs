//! Provider-side adapter for the runtime-owned modify planning router.

use std::ffi::c_void;
use std::mem::size_of;
use std::ptr;

use pgrx::pg_sys;

use crate::runtime_api::{FfiErrorRecord, ModifyPlannerDescriptor};

use super::planning;

pub(super) fn descriptor() -> ModifyPlannerDescriptor {
    ModifyPlannerDescriptor {
        struct_size: size_of::<ModifyPlannerDescriptor>() as u32,
        context: ptr::null_mut(),
        planner_pre: Some(planner_pre),
        planner_post: Some(planner_post),
        create_upper_paths: Some(plan_upper_paths),
    }
}

unsafe extern "C-unwind" fn planner_pre(
    _context: *mut c_void,
    parse: *mut pg_sys::Query,
    error: *mut FfiErrorRecord,
) -> u32 {
    let operation = || {
        // SAFETY: `parse` is the live query forwarded by the planner hook.
        unsafe { planning::prepare_query_tree(parse) }
    };
    // SAFETY: the runtime supplies a live error record and forwards a live
    // rewrite-complete planner query for this synchronous callback.
    unsafe { (&mut *error).capture(operation) }
}

unsafe extern "C-unwind" fn planner_post(
    _context: *mut c_void,
    planned: *mut pg_sys::PlannedStmt,
    error: *mut FfiErrorRecord,
) -> u32 {
    let operation = || {
        // SAFETY: `planned` is the live non-null planner result forwarded by
        // the runtime.
        unsafe { planning::fixup_planned_statement(planned) };
        Ok(())
    };
    // SAFETY: the runtime owns the exact-build error record for the duration
    // of this callback and only dispatches post callbacks for a non-null plan.
    unsafe { (&mut *error).capture(operation) }
}

unsafe extern "C-unwind" fn plan_upper_paths(
    _context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
    error: *mut FfiErrorRecord,
) -> u32 {
    let operation = || {
        // SAFETY: all arguments are forwarded from the live PostgreSQL
        // upper-path hook invocation.
        unsafe {
            planning::create_upper_paths(root, stage, input_rel, output_rel, extra)
        };
        Ok(())
    };
    // SAFETY: the runtime forwards live PostgreSQL upper-path hook arguments
    // and consumes the error record before returning to PostgreSQL.
    unsafe { (&mut *error).capture(operation) }
}
