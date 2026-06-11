//! ModifyTable node wrapper: discovering ModifyTable DML frame boundaries.
//!
//! PostgreSQL's table-AM callbacks carry no statement/frame context, so the
//! frame boundary is found by wrapping each ModifyTable node's
//! `ExecProcNodeReal` ([`lakebase_modifytable_wrapper`]): it pushes the frame on
//! entry and finalizes it when the node reports end-of-node. [`NODE_ADAPTERS`]
//! maps each wrapped PlanState to its saved dispatcher + command type;
//! [`EXECUTOR_ADAPTERS`] tracks the wrapped nodes per executor so ResourceOwner
//! cleanup can unwind them after an ERROR.
//!
//! The frame/session lifecycle this drives lives in [`super::session`]. The
//! dependency is one-directional: this module calls into `session`
//! (`push_current_frame`, `pop_current_frame`, `finish_frame`,
//! `abort_frame_and_remove_stack`, `current_modifytable_frames`) and hands the
//! ModifyTable node's command type to `session` at push time, so `session`
//! never queries this module back.

use crate::diag::{PgReportError, ReportableError};
use crate::resource::{self, ResourceHandle};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr::NonNull;

use super::session::{
    FrameKey, abort_frame_and_remove_stack, current_modifytable_frames, finish_frame,
    internal_error, pop_current_frame, push_current_frame,
};

#[derive(Clone, Copy)]
struct NodeAdapter {
    estate: NonNull<pg_sys::EState>,
    original: unsafe extern "C-unwind" fn(
        pstate: *mut pg_sys::PlanState,
    ) -> *mut pg_sys::TupleTableSlot,
    cmd_type: pg_sys::CmdType::Type,
}

struct ExecutorAdapter {
    estate: NonNull<pg_sys::EState>,
    resource_handle: ResourceHandle,
    nodes: Vec<NonNull<pg_sys::PlanState>>,
}

thread_local! {
    // PlanState -> wrapper metadata installed by ExecutorStart.  This map is
    // separate from FRAMES because a ModifyTable node may exist before it has
    // produced any table-AM write callback.
    static NODE_ADAPTERS: RefCell<HashMap<NonNull<pg_sys::PlanState>, NodeAdapter>> =
        RefCell::new(HashMap::new());
    // EState -> all ModifyTable nodes wrapped for that executor.  The
    // ExecutorAdapter ResourceOwner cleanup removes stale wrappers if ERROR
    // prevents ExecutorEnd from running.
    static EXECUTOR_ADAPTERS: RefCell<HashMap<NonNull<pg_sys::EState>, ExecutorAdapter>> =
        RefCell::new(HashMap::new());
}

fn lookup_node_adapter(
    ps: NonNull<pg_sys::PlanState>,
) -> Result<NodeAdapter, PgReportError> {
    NODE_ADAPTERS.with(|adapters| {
        adapters.borrow().get(&ps).copied().ok_or_else(|| {
            internal_error("ModifyTable node adapter missing during DML execution")
        })
    })
}

pub(crate) fn register_executor_adapter(
    estate: NonNull<pg_sys::EState>,
    nodes: Vec<NonNull<pg_sys::PlanState>>,
) -> Result<(), PgReportError> {
    if nodes.is_empty() {
        return Ok(());
    }

    let new_nodes: Vec<_> = nodes
        .into_iter()
        .filter(|ps| {
            !NODE_ADAPTERS.with(|adapters| adapters.borrow().contains_key(ps))
        })
        .collect();
    if new_nodes.is_empty() {
        return Ok(());
    }

    let estate_key = estate;
    let resource_handle =
        resource::remember_resource(move || cleanup_executor_adapter(estate_key));

    // Register the ExecutorAdapter before wrapping nodes.  If wrapping a later
    // node fails, ResourceOwner cleanup can still remove the nodes already
    // wrapped for this executor instead of leaving stale NODE_ADAPTERS entries.
    EXECUTOR_ADAPTERS.with(|adapters| {
        adapters.borrow_mut().insert(
            estate,
            ExecutorAdapter {
                estate,
                resource_handle,
                nodes: Vec::with_capacity(new_nodes.len()),
            },
        );
    });

    for ps in new_nodes {
        wrap_modifytable_node(estate, ps)?;
        EXECUTOR_ADAPTERS.with(|adapters| {
            let mut adapters = adapters.borrow_mut();
            let adapter = adapters
                .get_mut(&estate)
                .expect("ExecutorAdapter should exist while wrapping DML nodes");
            adapter.nodes.push(ps);
        });
    }

    Ok(())
}

fn wrap_modifytable_node(
    estate: NonNull<pg_sys::EState>,
    ps: NonNull<pg_sys::PlanState>,
) -> Result<(), PgReportError> {
    unsafe {
        if (*ps.as_ptr()).type_ != pg_sys::NodeTag::T_ModifyTableState {
            return Err(internal_error(
                "attempted to wrap a non-ModifyTable plan state",
            ));
        }

        let original = (*ps.as_ptr()).ExecProcNodeReal.ok_or_else(|| {
            internal_error(
                "ModifyTable ExecProcNodeReal is not initialized after ExecutorStart",
            )
        })?;

        // Wrap only ExecProcNodeReal.  PostgreSQL's ExecInitNode has already
        // chosen the real implementation by ExecutorStart time; replacing this
        // slot lets us bracket calls to exactly this ModifyTable node while
        // preserving executor instrumentation and outer dispatch machinery.
        let mtstate = ps.as_ptr() as *mut pg_sys::ModifyTableState;
        NODE_ADAPTERS.with(|adapters| {
            adapters.borrow_mut().insert(
                ps,
                NodeAdapter {
                    estate,
                    original,
                    cmd_type: (*mtstate).operation,
                },
            );
        });
        (*ps.as_ptr()).ExecProcNodeReal = Some(lakebase_modifytable_wrapper);
    }

    Ok(())
}

fn cleanup_executor_adapter(estate: NonNull<pg_sys::EState>) {
    // Abort/error cleanup for wrappers installed in ExecutorStart.  We do not
    // try to restore ExecProcNodeReal because the owning executor memory is
    // being released; removing our TLS references is the important part.  If a
    // direct ExecProcNodeReal call raised a PostgreSQL ERROR, Rust cleanup after
    // `push_current_frame` did not run, so remove the corresponding frame key
    // here as the ResourceOwner cleanup boundary.
    let adapter =
        EXECUTOR_ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&estate));
    let Some(adapter) = adapter else {
        return;
    };

    debug_assert_eq!(adapter.estate, estate);
    NODE_ADAPTERS.with(|node_adapters| {
        let mut node_adapters = node_adapters.borrow_mut();
        for node in adapter.nodes.iter() {
            node_adapters.remove(node);
        }
    });
    for node in adapter.nodes {
        abort_frame_and_remove_stack(FrameKey::ModifyTable(node));
    }
}

pub(crate) fn end_executor_adapter(estate: NonNull<pg_sys::EState>) {
    // Normal ExecutorEnd path.  This is not a DML success boundary; PostgreSQL
    // calls ExecutorFinish before ExecutorEnd on normal portal cleanup, and
    // ERROR paths may skip ExecutorEnd entirely.  Therefore this function only
    // removes adapter state and forgets the adapter ResourceOwner handle.
    let adapter =
        EXECUTOR_ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&estate));
    let Some(adapter) = adapter else {
        return;
    };

    resource::forget_resource(adapter.resource_handle);
    debug_assert_eq!(adapter.estate, estate);
    NODE_ADAPTERS.with(|node_adapters| {
        let mut node_adapters = node_adapters.borrow_mut();
        for node in adapter.nodes {
            node_adapters.remove(&node);
        }
    });
}

pub(crate) fn check_executor_finish_invariants(
    estate: NonNull<pg_sys::EState>,
) -> Result<(), PgReportError> {
    // ExecutorFinish can run executor post-processing and triggers.  If a
    // ModifyTable frame for this executor is still on the current stack at this
    // point, our wrapper push/pop accounting is inconsistent.  In release builds
    // the hook reports this as a warning so unusual but legal portal paths do
    // not become user-visible query failures.
    for ps in current_modifytable_frames() {
        let adapter =
            NODE_ADAPTERS.with(|adapters| adapters.borrow().get(&ps).copied());
        if adapter.is_some_and(|adapter| adapter.estate == estate) {
            return Err(internal_error(
                "ExecutorFinish reached while a ModifyTable frame is active",
            ));
        }
    }
    Ok(())
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn lakebase_modifytable_wrapper(
    ps: *mut pg_sys::PlanState,
) -> *mut pg_sys::TupleTableSlot {
    let Some(ps_key) = NonNull::new(ps) else {
        return std::ptr::null_mut();
    };
    let key = FrameKey::ModifyTable(ps_key);

    let adapter = lookup_node_adapter(ps_key).report_unwrap();

    // Frame boundary for this ModifyTable node: while PostgreSQL runs the node,
    // table-AM callbacks resolve the current frame to `key` and lazily create
    // per-relation sessions. A null slot with `mt_done` means end-of-node, so
    // finalization waits until then (a non-null slot is still RETURNING output).
    push_current_frame(key, adapter.cmd_type);
    // `original` is the saved ExecProcNodeReal; it can re-enter pgrx
    // `#[pg_guard]` callbacks, so it is called directly (not via
    // `pg_guard_ffi_boundary`). If it longjmps past the pop below, the executor
    // adapter's ResourceOwner cleanup removes the frame key.
    let slot = unsafe { (adapter.original)(ps) };

    let mtstate = ps as *mut pg_sys::ModifyTableState;
    let finalize_result = if slot.is_null() && unsafe { (*mtstate).mt_done } {
        finish_frame(key)
    } else {
        Ok(())
    };
    pop_current_frame(key);
    finalize_result.report_unwrap();

    slot
}
