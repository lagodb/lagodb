//! PostgreSQL hook wiring for AM DML frame lifecycle.
//!
//! PostgreSQL's table-AM callbacks are intentionally low level: `tuple_insert`,
//! `tuple_update`, `tuple_delete`, and `multi_insert` are invoked at the point a
//! physical relation is written, but the callbacks are not told where the
//! statement/frame begins or ends.  That is not enough for lakehouse-style table
//! AMs.  A writer usually needs an AM-facing `dml_init`/`dml_fini` shape:
//! open metadata and file writers once for the current write frame, accept one
//! or more tuple callbacks, then publish or abort the staged write exactly once.
//!
//! This module derives those frame boundaries from PostgreSQL execution hooks:
//!
//! - `ExecutorStart_hook`: PostgreSQL has finished `ExecInitNode` and every
//!   `ModifyTableState` has an `ExecProcNodeReal`.  We use this to find
//!   `ModifyTable` plan states, record their `CmdType`, and wrap only their
//!   `ExecProcNodeReal`.
//! - `ExecutorFinish_hook`: PostgreSQL uses this to complete remaining executor
//!   work such as `ExecPostprocessPlan` and AFTER triggers.  We do not finalize
//!   DML here; the wrapper finalizes when the `ModifyTable` node reports
//!   `NULL && mt_done`.  The hook is only an invariant check so we notice a
//!   frame that leaked into finish unexpectedly.
//! - `ExecutorEnd_hook`: PostgreSQL uses this to tear down executor state after
//!   `ExecutorFinish`.  It is a cleanup boundary, not a success boundary, so we
//!   remove wrapper bookkeeping here but leave DML success/failure decisions to
//!   the frame manager and `ResourceOwner`.
//! - `ProcessUtility_hook` via our utility-hook registry: COPY FROM bypasses
//!   `ModifyTable` and calls table-AM insert callbacks directly, so COPY needs a
//!   separate frame around the utility command.
//!
//! Success finalization is frame scoped: `ModifyTable` frames finish when the
//! node reaches `mt_done`; COPY FROM frames finish in the utility post hook.
//! ERROR, abort, and rollback-to-savepoint are handled by `ResourceOwner`
//! cleanup registered by the frame manager.

use crate::diag::ReportableError;
use crate::hooks::{
    CopyStmtNode, PostUtilityContext, UtilityHook, UtilityHookError, UtilityNode,
    register_utility_hook,
};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{pg_guard, pg_sys};
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::{Once, OnceLock};

use super::session;

static DML_LIFECYCLE_INIT: Once = Once::new();
static PREV_EXECUTOR_START: OnceLock<pg_sys::ExecutorStart_hook_type> =
    OnceLock::new();
static PREV_EXECUTOR_FINISH: OnceLock<pg_sys::ExecutorFinish_hook_type> =
    OnceLock::new();
static PREV_EXECUTOR_END: OnceLock<pg_sys::ExecutorEnd_hook_type> = OnceLock::new();

// Hook chaining matters because PostgreSQL exposes each executor hook as a
// single global function pointer.  Extensions cooperate by saving the previous
// pointer and invoking it, or the `standard_*` implementation if there was no
// previous hook.  The pgrx FFI boundary around previous/standard calls lets
// Postgres ERRORs unwind safely through Rust frames.
unsafe fn invoke_prev_executor_start(
    prev: unsafe extern "C-unwind" fn(
        query_desc: *mut pg_sys::QueryDesc,
        eflags: ::core::ffi::c_int,
    ),
    query_desc: *mut pg_sys::QueryDesc,
    eflags: ::core::ffi::c_int,
) {
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            prev(query_desc, eflags);
        });
    }
}

unsafe fn invoke_prev_executor_finish(
    prev: unsafe extern "C-unwind" fn(query_desc: *mut pg_sys::QueryDesc),
    query_desc: *mut pg_sys::QueryDesc,
) {
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            prev(query_desc);
        });
    }
}

unsafe fn invoke_prev_executor_end(
    prev: unsafe extern "C-unwind" fn(query_desc: *mut pg_sys::QueryDesc),
    query_desc: *mut pg_sys::QueryDesc,
) {
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            prev(query_desc);
        });
    }
}

fn report_executor_finish_invariant(err: ErrorReport) {
    #[cfg(debug_assertions)]
    Err::<(), ErrorReport>(err).report_unwrap();

    #[cfg(not(debug_assertions))]
    crate::diag::report_warning(&err.to_string());
}

// PostgreSQL's planstate walker visits children but not the root node passed to
// it.  `ModifyTable` may also appear outside the main plan tree: subplans/CTEs
// live in `es_subplanstates`, and data-modifying CTE auxiliary ModifyTables live
// in `es_auxmodifytables`.  We collect all three sources and de-duplicate by
// `PlanState` pointer so a single executor only wraps a node once.
struct ModifyTableCollector {
    nodes: Vec<NonNull<pg_sys::PlanState>>,
}

impl ModifyTableCollector {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    unsafe fn collect_tree(&mut self, planstate: *mut pg_sys::PlanState) {
        let Some(planstate) = NonNull::new(planstate) else {
            return;
        };

        self.visit(planstate);

        unsafe {
            pg_sys::planstate_tree_walker_impl(
                planstate.as_ptr(),
                Some(collect_modifytable_walker),
                self as *mut Self as *mut c_void,
            );
        }
    }

    unsafe fn collect_list(&mut self, list: *mut pg_sys::List) {
        let len = unsafe { pg_sys::list_length(list) };
        for index in 0..len {
            let planstate =
                unsafe { pg_sys::list_nth(list, index) } as *mut pg_sys::PlanState;
            unsafe {
                self.collect_tree(planstate);
            }
        }
    }

    fn visit(&mut self, planstate: NonNull<pg_sys::PlanState>) {
        let is_modifytable = unsafe {
            (*planstate.as_ptr()).type_ == pg_sys::NodeTag::T_ModifyTableState
        };
        if is_modifytable && !self.nodes.contains(&planstate) {
            self.nodes.push(planstate);
        }
    }

    fn into_nodes(self) -> Vec<NonNull<pg_sys::PlanState>> {
        self.nodes
    }
}

unsafe extern "C-unwind" fn collect_modifytable_walker(
    planstate: *mut pg_sys::PlanState,
    context: *mut c_void,
) -> bool {
    let collector = unsafe { &mut *(context as *mut ModifyTableCollector) };
    unsafe {
        collector.collect_tree(planstate);
    }
    false
}

unsafe fn collect_modifytable_nodes(
    query_desc: *mut pg_sys::QueryDesc,
    estate: *mut pg_sys::EState,
) -> Vec<NonNull<pg_sys::PlanState>> {
    let mut collector = ModifyTableCollector::new();
    unsafe {
        collector.collect_tree((*query_desc).planstate);
        collector.collect_list((*estate).es_subplanstates);
        collector.collect_list((*estate).es_auxmodifytables);
    }
    collector.into_nodes()
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: ::core::ffi::c_int,
) {
    // PostgreSQL calls ExecutorStart after planning and before the first
    // ExecutorRun.  We call the previous/standard hook first so the executor has
    // initialized plan states in the normal way.  After that, PG17 has already
    // populated `ExecProcNodeReal`, which is the stable per-node dispatch slot we
    // want to wrap.  `ExecSetExecProcNode` is not used here because that would
    // rebuild more executor dispatch state than necessary.
    unsafe {
        if let Some(prev) = PREV_EXECUTOR_START.get().copied().flatten() {
            invoke_prev_executor_start(prev, query_desc, eflags);
        } else {
            pg_sys::ffi::pg_guard_ffi_boundary(|| {
                pg_sys::standard_ExecutorStart(query_desc, eflags);
            });
        }
    }

    if query_desc.is_null()
        || (eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as ::core::ffi::c_int) != 0
    {
        // EXPLAIN-only initialization builds plan state for inspection but does
        // not execute DML.  Registering wrappers/resources there would create
        // stale state with no matching tuple callbacks.
        return;
    }

    let estate = unsafe { (*query_desc).estate };
    let Some(estate_key) = NonNull::new(estate) else {
        return;
    };

    let nodes = unsafe { collect_modifytable_nodes(query_desc, estate) };
    if nodes.is_empty() {
        return;
    }

    session::register_executor_adapter(estate_key, nodes).report_unwrap();
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_finish_hook(query_desc: *mut pg_sys::QueryDesc) {
    // ExecutorFinish is intentionally not a finalize boundary for us.  In
    // PostgreSQL it is the "finish executor work" phase (for example,
    // ExecPostprocessPlan and AFTER triggers), and it can be reached for portal
    // cleanup paths.  A successful DML frame is finalized by the ModifyTable
    // wrapper when the node returns NULL with mt_done set.  Here we only check,
    // before standard ExecutorFinish mutates executor state, that no frame for
    // this EState is still on the active stack.
    if !query_desc.is_null() {
        let estate = unsafe { (*query_desc).estate };
        if let Some(estate) = NonNull::new(estate) {
            if let Err(err) = session::check_executor_finish_invariants(estate) {
                report_executor_finish_invariant(err);
            }
        }
    }

    unsafe {
        if let Some(prev) = PREV_EXECUTOR_FINISH.get().copied().flatten() {
            invoke_prev_executor_finish(prev, query_desc);
        } else {
            pg_sys::ffi::pg_guard_ffi_boundary(|| {
                pg_sys::standard_ExecutorFinish(query_desc);
            });
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    // ExecutorEnd is PostgreSQL's executor teardown hook.  Normal portal CLOSE
    // calls PortalCleanup, which invokes ExecutorFinish before ExecutorEnd; an
    // ERROR path may skip ExecutorEnd entirely and rely on transaction/resource
    // cleanup.  For that reason we use this hook only to remove adapter state
    // installed in ExecutorStart.  It must not be treated as "statement
    // succeeded" or used to publish DML frames.
    if !query_desc.is_null() {
        let estate = unsafe { (*query_desc).estate };
        if let Some(estate) = NonNull::new(estate) {
            session::end_executor_adapter(estate);
        }
    }

    unsafe {
        if let Some(prev) = PREV_EXECUTOR_END.get().copied().flatten() {
            invoke_prev_executor_end(prev, query_desc);
        } else {
            pg_sys::ffi::pg_guard_ffi_boundary(|| {
                pg_sys::standard_ExecutorEnd(query_desc);
            });
        }
    }
}

struct CopyFromFrameHook;

impl UtilityHook for CopyFromFrameHook {
    fn name(&self) -> &'static str {
        "dml_copy_from_frame"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        // COPY FROM is a utility statement, not a ModifyTable executor node.
        // PostgreSQL still calls the table-AM insert callbacks, so without a
        // utility-scope frame those callbacks would look like unmanaged DML.
        // COPY TO does not write through the table-AM DML callbacks and is
        // deliberately ignored here.
        let Some(copy_stmt) = context.cast::<CopyStmtNode>() else {
            return Ok(());
        };

        if copy_stmt.is_from {
            session::begin_copy_from_frame();
        }

        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        // The utility post hook only runs on the normal success path.  If COPY
        // errors while reading or inserting rows, PostgreSQL unwinds past the
        // post hook and ResourceOwner cleanup drops the frame, causing
        // abort_modify() for any unfinalized sessions.
        let Some(copy_stmt) = context.original_stmt().cast::<CopyStmtNode>() else {
            return Ok(());
        };

        if copy_stmt.is_from {
            session::finish_current_copy_frame()
                .map_err(|err| UtilityHookError::new(err.to_string()))?;
        }

        Ok(())
    }
}

/// Register DML frame lifecycle hooks.
///
/// This should be called during extension initialization. Repeated calls are
/// ignored in the current backend.  The hook state is process-local, matching
/// PostgreSQL's backend-per-connection model; each backend installs the hooks
/// once when the extension is loaded.
pub fn init_lifecycle_hooks() {
    DML_LIFECYCLE_INIT.call_once(|| {
        PREV_EXECUTOR_START.get_or_init(|| unsafe {
            let prev = pg_sys::ExecutorStart_hook;
            pg_sys::ExecutorStart_hook = Some(executor_start_hook);
            prev
        });

        PREV_EXECUTOR_FINISH.get_or_init(|| unsafe {
            let prev = pg_sys::ExecutorFinish_hook;
            pg_sys::ExecutorFinish_hook = Some(executor_finish_hook);
            prev
        });

        PREV_EXECUTOR_END.get_or_init(|| unsafe {
            let prev = pg_sys::ExecutorEnd_hook;
            pg_sys::ExecutorEnd_hook = Some(executor_end_hook);
            prev
        });

        register_utility_hook(
            pg_sys::NodeTag::T_CopyStmt,
            Box::new(CopyFromFrameHook),
        );
    });
}
