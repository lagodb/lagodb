//! Executor lifecycle for the base-owned query-offload scan.

mod owner;

use std::ffi::c_int;
use std::rc::Rc;
use std::{mem, ptr};

use lagodb_core::resource::{ResourceHandle, forget_resource, remember_resource};
use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::{
    ExecutionMetricsMode, ParallelExecutionHost, ParallelQueryOptions,
    PendingQueryExecution, QueryExecution, QueryExecutionError, QueryExecutionLimits,
    QueryExecutionRequest,
};
use lagodb_query::plan::SelectedQueryPlan;
use pgrx::{PgMemoryContexts, pg_guard, pg_sys};

use super::error::QueryHostError;
use super::explain::{ExplainOptions, QueryOffloadExplain};
use super::methods;
use super::parallel::PgParallelHost;
use super::table_scan_registry::TableScanRegistry;
use owner::QueryExecutionCell;

enum QueryPhase {
    Created,
    ExplainOnly,
    Active(Rc<QueryExecutionCell>),
    Closed,
}

#[repr(C)]
struct QueryOffloadScanState {
    base: pg_sys::CustomScanState,
    phase: QueryPhase,
    // Borrowed from the Active owner's stable Rc allocation at first execution.
    // Only the regular row method table accesses this pointer. End/abort revoke
    // access; ResourceOwner cleanup ends the portal's execution even when it
    // leaves this wrapper allocated until its MemoryContext is deleted.
    row_execution: *mut QueryExecution,
    resource: Option<ResourceHandle>,
    explain: QueryOffloadExplain,
}

impl QueryOffloadScanState {
    /// Recover the Rust wrapper allocated by this method table.
    ///
    /// # Safety
    ///
    /// `node` must have been returned by [`create_state`] and remain owned by
    /// its executor memory context.
    unsafe fn from_node(node: &mut pg_sys::CustomScanState) -> &mut Self {
        // SAFETY: `Self` is repr(C) and `base` is its first field.
        unsafe { &mut *ptr::from_mut(node).cast::<Self>() }
    }

    fn close(&mut self) -> Result<(), QueryExecutionError> {
        self.row_execution = ptr::null_mut();
        let result = match mem::replace(&mut self.phase, QueryPhase::Closed) {
            QueryPhase::Active(execution) => execution.close(),
            QueryPhase::Created | QueryPhase::ExplainOnly | QueryPhase::Closed => {
                Ok(())
            }
        };
        if let Some(resource) = self.resource.take() {
            let _ = forget_resource(resource);
        }
        result
    }

    fn abort(&mut self) {
        self.row_execution = ptr::null_mut();
        match mem::replace(&mut self.phase, QueryPhase::Closed) {
            QueryPhase::Active(execution) => execution.abort(),
            QueryPhase::Created | QueryPhase::ExplainOnly | QueryPhase::Closed => {}
        }
        if let Some(resource) = self.resource.take() {
            let _ = forget_resource(resource);
        }
    }
}

impl Drop for QueryOffloadScanState {
    fn drop(&mut self) {
        // PostgreSQL deletes descendant contexts before invoking this state
        // context's pgrx reset callback. A fallback evaluator's EState is one
        // such descendant, so this callback must only release Rust/provider
        // ownership and let PostgreSQL reclaim evaluator memory. Normal
        // EndCustomScan calls `close` while every descendant is still live.
        self.abort();
    }
}

struct WorkMemBudget;

impl WorkMemBudget {
    fn execution_limits(
        execution: ExecutionProfile,
    ) -> Result<QueryExecutionLimits, QueryHostError> {
        // PostgreSQL defines work_mem in KiB and enforces its positive GUC
        // range before executor startup. The complete budget belongs to the
        // DataFusion execution pool; provider libraries are outside the
        // engine's memory-accounting contract.
        // SAFETY: `work_mem` is a backend-local PostgreSQL GUC read on the
        // backend main thread.
        let work_mem_bytes = usize::try_from(unsafe { pg_sys::work_mem })
            .ok()
            .and_then(|kib| kib.checked_mul(1_024))
            .ok_or(QueryHostError::MemoryBudgetOverflow)?;
        // SAFETY: PostgreSQL derives this backend-local limit from work_mem and
        // hash_mem_multiplier with the same size_t clamp used by its executor.
        let hash_memory_bytes = unsafe { pg_sys::get_hash_memory_limit() };
        QueryExecutionLimits::try_new(work_mem_bytes, hash_memory_bytes, execution)
            .map_err(QueryHostError::from)
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn create_state(
    _scan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let state = QueryOffloadScanState {
        base: pg_sys::CustomScanState {
            ss: pg_sys::ScanState {
                ps: pg_sys::PlanState {
                    type_: pg_sys::NodeTag::T_CustomScanState,
                    ..Default::default()
                },
                ..Default::default()
            },
            methods: methods::tables().initial_exec(),
            ..Default::default()
        },
        phase: QueryPhase::Created,
        row_execution: ptr::null_mut(),
        resource: None,
        explain: QueryOffloadExplain::new(),
    };
    let state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state);
    state.cast()
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    if let Err(error) = unsafe { begin_scan(node, estate, eflags) } {
        error.into_report().report();
    }
}

unsafe fn begin_scan(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) -> Result<(), QueryHostError> {
    // Read the plan before borrowing the complete Rust state wrapper; both
    // occupy the same allocation through its leading CustomScanState field.
    let scan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
    if !matches!(state.phase, QueryPhase::Created) {
        return Err(QueryHostError::ExecutorContract(
            "BeginCustomScan was invoked outside the created phase",
        ));
    }
    let explain_only = (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0;
    // ExecInitNode installs PlanState instrumentation after BeginCustomScan;
    // EState carries the executor's request at this callback boundary.
    let instrumented = !explain_only && unsafe { (*estate).es_instrument } != 0;
    let selected =
        unsafe { SelectedQueryPlan::decode_execution(&*(*scan).custom_private) }
            .map_err(QueryHostError::invalid_plan)?;
    if explain_only {
        unsafe {
            state.explain.record_plan(&selected);
        }
        state.phase = QueryPhase::ExplainOnly;
        return Ok(());
    }
    let parallel_safe = selected.parallel_safe()
        && selected.largest_scan_rows()
            >= crate::gucs::query_offload_parallel_min_scan_rows();
    let (query, execution_profile, scans) = selected.into_parts();

    let limits = WorkMemBudget::execution_limits(execution_profile)?;
    let metrics_mode = if instrumented {
        ExecutionMetricsMode::Enabled
    } else {
        ExecutionMetricsMode::Disabled
    };
    let callbacks = scans
        .iter()
        .map(|scan| TableScanRegistry::resolve_provider_callbacks(scan.route()))
        .collect::<Result<Vec<_>, _>>()?;
    let host = PgParallelHost::leader();
    let parallel = if parallel_safe && host.worker_cap() >= 2 {
        let workers = scans
            .iter()
            .map(|scan| TableScanRegistry::resolve_worker_callbacks(scan.route()))
            .collect::<Result<Option<Vec<_>>, _>>()?;
        workers.map(|workers| {
            ParallelQueryOptions::new(
                host,
                &scans,
                workers,
                crate::gucs::query_offload_parallel_queue_size(),
                crate::gucs::query_offload_parallel_debug(),
            )
        })
    } else {
        None
    };
    let execution = Rc::new(QueryExecutionCell::new(PendingQueryExecution::try_new(
        QueryExecutionRequest {
            query,
            scans: &scans,
            limits,
            metrics_mode,
            callbacks: &callbacks,
            parallel,
            runtime_exprs: unsafe { (*scan).custom_exprs },
            parent: unsafe { &mut (*node).ss.ps },
        },
    )?));
    let cleanup = Rc::clone(&execution);
    state.resource = Some(remember_resource(move || cleanup.abort()));
    state.phase = QueryPhase::Active(execution);
    Ok(())
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn first_exec(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    // Keep provider/error allocations in the owning executor context, as the
    // core CustomScan Start boundary does. The regular row path has no start check.
    let estate = unsafe { (*node).ss.ps.state };
    let previous = unsafe { pg_sys::MemoryContextSwitchTo((*estate).es_query_cxt) };
    let result = (|| {
        let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
        let QueryPhase::Active(execution) = &state.phase else {
            return Err(QueryHostError::ExecutorContract(
                "query startup has no binding",
            ));
        };
        state.row_execution = unsafe { execution.start() }?;
        Ok(())
    })();
    unsafe { pg_sys::MemoryContextSwitchTo(previous) };
    if let Err(error) = result {
        error.into_report().report();
    }
    unsafe { (*node).methods = methods::tables().exec() };
    unsafe { exec(node) }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe { pg_sys::ExecScan(&mut (*node).ss, Some(next_tuple), Some(recheck)) }
}

#[pg_guard]
unsafe extern "C-unwind" fn next_tuple(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    match unsafe { scan_next(scan_state) } {
        Ok(slot) => slot,
        Err(error) => error.into_report().report(),
    }
}

unsafe fn scan_next(
    scan_state: *mut pg_sys::ScanState,
) -> Result<*mut pg_sys::TupleTableSlot, QueryHostError> {
    // Establish PostgreSQL-owned executor fields before borrowing the complete
    // Rust wrapper that contains this ScanState.
    let slot = unsafe { (*scan_state).ss_ScanTupleSlot };
    let per_tuple_context =
        unsafe { (*(*scan_state).ps.ps_ExprContext).ecxt_per_tuple_memory };
    let _ = unsafe { pg_sys::ExecClearTuple(slot) };
    let state = unsafe {
        QueryOffloadScanState::from_node(
            &mut *scan_state.cast::<pg_sys::CustomScanState>(),
        )
    };
    // SAFETY: first_exec publishes the owner's stable execution address before
    // installing this method table. Rescans retain that object; PG never resumes
    // this node after End or portal failure. Callback borrows do not overlap,
    // including owner-side rescan/EXPLAIN access and final resource cleanup.
    let execution = unsafe { &mut *state.row_execution };
    unsafe { execution.next_into_slot(slot, per_tuple_context) }?;
    Ok(slot)
}

#[pg_guard]
unsafe extern "C-unwind" fn recheck(
    _scan_state: *mut pg_sys::ScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    true
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan(node: *mut pg_sys::CustomScanState) {
    let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
    let result = match &mut state.phase {
        QueryPhase::Active(execution) => {
            let changed_parameters = unsafe { (*node).ss.ps.chgParam };
            unsafe { execution.rescan(changed_parameters) }
        }
        QueryPhase::ExplainOnly => Ok(()),
        QueryPhase::Created | QueryPhase::Closed => {
            Err(QueryHostError::ExecutorContract(
                "ReScanCustomScan was invoked outside an active phase",
            ))
        }
    };
    if let Err(error) = result {
        error.into_report().report();
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end(node: *mut pg_sys::CustomScanState) {
    let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
    if let Err(error) = state.close() {
        QueryHostError::from(error).into_report().report();
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn explain(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    explain: *mut pg_sys::ExplainState,
) {
    // Read the PostgreSQL-owned plan before borrowing the complete wrapper;
    // `node` aliases its leading `CustomScanState` field.
    let scan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
    let options = unsafe { ExplainOptions::from_state(explain) };
    let result = (|| {
        // auto_explain without ANALYZE installs no instrumentation, so Begin
        // deliberately does no EXPLAIN work. If it later elects to log this
        // statement, its callback still owns the live CustomScan plan and can
        // capture the presentation metadata on demand.
        if matches!(&state.phase, QueryPhase::Active(_)) && !state.explain.has_plan()
        {
            let selected = unsafe {
                SelectedQueryPlan::decode_execution(&*(*scan).custom_private)
            }
            .map_err(QueryHostError::invalid_plan)?;
            unsafe { state.explain.record_plan(&selected) };
        }

        match &state.phase {
            QueryPhase::Active(execution) => unsafe {
                execution.with_ref(
                    |pending| {
                        state.explain.emit(
                            Some(pending.fragment()),
                            None,
                            None,
                            None,
                            options,
                            explain,
                        )
                    },
                    |execution| {
                        let metrics = execution.metrics();
                        let planned_mode = execution.planned_mode();
                        let physical_plan = options.engine_diagnostics().then(|| {
                            execution
                                .physical_plan_analyze(options.timing)
                                .unwrap_or_else(|| execution.physical_plan_explain())
                        });
                        state.explain.emit(
                            Some(execution.fragment()),
                            metrics.as_ref(),
                            Some(planned_mode),
                            physical_plan.as_ref(),
                            options,
                            explain,
                        )
                    },
                )
            }
            .unwrap_or_else(|| unsafe {
                state.explain.emit(None, None, None, None, options, explain)
            }),
            QueryPhase::ExplainOnly => unsafe {
                state.explain.emit(None, None, None, None, options, explain)
            },
            QueryPhase::Created | QueryPhase::Closed => unsafe {
                state.explain.emit(None, None, None, None, options, explain)
            },
        }
    })();
    if let Err(error) = result {
        error.into_report().report();
    }
}
