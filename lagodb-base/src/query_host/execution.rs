//! Executor lifecycle for the base-owned serial query-offload scan.

use std::cell::UnsafeCell;
use std::ffi::c_int;
use std::rc::Rc;
use std::{mem, ptr};

use lagodb_core::resource::{ResourceHandle, forget_resource, remember_resource};
use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::{
    QueryExecutionError, SerialExecutionLimits, SerialQueryExecution,
};
use lagodb_query::plan::SelectedQueryPlan;
use pgrx::{PgMemoryContexts, pg_guard, pg_sys};

use crate::runtime_api::table_scan_registry::TableScanRegistry;

use super::error::QueryHostError;
use super::explain::QueryOffloadExplain;
use super::methods;

enum QueryPhase {
    Created,
    ExplainOnly,
    Running(Rc<QueryExecutionCell>),
    Closed,
}

/// Single-threaded owner shared by the CustomScan state and PostgreSQL's
/// ResourceOwner cleanup callback.
///
/// pgrx converts a PostgreSQL ERROR raised by a protected outbound FFI call
/// into a Rust unwind before reporting it again at the inbound callback
/// boundary. After control returns to PostgreSQL, cleanup of a failed portal
/// may omit `EndCustomScan`, and its ResourceOwner may be released independently
/// of the executor MemoryContext. Both cleanup paths therefore share this cell:
/// whichever runs first takes the execution, while the other observes an empty
/// cell. Normal `EndCustomScan` closes the same value and unregisters the
/// callback. Executor and cleanup callbacks are serialized on one backend
/// thread, so accesses cannot overlap.
struct QueryExecutionCell {
    execution: UnsafeCell<Option<SerialQueryExecution>>,
}

impl QueryExecutionCell {
    fn new(execution: SerialQueryExecution) -> Self {
        Self {
            execution: UnsafeCell::new(Some(execution)),
        }
    }

    /// # Safety
    ///
    /// The caller must be the active PostgreSQL backend callback for this
    /// CustomScan; no ResourceOwner cleanup callback may run concurrently.
    unsafe fn with_mut<R>(
        &self,
        operation: impl FnOnce(&mut SerialQueryExecution) -> R,
    ) -> Option<R> {
        unsafe { (&mut *self.execution.get()).as_mut().map(operation) }
    }

    /// # Safety
    ///
    /// The caller must uphold the same serialized callback contract as
    /// [`Self::with_mut`].
    unsafe fn with_ref<R>(
        &self,
        operation: impl FnOnce(&SerialQueryExecution) -> R,
    ) -> Option<R> {
        unsafe { (&*self.execution.get()).as_ref().map(operation) }
    }

    fn close(&self) -> Result<(), QueryExecutionError> {
        // SAFETY: normal executor close and ResourceOwner cleanup are
        // serialized on the PostgreSQL backend thread.
        unsafe { (&mut *self.execution.get()).take() }
            .map_or(Ok(()), SerialQueryExecution::close)
    }

    fn abort(&self) {
        // PG evaluator objects must not invoke commit-style executor cleanup
        // after an ERROR unwind. The execution owner still releases Rust and
        // provider resources; the outer executor's statement context reclaims
        // its evaluator state.
        if let Some(execution) = unsafe { (&mut *self.execution.get()).take() } {
            execution.abort();
        }
    }
}

#[repr(C)]
struct QueryOffloadScanState {
    base: pg_sys::CustomScanState,
    phase: QueryPhase,
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
        let result = match mem::replace(&mut self.phase, QueryPhase::Closed) {
            QueryPhase::Running(execution) => execution.close(),
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
        if let QueryPhase::Running(execution) =
            mem::replace(&mut self.phase, QueryPhase::Closed)
        {
            execution.abort();
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
    ) -> Result<SerialExecutionLimits, QueryHostError> {
        // PostgreSQL defines work_mem in KiB and enforces its positive GUC
        // range before executor startup. The complete budget belongs to the
        // DataFusion execution pool; provider libraries are outside the
        // engine's memory-accounting contract.
        // SAFETY: `work_mem` is a backend-local PostgreSQL GUC read on the
        // backend main thread.
        let total = usize::try_from(unsafe { pg_sys::work_mem })
            .ok()
            .and_then(|kib| kib.checked_mul(1_024))
            .ok_or(QueryHostError::MemoryBudgetOverflow)?;
        SerialExecutionLimits::try_new(total, execution).map_err(QueryHostError::from)
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
            methods: methods::tables().exec(),
            ..Default::default()
        },
        phase: QueryPhase::Created,
        resource: None,
        explain: QueryOffloadExplain::new(),
    };
    let state = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state);
    state.cast()
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    if let Err(error) = unsafe { begin_scan(node, eflags) } {
        error.into_report().report();
    }
}

unsafe fn begin_scan(
    node: *mut pg_sys::CustomScanState,
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
    if !explain_only {
        state.explain.start_execution();
    }
    let selected =
        unsafe { SelectedQueryPlan::decode_execution(&*(*scan).custom_private) }
            .map_err(QueryHostError::invalid_plan)?;
    let (query, execution_profile, scans) = selected.into_parts();
    state
        .explain
        .record_plan(&scans, execution_profile, query.fragment().summary());

    if explain_only {
        state.phase = QueryPhase::ExplainOnly;
        return Ok(());
    }

    let limits = WorkMemBudget::execution_limits(execution_profile)?;
    let callbacks = scans
        .iter()
        .map(|scan| TableScanRegistry::callbacks(scan.provider()))
        .collect::<Result<Vec<_>, _>>()?;
    let execution = Rc::new(QueryExecutionCell::new(SerialQueryExecution::try_new(
        query,
        &scans,
        limits,
        &callbacks,
        unsafe { (*scan).custom_exprs },
        unsafe { &mut (*node).ss.ps },
    )?));
    let cleanup = Rc::clone(&execution);
    state.resource = Some(remember_resource(move || cleanup.abort()));
    state.phase = QueryPhase::Running(execution);
    Ok(())
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
    let QueryPhase::Running(execution) = &state.phase else {
        return Err(QueryHostError::ExecutorContract(
            "ExecCustomScan was invoked while query offload was not running",
        ));
    };
    let produced = unsafe {
        execution
            .with_mut(|execution| execution.next_into_slot(slot, per_tuple_context))
    }
    .ok_or(QueryHostError::ExecutorContract(
        "query-offload execution was already released",
    ))??;
    if !produced {
        return Ok(slot);
    }
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
    let result = (|| match &mut state.phase {
        QueryPhase::Running(execution) => {
            unsafe { execution.with_mut(SerialQueryExecution::rescan) }
                .ok_or(QueryHostError::ExecutorContract(
                    "query-offload execution was already released",
                ))?
                .map_err(QueryHostError::from)
        }
        QueryPhase::ExplainOnly => Ok(()),
        QueryPhase::Created | QueryPhase::Closed => {
            Err(QueryHostError::ExecutorContract(
                "ReScanCustomScan was invoked outside an active phase",
            ))
        }
    })();
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
    let state = unsafe { QueryOffloadScanState::from_node(&mut *node) };
    let result = match &state.phase {
        QueryPhase::Running(execution) => unsafe {
            execution.with_ref(|execution| {
                let metrics = execution.metrics();
                state.explain.emit(
                    Some(&metrics),
                    Some(execution.physical_operators()),
                    explain,
                )
            })
        }
        .unwrap_or_else(|| unsafe { state.explain.emit(None, None, explain) }),
        QueryPhase::Created | QueryPhase::ExplainOnly | QueryPhase::Closed => unsafe {
            state.explain.emit(None, None, explain)
        },
    };
    if let Err(error) = result {
        error.into_report().report();
    }
}
