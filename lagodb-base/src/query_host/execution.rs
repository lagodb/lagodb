//! Executor lifecycle for the base-owned scalar AggregateScan.

use std::ffi::c_int;
use std::mem;

use lagodb_core::diag::report_warning;
use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::{SerialCountExecution, SerialExecutionLimits};
use lagodb_query::plan::{
    AggregateExpression, QueryNode, QueryPlanData, QueryPlanEnvelope,
};
use pgrx::{PgMemoryContexts, pg_guard, pg_sys};

use crate::runtime_api::source_directory;

use super::error::QueryHostError;
use super::methods;
use super::metrics::AggregateExplain;

enum AggregatePhase {
    Created,
    ExplainOnly,
    Running(Box<SerialCountExecution>),
    Closed,
}

#[repr(C)]
struct AggregateScanState {
    base: pg_sys::CustomScanState,
    phase: AggregatePhase,
    explain: AggregateExplain,
}

impl AggregateScanState {
    /// Recover the Rust wrapper allocated by this method table.
    ///
    /// # Safety
    ///
    /// `node` must have been returned by [`create_state`] and remain owned by
    /// its executor memory context.
    unsafe fn from_node<'a>(node: *mut pg_sys::CustomScanState) -> &'a mut Self {
        // SAFETY: `Self` is repr(C) and `base` is its first field.
        unsafe { &mut *node.cast::<Self>() }
    }

    fn close(&mut self) -> Result<(), lagodb_query::datafusion::QueryExecutionError> {
        match mem::replace(&mut self.phase, AggregatePhase::Closed) {
            AggregatePhase::Running(execution) => execution.close(),
            AggregatePhase::Created
            | AggregatePhase::ExplainOnly
            | AggregatePhase::Closed => Ok(()),
        }
    }
}

impl Drop for AggregateScanState {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            report_warning(format_args!(
                "LagoDB Aggregate cleanup failed after executor abort: {error}",
            ));
        }
    }
}

struct CountOutputSlot;

impl CountOutputSlot {
    unsafe fn validate(
        scan: *mut pg_sys::CustomScan,
        slot: *mut pg_sys::TupleTableSlot,
        query: &QueryPlanData,
    ) -> Result<Self, QueryHostError> {
        let layout = query.tuple_layout();
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        if unsafe { (*tuple_desc).natts } as usize != layout.len() {
            return Err(QueryHostError::ExecutorContract(
                "scan slot width differs from the encoded output layout",
            ));
        }
        let attribute = unsafe { &*(*tuple_desc).attrs.as_ptr() };
        let planned = layout.slot();
        if attribute.attisdropped
            || attribute.atttypid != planned.type_oid()
            || attribute.atttypmod != planned.typmod()
            || attribute.attcollation != planned.collation()
        {
            return Err(QueryHostError::ExecutorContract(
                "scan slot type differs from the encoded COUNT output",
            ));
        }

        let target_list = unsafe { (*scan).custom_scan_tlist };
        if unsafe { pg_sys::list_length(target_list) } != 1 {
            return Err(QueryHostError::ExecutorContract(
                "AggregateScan scan target list must contain one output",
            ));
        }
        let target =
            unsafe { pg_sys::list_nth(target_list, 0) }.cast::<pg_sys::TargetEntry>();
        if unsafe { (*target).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
            || unsafe { (*target).resno } != 1
            || unsafe { (*target).resjunk }
            || unsafe { (*(*target).expr).type_ } != pg_sys::NodeTag::T_Aggref
        {
            return Err(QueryHostError::ExecutorContract(
                "AggregateScan scan target list is not the planned COUNT output",
            ));
        }

        let QueryNode::Project(project) = query.fragment().root() else {
            return Err(QueryHostError::ExecutorContract(
                "query fragment root is not a projection",
            ));
        };
        let QueryNode::Aggregate(aggregate) = project.input() else {
            return Err(QueryHostError::ExecutorContract(
                "query fragment projection does not consume an aggregate",
            ));
        };
        let [expression] = aggregate.aggregates() else {
            return Err(QueryHostError::ExecutorContract(
                "query fragment aggregate is not the planned scalar COUNT",
            ));
        };
        let AggregateExpression::CountStar(count) = expression;
        let expression = unsafe { (*target).expr }.cast::<pg_sys::Aggref>();
        if unsafe { (*expression).aggfnoid } != count.function_oid()
            || unsafe { (*expression).aggtype } != count.result_type()
            || !unsafe { (*expression).aggstar }
            || !unsafe { (*expression).args }.is_null()
            || !unsafe { (*expression).aggdirectargs }.is_null()
            || !unsafe { (*expression).aggfilter }.is_null()
            || unsafe { (*expression).aggsplit } != pg_sys::AggSplit::AGGSPLIT_SIMPLE
        {
            return Err(QueryHostError::ExecutorContract(
                "AggregateScan scan expression differs from the encoded COUNT",
            ));
        }
        Ok(Self)
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
    let state = AggregateScanState {
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
        phase: AggregatePhase::Created,
        explain: AggregateExplain::new(),
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
    let state = unsafe { AggregateScanState::from_node(node) };
    if !matches!(state.phase, AggregatePhase::Created) {
        return Err(QueryHostError::ExecutorContract(
            "BeginCustomScan was invoked outside the created phase",
        ));
    }
    let explain_only = (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0;
    if !explain_only {
        state.explain.start_execution();
    }
    let scan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let envelope = unsafe { QueryPlanEnvelope::decode((*scan).custom_private) }
        .map_err(QueryHostError::invalid_plan)?;
    let (query, execution_profile, source) = envelope.into_parts();
    let _output = unsafe {
        CountOutputSlot::validate(scan, (*node).ss.ss_ScanTupleSlot, &query)
    }?;
    state.explain.record_plan(
        source.provider(),
        source.source(),
        source.estimate(),
        execution_profile,
    );

    if explain_only {
        state.phase = AggregatePhase::ExplainOnly;
        return Ok(());
    }

    let limits = WorkMemBudget::execution_limits(execution_profile)?;
    let callbacks = source_directory::serial_source_callbacks(source.provider())?;
    let execution = Box::new(SerialCountExecution::try_new(
        query, source, limits, callbacks,
    )?);
    state.phase = AggregatePhase::Running(execution);
    Ok(())
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe { pg_sys::ExecScan(&mut (*node).ss, Some(next_count), Some(recheck)) }
}

#[pg_guard]
unsafe extern "C-unwind" fn next_count(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    match unsafe { scan_next_count(scan_state) } {
        Ok(slot) => slot,
        Err(error) => error.into_report().report(),
    }
}

unsafe fn scan_next_count(
    scan_state: *mut pg_sys::ScanState,
) -> Result<*mut pg_sys::TupleTableSlot, QueryHostError> {
    let state = unsafe {
        AggregateScanState::from_node(scan_state.cast::<pg_sys::CustomScanState>())
    };
    let AggregatePhase::Running(execution) = &mut state.phase else {
        return Err(QueryHostError::ExecutorContract(
            "ExecCustomScan was invoked while AggregateScan was not running",
        ));
    };
    let slot = unsafe { (*scan_state).ss_ScanTupleSlot };
    let _ = unsafe { pg_sys::ExecClearTuple(slot) };
    let per_tuple_context =
        unsafe { (*(*scan_state).ps.ps_ExprContext).ecxt_per_tuple_memory };
    if !unsafe { execution.next_into_slot(slot, per_tuple_context) }? {
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
    let state = unsafe { AggregateScanState::from_node(node) };
    let result = match &mut state.phase {
        AggregatePhase::Running(execution) => {
            execution.rescan().map_err(QueryHostError::from)
        }
        AggregatePhase::ExplainOnly => Ok(()),
        AggregatePhase::Created | AggregatePhase::Closed => {
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
    let state = unsafe { AggregateScanState::from_node(node) };
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
    let state = unsafe { AggregateScanState::from_node(node) };
    let (metrics, physical_operators) = match &state.phase {
        AggregatePhase::Running(execution) => (
            Some(execution.metrics()),
            Some(execution.physical_operators()),
        ),
        AggregatePhase::Created
        | AggregatePhase::ExplainOnly
        | AggregatePhase::Closed => (None, None),
    };
    if let Err(error) = unsafe {
        state
            .explain
            .emit(metrics.as_ref(), physical_operators, explain)
    } {
        error.into_report().report();
    }
}
