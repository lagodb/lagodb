use std::collections::HashSet;
use std::ffi::c_void;
use std::rc::Rc;

use crate::access::mutation::acquire_modify_query_state;
use crate::api::TableAccessMethod;
use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::execution::exec::provider_scan_purpose;
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::modify::LakebaseCustomModifyProvider;
use crate::diag::{PgReportError, ReportableError};
use crate::resource::{ResourceHandle, forget_resource, remember_resource};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

use super::bridge::{LakebaseModifyBridge, ModifyNodeCell};
use super::execution::ModifyNodeState;
use super::methods;

unsafe extern "C-unwind" {
    unsafe fn lakebase_exec_modify_table(
        mtstate: *mut pg_sys::ModifyTableState,
        bridge: *mut LakebaseModifyBridge,
    ) -> *mut pg_sys::TupleTableSlot;
}

#[repr(C)]
struct LakebaseModifyTableState<P: LakebaseCustomModifyProvider> {
    /// PostgreSQL ABI prefix; must remain the first field.
    base: pg_sys::CustomScanState,
    /// Wrapped upstream ModifyTable executor state.
    inner: *mut pg_sys::ModifyTableState,
    /// Owner of relation-local AM ModifyState values.
    execution: Option<Rc<ModifyNodeCell<P>>>,
    /// Stable C ABI view into `execution`.
    bridge: Option<LakebaseModifyBridge>,
    /// ERROR/savepoint cleanup registration.
    resource: Option<ResourceHandle>,
    phase: ModifyNodePhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModifyNodePhase {
    Created,
    Begun,
    Ended,
}

impl<P: LakebaseCustomModifyProvider> Drop for LakebaseModifyTableState<P> {
    fn drop(&mut self) {
        if let Some(execution) = self.execution.as_ref() {
            // SAFETY: backend executor callbacks are serialized; Drop cannot
            // overlap a mutation callback for this state.
            unsafe { execution.with_mut(ModifyNodeState::abort) };
        }
        if let Some(resource) = self.resource.take() {
            let _ = forget_resource(resource);
        }
    }
}

unsafe fn state<'a, P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) -> &'a mut LakebaseModifyTableState<P> {
    // SAFETY: allocated by `create_state`, repr(C), base at offset zero.
    unsafe { &mut *node.cast::<LakebaseModifyTableState<P>>() }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn create_state<
    P: LakebaseCustomModifyProvider,
>(
    _scan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let wrapper = LakebaseModifyTableState::<P> {
        base: pg_sys::CustomScanState {
            ss: pg_sys::ScanState {
                ps: pg_sys::PlanState {
                    type_: pg_sys::NodeTag::T_CustomScanState,
                    ..Default::default()
                },
                ..Default::default()
            },
            methods: &methods::tables::<P>().modify_exec,
            ..Default::default()
        },
        inner: std::ptr::null_mut(),
        execution: None,
        bridge: None,
        resource: None,
        phase: ModifyNodePhase::Created,
    };
    let ptr = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(wrapper);
    ptr.cast()
}

struct ModifyScanBinder<'a, P: LakebaseCustomModifyProvider> {
    execution: &'a ModifyNodeCell<P>,
    bound_scans: Result<usize, CustomScanError>,
    target_rtis: HashSet<pg_sys::Index>,
}

unsafe fn scan_state_relation(
    plan_state: *mut pg_sys::PlanState,
) -> pg_sys::Relation {
    if plan_state.is_null()
        || !matches!(
            unsafe { (*plan_state).type_ },
            pg_sys::NodeTag::T_SeqScanState
                | pg_sys::NodeTag::T_SampleScanState
                | pg_sys::NodeTag::T_IndexScanState
                | pg_sys::NodeTag::T_IndexOnlyScanState
                | pg_sys::NodeTag::T_BitmapHeapScanState
                | pg_sys::NodeTag::T_TidScanState
                | pg_sys::NodeTag::T_TidRangeScanState
                | pg_sys::NodeTag::T_ForeignScanState
                | pg_sys::NodeTag::T_CustomScanState
        )
    {
        return std::ptr::null_mut();
    }
    unsafe { (*plan_state.cast::<pg_sys::ScanState>()).ss_currentRelation }
}

unsafe fn is_provider_relation<P: LakebaseCustomModifyProvider>(
    relation: pg_sys::Relation,
) -> bool {
    !relation.is_null()
        && P::AccessMethod::access_method_oid()
            .is_some_and(|oid| unsafe { (*(*relation).rd_rel).relam } == oid)
}

unsafe fn scan_rti(plan_state: *mut pg_sys::PlanState) -> Option<pg_sys::Index> {
    let relation = unsafe { scan_state_relation(plan_state) };
    if relation.is_null() {
        return None;
    }
    let plan = unsafe { (*plan_state).plan };
    Some(unsafe { (*plan.cast::<pg_sys::Scan>()).scanrelid })
}

unsafe fn bind_tree<P: LakebaseCustomModifyProvider>(
    plan_state: *mut pg_sys::PlanState,
    context: &mut ModifyScanBinder<'_, P>,
) {
    if plan_state.is_null() || context.bound_scans.is_err() {
        return;
    }
    if unsafe { (*plan_state).type_ } == pg_sys::NodeTag::T_CustomScanState {
        let custom = plan_state.cast::<pg_sys::CustomScanState>();
        let purpose = match unsafe { provider_scan_purpose::<P>((*plan_state).plan) }
        {
            Ok(purpose) => purpose,
            Err(error) => {
                context.bound_scans = Err(error);
                return;
            }
        };
        if purpose == Some(ScanPurpose::Modify) {
            let relation = unsafe { (*custom).ss.ss_currentRelation };
            if relation.is_null() {
                context.bound_scans = Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "Modify CustomScan has no open relation",
                )
                .into());
                return;
            }
            let oid = unsafe { (*relation).rd_id };
            let scan_context =
                unsafe { CustomScanStateWrapper::<P>::from_node_ptr(custom) }
                    .active_provider_state_mut()
                    .and_then(|state| P::modify_scan_context(state));
            let Some(scan_context) = scan_context else {
                context.bound_scans = Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "Modify CustomScan provider state is not active",
                )
                .into());
                return;
            };
            let binding = match unsafe {
                context
                    .execution
                    .with_mut(|execution| execution.bind_scan(oid, scan_context))
            } {
                Ok(binding) => binding,
                Err(error) => {
                    context.bound_scans = Err(error.into());
                    return;
                }
            };
            if let Err(error) =
                unsafe { super::binding::bind_modify_scan::<P>(custom, binding) }
            {
                context.bound_scans = Err(error);
                return;
            }
            if let Ok(bound_scans) = &mut context.bound_scans {
                *bound_scans += 1;
            }
        } else if unsafe { scan_rti(plan_state) }
            .is_some_and(|rti| context.target_rtis.contains(&rti))
            && unsafe { is_provider_relation::<P>((*custom).ss.ss_currentRelation) }
        {
            context.bound_scans = Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "required Modify relation was planned with an unbindable CustomScan",
            )
            .into());
            return;
        }
    } else if unsafe { scan_rti(plan_state) }
        .is_some_and(|rti| context.target_rtis.contains(&rti))
        && unsafe { is_provider_relation::<P>(scan_state_relation(plan_state)) }
    {
        context.bound_scans = Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "required Modify relation was planned with a standard scan",
        )
        .into());
        return;
    }
    unsafe {
        pg_sys::planstate_tree_walker_impl(
            plan_state,
            Some(bind_walker::<P>),
            std::ptr::from_mut(context).cast(),
        );
    }
}

unsafe extern "C-unwind" fn bind_walker<P: LakebaseCustomModifyProvider>(
    plan_state: *mut pg_sys::PlanState,
    raw_context: *mut c_void,
) -> bool {
    let context = unsafe { &mut *raw_context.cast::<ModifyScanBinder<'_, P>>() };
    unsafe { bind_tree::<P>(plan_state, context) };
    context.bound_scans.is_err()
}

unsafe fn replace_aux_entry(
    estate: *mut pg_sys::EState,
    inner: *mut pg_sys::ModifyTableState,
    outer: *mut pg_sys::CustomScanState,
) {
    let list = unsafe { (*estate).es_auxmodifytables };
    for index in 0..unsafe { pg_sys::list_length(list) } {
        let cell = unsafe { pg_sys::list_nth_cell(list, index) };
        if unsafe { (*cell).ptr_value } == inner.cast() {
            unsafe { (*cell).ptr_value = outer.cast() };
        }
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn begin<P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) {
    unsafe { begin_impl::<P>(node, estate, eflags) }.report_unwrap();
}

/// Initialize the wrapped ModifyTable and bind all provider scans before the
/// executor enters mutation callbacks. The FFI wrapper above is the only
/// reporting boundary for initialization failures.
unsafe fn begin_impl<P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) -> Result<(), CustomScanError> {
    let state = unsafe { state::<P>(node) };
    if state.phase != ModifyNodePhase::Created {
        return Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "LakebaseModifyTable was initialized more than once",
        )
        .into());
    }
    let plan = unsafe { (*node).ss.ps.plan.cast::<pg_sys::CustomScan>() };
    let inner_plan =
        unsafe { pg_sys::list_nth((*plan).custom_plans, 0).cast::<pg_sys::Plan>() };
    let inner_state = unsafe { pg_sys::ExecInitNode(inner_plan, estate, eflags) };
    if inner_state.is_null()
        || unsafe { (*inner_state).type_ } != pg_sys::NodeTag::T_ModifyTableState
    {
        return Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "LakebaseModifyTable child is not ModifyTableState",
        )
        .into());
    }
    state.inner = inner_state.cast();
    unsafe {
        (*node).custom_ps = pg_sys::lappend(std::ptr::null_mut(), inner_state.cast());
    }
    state.phase = ModifyNodePhase::Begun;

    if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(());
    }

    let query_state = acquire_modify_query_state::<P::AccessMethod>(estate)?;
    let execution = unsafe {
        ModifyNodeState::<P>::from_modify_table_state(state.inner, query_state)
    }?;
    let operation = unsafe { (*state.inner).operation };
    let input_plan = if matches!(
        operation,
        pg_sys::CmdType::CMD_UPDATE
            | pg_sys::CmdType::CMD_DELETE
            | pg_sys::CmdType::CMD_MERGE
    ) {
        let subplan = unsafe { (*state.inner).ps.lefttree };
        Some(unsafe { (*subplan).plan })
    } else {
        None
    };

    let execution = Rc::new(unsafe {
        ModifyNodeCell::<P>::new(execution, operation, input_plan)
    }?);
    let cleanup = Rc::clone(&execution);
    state.resource = Some(remember_resource(move || {
        // SAFETY: ResourceOwner cleanup runs after executor control has left
        // the mutation callback and is serialized in the backend thread.
        unsafe { cleanup.with_mut(ModifyNodeState::abort) };
    }));
    state.bridge = Some(execution.bridge());
    state.execution = Some(execution);

    let mut bind_context = ModifyScanBinder {
        execution: state
            .execution
            .as_deref()
            .expect("execution was just installed"),
        bound_scans: Ok(0),
        target_rtis: {
            let plan =
                unsafe { (*state.inner).ps.plan.cast::<pg_sys::ModifyTable>() };
            let relations = unsafe { (*plan).resultRelations };
            let count = unsafe { pg_sys::list_length(relations) };
            let mut rtis = HashSet::with_capacity(count as usize);
            for index in 0..count {
                rtis.insert(unsafe {
                    pg_sys::list_nth_int(relations, index) as pg_sys::Index
                });
            }
            rtis
        },
    };
    unsafe { bind_tree::<P>((*state.inner).ps.lefttree, &mut bind_context) };
    let bound_scans = bind_context.bound_scans?;
    if matches!(
        operation,
        pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE
    ) && bound_scans == 0
    {
        return Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "required Modify plan contains no bindable provider scan",
        )
        .into());
    }

    unsafe { replace_aux_entry(estate, state.inner, node) };
    Ok(())
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec<P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = unsafe { state::<P>(node) };
    let instrument = unsafe { (*state.inner).ps.instrument };
    if !instrument.is_null() {
        unsafe { pg_sys::InstrStartNode(instrument) };
    }
    let result = unsafe {
        lakebase_exec_modify_table(
            state.inner,
            state
                .bridge
                .as_mut()
                .expect("BeginCustomScan initialized bridge"),
        )
    };
    if !instrument.is_null() {
        unsafe {
            pg_sys::InstrStopNode(
                instrument,
                if result.is_null() { 0.0 } else { 1.0 },
            )
        };
    }
    if result.is_null() && unsafe { (*state.inner).mt_done } {
        let execution = state
            .execution
            .as_ref()
            .expect("BeginCustomScan initialized execution");
        // SAFETY: this callback is the exclusive executor access.
        unsafe { execution.with_mut(ModifyNodeState::finish) }.report_unwrap();
    }
    result
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn end<P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) {
    let state = unsafe { state::<P>(node) };
    if state.phase == ModifyNodePhase::Ended {
        return;
    }
    if !state.inner.is_null() {
        unsafe { pg_sys::ExecEndNode(&mut (*state.inner).ps) };
        state.inner = std::ptr::null_mut();
    }
    if let Some(execution) = state.execution.as_ref() {
        // SAFETY: inner Modify scans are ended, so no borrowed binding can be
        // accessed while aborting/releasing relation states.
        unsafe {
            execution.with_mut(|execution| {
                if !execution.is_finished() {
                    execution.abort();
                }
            })
        };
    }
    if let Some(resource) = state.resource.take() {
        let _ = forget_resource(resource);
    }
    state.bridge = None;
    state.execution = None;
    state.phase = ModifyNodePhase::Ended;
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn rescan<P: LakebaseCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) {
    let state = unsafe { state::<P>(node) };
    if !state.inner.is_null() {
        unsafe { pg_sys::ExecReScan(&mut (*state.inner).ps) };
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn explain(
    _node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    explain_state: *mut pg_sys::ExplainState,
) {
    unsafe {
        pg_sys::ExplainPropertyText(
            c"Executor".as_ptr(),
            c"PG17 Lakebase ModifyTable fork".as_ptr(),
            explain_state,
        );
    }
}
