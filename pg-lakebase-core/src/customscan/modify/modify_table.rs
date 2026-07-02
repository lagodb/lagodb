use std::collections::HashSet;
use std::ffi::c_void;
use std::rc::Rc;

use crate::access::mutation::acquire_modify_query_state;
use crate::api::{AmResult, TableAccessMethod};
use crate::customscan::CustomScanError;
use crate::customscan::provider::LakebaseCustomModifyProvider;
use crate::diag::{PgReportError, ReportableError};
use crate::resource::{ResourceHandle, forget_resource, remember_resource};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::{pg_guard, pg_sys};

use super::bridge::{LakebaseModifyBridge, ModifyNodeCell};
use super::execution::ModifyNodeState;
use super::{methods, planning};

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

fn find_wholerow_attno(plan: *mut pg_sys::Plan) -> AmResult<pg_sys::AttrNumber> {
    if plan.is_null() {
        return Err(PgReportError::from_message(
            pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "ModifyTable input plan is NULL",
        ));
    }
    // SAFETY: plan belongs to the initialized inner node.
    let tlist = unsafe { (*plan).targetlist };
    let wholerow = unsafe {
        pg_sys::ExecFindJunkAttributeInTlist(tlist, planning::WHOLEROW_NAME.as_ptr())
    };
    Ok(wholerow.max(0))
}

struct BindContext<'a, P: LakebaseCustomModifyProvider> {
    execution: &'a ModifyNodeCell<P>,
    error: Option<BindError>,
    bound_scans: usize,
    target_rtis: HashSet<pg_sys::Index>,
}

enum BindError {
    Execution(PgReportError),
    Scan(CustomScanError),
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
    if plan.is_null() {
        return None;
    }
    Some(unsafe { (*plan.cast::<pg_sys::Scan>()).scanrelid })
}

unsafe fn bind_tree<P: LakebaseCustomModifyProvider>(
    plan_state: *mut pg_sys::PlanState,
    context: &mut BindContext<'_, P>,
) {
    if plan_state.is_null() || context.error.is_some() {
        return;
    }
    if unsafe { (*plan_state).type_ } == pg_sys::NodeTag::T_CustomScanState {
        let custom = plan_state.cast::<pg_sys::CustomScanState>();
        let purpose = match unsafe {
            crate::customscan::exec::provider_scan_purpose::<P>((*plan_state).plan)
        } {
            Ok(purpose) => purpose,
            Err(error) => {
                context.error = Some(BindError::Scan(error));
                return;
            }
        };
        if purpose == Some(crate::customscan::ScanPurpose::Modify) {
            let relation = unsafe { (*custom).ss.ss_currentRelation };
            if relation.is_null() {
                context.error =
                    Some(BindError::Execution(PgReportError::from_message(
                        pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "Modify CustomScan has no open relation",
                    )));
                return;
            }
            let oid = unsafe { (*relation).rd_id };
            let scan_context = unsafe {
                crate::customscan::state::CustomScanStateWrapper::<P>::from_node_ptr(
                    custom,
                )
            }
            .active_provider_state_mut()
            .and_then(|state| P::modify_scan_context(state));
            let Some(scan_context) = scan_context else {
                context.error =
                    Some(BindError::Execution(PgReportError::from_message(
                        pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "Modify CustomScan provider state is not active",
                    )));
                return;
            };
            let binding = match unsafe {
                context
                    .execution
                    .with_mut(|execution| execution.bind_scan(oid, scan_context))
            } {
                Ok(binding) => binding,
                Err(error) => {
                    context.error = Some(BindError::Execution(error));
                    return;
                }
            };
            if let Err(error) = unsafe {
                crate::customscan::exec::bind_modify_scan::<P>(custom, binding)
            } {
                context.error = Some(BindError::Scan(error));
                return;
            }
            context.bound_scans += 1;
        } else if unsafe { scan_rti(plan_state) }
            .is_some_and(|rti| context.target_rtis.contains(&rti))
            && unsafe { is_provider_relation::<P>((*custom).ss.ss_currentRelation) }
        {
            context.error = Some(BindError::Execution(PgReportError::from_message(
                pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "required Modify relation was planned with an unbindable CustomScan",
            )));
            return;
        }
    } else if unsafe { scan_rti(plan_state) }
        .is_some_and(|rti| context.target_rtis.contains(&rti))
        && unsafe { is_provider_relation::<P>(scan_state_relation(plan_state)) }
    {
        context.error = Some(BindError::Execution(PgReportError::from_message(
            pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "required Modify relation was planned with a standard scan",
        )));
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
    let context = unsafe { &mut *raw_context.cast::<BindContext<'_, P>>() };
    unsafe { bind_tree::<P>(plan_state, context) };
    context.error.is_some()
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
    let state = unsafe { state::<P>(node) };
    if state.phase != ModifyNodePhase::Created {
        pgrx::error!("LakebaseModifyTable was initialized more than once");
    }
    let plan = unsafe { (*node).ss.ps.plan.cast::<pg_sys::CustomScan>() };
    let inner_plan =
        unsafe { pg_sys::list_nth((*plan).custom_plans, 0).cast::<pg_sys::Plan>() };
    let inner_state = unsafe { pg_sys::ExecInitNode(inner_plan, estate, eflags) };
    if inner_state.is_null()
        || unsafe { (*inner_state).type_ } != pg_sys::NodeTag::T_ModifyTableState
    {
        pgrx::error!("LakebaseModifyTable child is not ModifyTableState");
    }
    state.inner = inner_state.cast();
    unsafe {
        (*node).custom_ps = pg_sys::lappend(std::ptr::null_mut(), inner_state.cast());
    }
    state.phase = ModifyNodePhase::Begun;

    if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return;
    }

    let query_state =
        acquire_modify_query_state::<P::AccessMethod>(estate).report_unwrap();
    let execution = unsafe {
        ModifyNodeState::<P>::from_modify_table_state(state.inner, query_state)
    }
    .report_unwrap();
    let operation = unsafe { (*state.inner).operation };
    let wholerow_attno = if matches!(
        operation,
        pg_sys::CmdType::CMD_UPDATE
            | pg_sys::CmdType::CMD_DELETE
            | pg_sys::CmdType::CMD_MERGE
    ) {
        let subplan = unsafe { (*state.inner).ps.lefttree };
        let attno = find_wholerow_attno(unsafe { (*subplan).plan }).report_unwrap();
        if operation == pg_sys::CmdType::CMD_UPDATE && attno <= 0 {
            Err::<(), _>(PgReportError::from_message(
                pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "Lakebase UPDATE input is missing PostgreSQL wholerow",
            ))
            .report_unwrap();
        }
        attno
    } else {
        0
    };

    let execution = Rc::new(ModifyNodeCell::<P>::new(execution, wholerow_attno));
    let cleanup = Rc::clone(&execution);
    state.resource = Some(remember_resource(move || {
        // SAFETY: ResourceOwner cleanup runs after executor control has left
        // the mutation callback and is serialized in the backend thread.
        unsafe { cleanup.with_mut(ModifyNodeState::abort) };
    }));
    state.bridge = Some(execution.bridge());
    state.execution = Some(execution);

    let mut bind_context = BindContext {
        execution: state
            .execution
            .as_deref()
            .expect("execution was just installed"),
        error: None,
        bound_scans: 0,
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
    if let Some(error) = bind_context.error {
        match error {
            BindError::Execution(error) => Err::<(), _>(error).report_unwrap(),
            BindError::Scan(error) => Err::<(), _>(error).report_unwrap(),
        }
    }
    if matches!(
        operation,
        pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE
    ) && bind_context.bound_scans == 0
    {
        Err::<(), _>(PgReportError::from_message(
            pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "required Modify plan contains no bindable provider scan",
        ))
        .report_unwrap();
    }

    unsafe { replace_aux_entry(estate, state.inner, node) };
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
