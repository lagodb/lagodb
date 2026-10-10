use std::ffi::c_void;
use std::ptr;
use std::rc::Rc;

use crate::access::mutation::acquire_modify_query_state;
use crate::api::TableAccessMethod;
use crate::customscan::error::CustomScanError;
use crate::customscan::modify::LagodbCustomModifyProvider;
use crate::diag::{PgReportError, ReportableError};
use crate::resource::{ResourceHandle, forget_resource, remember_resource};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{
    pg_guard,
    pg_sys::{self, ffi::pg_guard_ffi_boundary},
};

use super::bridge::{LagodbModifyBridge, ModifyNodeCell};
use super::execution::ModifyNodeState;
use super::methods;
use super::scan_binder::ModifyScanBinder;

unsafe extern "C-unwind" {
    unsafe fn lagodb_exec_init_modify_table(
        plan: *mut pg_sys::ModifyTable,
        estate: *mut pg_sys::EState,
        eflags: i32,
        provider_owns_partitioned_table: bool,
    ) -> *mut pg_sys::ModifyTableState;

    unsafe fn lagodb_exec_modify_table(
        mtstate: *mut pg_sys::ModifyTableState,
        bridge: *mut LagodbModifyBridge,
    ) -> *mut pg_sys::TupleTableSlot;

    unsafe fn lagodb_exec_end_modify_table(mtstate: *mut pg_sys::ModifyTableState);

    unsafe fn lagodb_exec_rescan_modify_table(mtstate: *mut pg_sys::ModifyTableState);
}

#[repr(C)]
struct LagodbModifyTableState<P: LagodbCustomModifyProvider> {
    /// PostgreSQL ABI prefix; must remain the first field.
    base: pg_sys::CustomScanState,
    /// Wrapped upstream ModifyTable executor state.
    inner: *mut pg_sys::ModifyTableState,
    /// Owner of relation-local AM ModifyState values.
    execution: Option<Rc<ModifyNodeCell<P>>>,
    /// Stable C ABI view into `execution`.
    bridge: Option<LagodbModifyBridge>,
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

impl<P: LagodbCustomModifyProvider> Drop for LagodbModifyTableState<P> {
    fn drop(&mut self) {
        if let Some(execution) = self.execution.as_ref() {
            // Normal ExecEnd clears execution. Reaching this fallback means
            // the query is being discarded, not just one shared node ending.
            // SAFETY: backend executor callbacks are serialized; Drop cannot
            // overlap a mutation callback for this state.
            unsafe { execution.with_mut(ModifyNodeState::cleanup_after_error) };
        }
        if let Some(resource) = self.resource.take() {
            let _ = forget_resource(resource);
        }
    }
}

unsafe fn state<'a, P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) -> &'a mut LagodbModifyTableState<P> {
    // SAFETY: allocated by `create_state`, repr(C), base at offset zero.
    unsafe { &mut *node.cast::<LagodbModifyTableState<P>>() }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn create_state<P: LagodbCustomModifyProvider>(
    _scan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let wrapper = LagodbModifyTableState::<P> {
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
pub(super) unsafe extern "C-unwind" fn begin<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) {
    unsafe { begin_impl::<P>(node, estate, eflags) }.report_unwrap();
}

/// Initialize the wrapped ModifyTable and bind all provider scans before the
/// executor enters mutation callbacks. The FFI wrapper above is the only
/// reporting boundary for initialization failures.
unsafe fn begin_impl<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) -> Result<(), CustomScanError> {
    let state = unsafe { state::<P>(node) };
    if state.phase != ModifyNodePhase::Created {
        return Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "LagoDBModifyTable was initialized more than once",
        )
        .into());
    }
    let plan = unsafe { (*node).ss.ps.plan.cast::<pg_sys::CustomScan>() };
    let inner_plan =
        unsafe { pg_sys::list_nth((*plan).custom_plans, 0).cast::<pg_sys::Plan>() };
    debug_assert_eq!(
        unsafe { (*inner_plan).type_ },
        pg_sys::NodeTag::T_ModifyTable
    );
    let modify_plan = inner_plan.cast::<pg_sys::ModifyTable>();
    let root_rti = if unsafe { (*modify_plan).rootRelation } > 0 {
        unsafe { (*modify_plan).rootRelation }
    } else {
        unsafe {
            pg_sys::list_nth_int((*modify_plan).resultRelations, 0) as pg_sys::Index
        }
    };
    // SAFETY: ModifyTable root/result RTIs are one-based indexes into the
    // executor range table established by InitPlan before this node begins.
    let root_rte = unsafe {
        pg_sys::list_nth(
            (*estate).es_range_table,
            i32::try_from(root_rti - 1)
                .expect("a PostgreSQL range-table index fits in i32"),
        )
        .cast::<pg_sys::RangeTblEntry>()
    };
    let provider_owned_partitioned_table = P::OWNS_PARTITIONED_TABLE
        && unsafe { (*root_rte).relkind } as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
        && P::AccessMethod::access_method_oid().is_some_and(|access_method| {
            access_method == unsafe { pg_sys::get_rel_relam((*root_rte).relid) }
        });
    let inner = unsafe {
        pg_guard_ffi_boundary(|| {
            lagodb_exec_init_modify_table(
                inner_plan.cast(),
                estate,
                eflags,
                provider_owned_partitioned_table,
            )
        })
    };
    let inner_state = inner.cast::<pg_sys::PlanState>();
    state.inner = inner;
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
        // PG releases spill files before this callback. Detach trigger rows
        // even for finished writes, before SPI later destroys executor memory.
        // SAFETY: ResourceOwner cleanup runs after executor control has left
        // the mutation callback and is serialized in the backend thread.
        unsafe { cleanup.with_mut(ModifyNodeState::cleanup_after_error) };
    }));
    let root_info = unsafe { (*state.inner).rootResultRelInfo };
    let root_relation = unsafe { (*root_info).ri_RelationDesc };
    debug_assert_eq!(
        provider_owned_partitioned_table,
        unsafe { ModifyScanBinder::<P>::owns_relation(root_relation) }
            && unsafe { (*(*root_relation).rd_rel).relkind } as u8
                == pg_sys::RELKIND_PARTITIONED_TABLE
    );
    let provider_owned_partitioned_table_state: *mut c_void =
        if provider_owned_partitioned_table {
            unsafe {
                execution.with_mut(|execution| execution.resolve_relation(root_info))
            }?
            .map_or(std::ptr::null_mut(), |relation| relation.as_ptr().cast())
        } else {
            std::ptr::null_mut()
        };
    if provider_owned_partitioned_table
        && provider_owned_partitioned_table_state.is_null()
    {
        return Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "provider-owned partitioned table has no mutation relation state",
        )
        .into());
    }
    state.bridge = Some(execution.bridge(
        provider_owned_partitioned_table,
        provider_owned_partitioned_table_state,
    ));
    state.execution = Some(execution);

    // A WHERE FALSE target can disappear completely; only surviving scans bind.
    unsafe {
        ModifyScanBinder::<P>::bind(
            state
                .execution
                .as_deref()
                .expect("execution was just installed"),
            plan,
            state.inner,
        )
    }?;

    unsafe { replace_aux_entry(estate, state.inner, node) };
    Ok(())
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn exec<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = unsafe { state::<P>(node) };
    let bridge = state
        .bridge
        .as_mut()
        .expect("BeginCustomScan initialized bridge");
    let result = unsafe {
        pg_guard_ffi_boundary(|| lagodb_exec_modify_table(state.inner, bridge))
    };
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
pub(super) unsafe extern "C-unwind" fn end<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) {
    let state = unsafe { state::<P>(node) };
    if state.phase == ModifyNodePhase::Ended {
        return;
    }
    if !state.inner.is_null() {
        // SAFETY: the matching C initializer owns the embedded node's layout
        // and the fork-specific cleanup, including standalone root slots.
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_exec_end_modify_table(state.inner))
        };
        state.inner = ptr::null_mut();
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
pub(super) unsafe extern "C-unwind" fn rescan<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
) {
    let state = unsafe { state::<P>(node) };
    if !state.inner.is_null() {
        // SAFETY: only the matching lifecycle adapter may operate this node.
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_exec_rescan_modify_table(state.inner))
        };
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
            c"PG17 LagoDB ModifyTable fork".as_ptr(),
            explain_state,
        );
    }
}
