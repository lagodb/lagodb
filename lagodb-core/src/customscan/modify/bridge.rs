use std::cell::UnsafeCell;
use std::ffi::c_void;

use crate::api::{
    AmResult, MutationDeleteContext, MutationOutcome, MutationUpdateContext,
    MutationWriteContext,
};
use crate::customscan::modify::LagodbCustomModifyProvider;
use crate::diag::{PgReportError, ReportableError};
use crate::handles::{ItemPointer, SnapshotHandle};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

use super::execution::{ModifyNodeState, ResultRelationState};
use super::planning::WHOLEROW_NAME;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) enum LagodbMutationOutcome {
    Applied = 0,
    SelfModified = 1,
    Deleted = 2,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct LagodbMutationResult {
    pub outcome: LagodbMutationOutcome,
    pub modifying_cid: pg_sys::CommandId,
}

#[repr(C)]
pub(super) struct LagodbPreparedUpdateTriggerRows {
    pub old_tid: pg_sys::ItemPointerData,
    pub new_tid: pg_sys::ItemPointerData,
}

#[repr(C)]
pub(super) struct LagodbModifyBridge {
    pub state: *mut c_void,
    pub postgres_indexes: bool,
    pub resolve_relation: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::ResultRelInfo,
    ) -> *mut c_void,
    pub wholerow_attno:
        unsafe extern "C-unwind" fn(*mut c_void) -> pg_sys::AttrNumber,
    pub insert: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::TupleTableSlot,
        pg_sys::CommandId,
        i32,
    ),
    pub preserve_trigger_row: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::TupleTableSlot,
        *mut pg_sys::ItemPointerData,
    ),
    pub prepare_update_trigger_rows: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::ResultRelInfo,
        *mut pg_sys::ResultRelInfo,
        *mut pg_sys::TupleTableSlot,
        *mut pg_sys::TupleTableSlot,
        *mut LagodbPreparedUpdateTriggerRows,
    ),
    pub update: unsafe extern "C-unwind" fn(
        *mut c_void,
        *const pg_sys::ItemPointerData,
        *mut pg_sys::TupleTableSlot,
        *mut pg_sys::TupleTableSlot,
        pg_sys::CommandId,
        pg_sys::Snapshot,
        pg_sys::Snapshot,
        bool,
    ) -> LagodbMutationResult,
    pub delete_: unsafe extern "C-unwind" fn(
        *mut c_void,
        *const pg_sys::ItemPointerData,
        pg_sys::CommandId,
        pg_sys::Snapshot,
        pg_sys::Snapshot,
        bool,
        bool,
    ) -> LagodbMutationResult,
}

/// Single-threaded executor owner shared only with ResourceOwner cleanup.
///
/// PostgreSQL completes one storage callback before triggers can recursively
/// enter another ModifyTable node. Nested SPI therefore owns another
/// `ModifyNodeCell<P>`; no two mutable accesses to this cell overlap.
pub(super) struct ModifyNodeCell<P: LagodbCustomModifyProvider> {
    inner: UnsafeCell<ModifyNodeState<P>>,
    wholerow_attno: pg_sys::AttrNumber,
}

impl<P: LagodbCustomModifyProvider> ModifyNodeCell<P> {
    /// Build the cell and derive the whole-row junk attribute from the live
    /// child plan used by UPDATE/DELETE/MERGE.
    ///
    /// # Safety
    ///
    /// For `Some(input_plan)`, the pointer must be the initialized child Plan
    /// owned by the wrapped ModifyTable state. `None` is used for INSERT,
    /// which does not consume a whole-row junk attribute.
    pub(super) unsafe fn new(
        execution: ModifyNodeState<P>,
        operation: pg_sys::CmdType::Type,
        input_plan: Option<*mut pg_sys::Plan>,
    ) -> AmResult<Self> {
        let wholerow_attno = if matches!(
            operation,
            pg_sys::CmdType::CMD_UPDATE
                | pg_sys::CmdType::CMD_DELETE
                | pg_sys::CmdType::CMD_MERGE
        ) {
            let plan = input_plan.ok_or_else(|| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "ModifyTable input plan is missing",
                )
            })?;
            let tlist = unsafe { (*plan).targetlist };
            let wholerow = unsafe {
                pg_sys::ExecFindJunkAttributeInTlist(tlist, WHOLEROW_NAME.as_ptr())
            };
            if operation == pg_sys::CmdType::CMD_UPDATE && wholerow <= 0 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "LagoDB UPDATE input is missing PostgreSQL wholerow",
                ));
            }
            wholerow.max(0)
        } else {
            0
        };
        Ok(Self {
            inner: UnsafeCell::new(execution),
            wholerow_attno,
        })
    }

    /// # Safety
    ///
    /// The caller must uphold PostgreSQL's single-threaded, non-reentrant
    /// mutation callback contract for this ModifyTable execution.
    pub unsafe fn with_mut<R>(
        &self,
        operation: impl FnOnce(&mut ModifyNodeState<P>) -> R,
    ) -> R {
        operation(unsafe { &mut *self.inner.get() })
    }

    pub fn bridge(&self) -> LagodbModifyBridge {
        LagodbModifyBridge {
            state: std::ptr::from_ref(self).cast_mut().cast(),
            postgres_indexes: P::MODIFY_CAPABILITIES.postgres_indexes(),
            resolve_relation: resolve_relation::<P>,
            wholerow_attno: wholerow_attno::<P>,
            insert: insert::<P>,
            preserve_trigger_row: preserve_trigger_row::<P>,
            prepare_update_trigger_rows: prepare_update_trigger_rows::<P>,
            update: update::<P>,
            delete_: delete_::<P>,
        }
    }
}

/// PostgreSQL's modify-table C bridge supplies a live slot and row identity
/// for trigger-row preservation; the outer callback reports the Rust result.
#[pg_guard]
unsafe extern "C-unwind" fn preserve_trigger_row<P: LagodbCustomModifyProvider>(
    relation: *mut c_void,
    slot: *mut pg_sys::TupleTableSlot,
    row_id: *mut pg_sys::ItemPointerData,
) {
    let relation = relation.cast::<ResultRelationState<P>>();
    let preserved =
        unsafe { (&mut *relation).preserve_trigger_row(slot) }.report_unwrap();
    unsafe { preserved.write_to_raw(row_id) };
}

/// PostgreSQL passes live trigger metadata and output storage from the active
/// ModifyTable callback. The slot for each provider-owned side is non-NULL;
/// the OLD slot is intentionally NULL when the source side remains native.
/// The outer callback reports the Rust result.
#[pg_guard]
unsafe extern "C-unwind" fn prepare_update_trigger_rows<
    P: LagodbCustomModifyProvider,
>(
    state: *mut c_void,
    source_info: *mut pg_sys::ResultRelInfo,
    destination_info: *mut pg_sys::ResultRelInfo,
    old_slot: *mut pg_sys::TupleTableSlot,
    new_slot: *mut pg_sys::TupleTableSlot,
    prepared: *mut LagodbPreparedUpdateTriggerRows,
) {
    let cell = unsafe { cell::<P>(state) };
    let rows = unsafe {
        cell.with_mut(|execution| {
            execution.prepare_update_trigger_rows(
                source_info,
                destination_info,
                old_slot,
                new_slot,
            )
        })
    }
    .report_unwrap();
    if let Some(old_tid) = rows.old_tid {
        unsafe { old_tid.write_to_raw(&raw mut (*prepared).old_tid) };
    }
    if let Some(new_tid) = rows.new_tid {
        unsafe { new_tid.write_to_raw(&raw mut (*prepared).new_tid) };
    }
}

unsafe fn cell<'a, P: LagodbCustomModifyProvider>(
    state: *mut c_void,
) -> &'a ModifyNodeCell<P> {
    // SAFETY: state is created by `ModifyNodeCell<P>::bridge` and the owning Rc is
    // held by both the CustomScan state and ResourceOwner for the call.
    unsafe { &*state.cast::<ModifyNodeCell<P>>() }
}

#[pg_guard]
unsafe extern "C-unwind" fn resolve_relation<P: LagodbCustomModifyProvider>(
    state: *mut c_void,
    result_rel_info: *mut pg_sys::ResultRelInfo,
) -> *mut c_void {
    let cell = unsafe { cell::<P>(state) };
    // SAFETY: executor callbacks are serialized and resolution is the only
    // operation that mutates the relation ownership maps.
    unsafe { cell.with_mut(|execution| execution.resolve_relation(result_rel_info)) }
        .report_unwrap()
        .map_or(std::ptr::null_mut(), |relation| relation.as_ptr().cast())
}

#[pg_guard]
unsafe extern "C-unwind" fn wholerow_attno<P: LagodbCustomModifyProvider>(
    state: *mut c_void,
) -> pg_sys::AttrNumber {
    let cell = unsafe { cell::<P>(state) };
    cell.wholerow_attno
}

/// PostgreSQL supplies a live relation state and insert slot for this
/// ModifyTable callback; the outer callback reports the Rust result.
#[pg_guard]
unsafe extern "C-unwind" fn insert<P: LagodbCustomModifyProvider>(
    relation: *mut c_void,
    new_slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: i32,
) {
    let relation = relation.cast::<ResultRelationState<P>>();
    unsafe {
        (&mut *relation).insert(new_slot, MutationWriteContext { cid, options })
    }
    .report_unwrap();
}

fn map_outcome(outcome: MutationOutcome) -> LagodbMutationResult {
    match outcome {
        MutationOutcome::Applied => LagodbMutationResult {
            outcome: LagodbMutationOutcome::Applied,
            modifying_cid: 0,
        },
        MutationOutcome::AlreadyModifiedInCurrentTransaction {
            modifying_command_id,
        } => LagodbMutationResult {
            outcome: LagodbMutationOutcome::SelfModified,
            modifying_cid: modifying_command_id,
        },
        MutationOutcome::Deleted => LagodbMutationResult {
            outcome: LagodbMutationOutcome::Deleted,
            modifying_cid: 0,
        },
    }
}

/// PostgreSQL's heap mutation callback guarantees a live row identity and
/// slots; an optional cross-check snapshot remains nullable by API contract.
#[pg_guard]
#[allow(clippy::too_many_arguments)] // C ABI mirrors PostgreSQL's callback context.
unsafe extern "C-unwind" fn update<P: LagodbCustomModifyProvider>(
    relation: *mut c_void,
    tuple_id: *const pg_sys::ItemPointerData,
    old_slot: *mut pg_sys::TupleTableSlot,
    new_slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
) -> LagodbMutationResult {
    let tuple_id = unsafe { ItemPointer::from_raw(tuple_id.cast_mut()) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };
    let crosscheck_handle = (!crosscheck.is_null())
        .then(|| unsafe { SnapshotHandle::from_raw(crosscheck) });
    let context = MutationUpdateContext {
        cid,
        snapshot: &snapshot_handle,
        crosscheck: crosscheck_handle.as_ref(),
        wait,
    };
    let relation = relation.cast::<ResultRelationState<P>>();
    let outcome =
        unsafe { (&mut *relation).update(tuple_id, old_slot, new_slot, context) }
            .report_unwrap();
    map_outcome(outcome)
}

/// PostgreSQL's heap mutation callback guarantees a live row identity; an
/// optional cross-check snapshot remains nullable by API contract.
#[pg_guard]
#[allow(clippy::too_many_arguments)] // C ABI mirrors PostgreSQL's callback context.
unsafe extern "C-unwind" fn delete_<P: LagodbCustomModifyProvider>(
    relation: *mut c_void,
    tuple_id: *const pg_sys::ItemPointerData,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
    changing_partition: bool,
) -> LagodbMutationResult {
    let tuple_id = unsafe { ItemPointer::from_raw(tuple_id.cast_mut()) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };
    let crosscheck_handle = (!crosscheck.is_null())
        .then(|| unsafe { SnapshotHandle::from_raw(crosscheck) });
    let context = MutationDeleteContext {
        cid,
        snapshot: &snapshot_handle,
        crosscheck: crosscheck_handle.as_ref(),
        wait,
        changing_partition,
    };
    let relation = relation.cast::<ResultRelationState<P>>();
    let outcome =
        unsafe { (&mut *relation).delete(tuple_id, context) }.report_unwrap();
    map_outcome(outcome)
}
