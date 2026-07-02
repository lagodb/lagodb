use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::ptr::NonNull;

use crate::api::{
    MutationDeleteContext, MutationOutcome, MutationUpdateContext,
    MutationWriteContext,
};
use crate::customscan::provider::LakebaseCustomModifyProvider;
use crate::diag::ReportableError;
use crate::handles::{ItemPointer, SnapshotHandle};
use pgrx::{pg_guard, pg_sys};

use super::execution::{ModifyNodeState, ResultRelationState};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) enum LakebaseMutationOutcome {
    Applied = 0,
    SelfModified = 1,
    Deleted = 2,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct LakebaseMutationResult {
    pub outcome: LakebaseMutationOutcome,
    pub modifying_cid: pg_sys::CommandId,
}

#[repr(C)]
pub(super) struct LakebasePreparedUpdateTriggerRows {
    pub old_tid: pg_sys::ItemPointerData,
    pub new_tid: pg_sys::ItemPointerData,
}

#[repr(C)]
pub(super) struct LakebaseModifyBridge {
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
        *mut LakebasePreparedUpdateTriggerRows,
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
    ) -> LakebaseMutationResult,
    pub delete_: unsafe extern "C-unwind" fn(
        *mut c_void,
        *const pg_sys::ItemPointerData,
        pg_sys::CommandId,
        pg_sys::Snapshot,
        pg_sys::Snapshot,
        bool,
        bool,
    ) -> LakebaseMutationResult,
}

/// Single-threaded executor owner shared only with ResourceOwner cleanup.
///
/// PostgreSQL completes one storage callback before triggers can recursively
/// enter another ModifyTable node. Nested SPI therefore owns another
/// `ModifyNodeCell<P>`; no two mutable accesses to this cell overlap.
pub(super) struct ModifyNodeCell<P: LakebaseCustomModifyProvider> {
    inner: UnsafeCell<ModifyNodeState<P>>,
    wholerow_attno: pg_sys::AttrNumber,
}

impl<P: LakebaseCustomModifyProvider> ModifyNodeCell<P> {
    pub fn new(
        execution: ModifyNodeState<P>,
        wholerow_attno: pg_sys::AttrNumber,
    ) -> Self {
        Self {
            inner: UnsafeCell::new(execution),
            wholerow_attno,
        }
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

    pub fn bridge(&self) -> LakebaseModifyBridge {
        LakebaseModifyBridge {
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

#[pg_guard]
unsafe extern "C-unwind" fn preserve_trigger_row<P: LakebaseCustomModifyProvider>(
    relation: *mut c_void,
    slot: *mut pg_sys::TupleTableSlot,
    row_id: *mut pg_sys::ItemPointerData,
) {
    if slot.is_null() || row_id.is_null() {
        pgrx::error!("Lakebase trigger-row preservation received NULL output");
    }
    let relation = NonNull::new(relation)
        .expect("resolved Lakebase relation state is non-NULL")
        .cast::<ResultRelationState<P>>();
    let preserved = unsafe { (&mut *relation.as_ptr()).preserve_trigger_row(slot) }
        .report_unwrap();
    unsafe { preserved.write_to_raw(row_id) };
}

#[pg_guard]
unsafe extern "C-unwind" fn prepare_update_trigger_rows<
    P: LakebaseCustomModifyProvider,
>(
    state: *mut c_void,
    source_info: *mut pg_sys::ResultRelInfo,
    destination_info: *mut pg_sys::ResultRelInfo,
    old_slot: *mut pg_sys::TupleTableSlot,
    new_slot: *mut pg_sys::TupleTableSlot,
    prepared: *mut LakebasePreparedUpdateTriggerRows,
) {
    if source_info.is_null() || destination_info.is_null() || prepared.is_null() {
        pgrx::error!("Lakebase UPDATE trigger preparation received NULL metadata");
    }
    let cell = unsafe { cell::<P>(state) };
    // SAFETY: executor callbacks are serialized. Slots and ResultRelInfos are
    // live for this call, and the returned identities are copied to C storage.
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

unsafe fn cell<'a, P: LakebaseCustomModifyProvider>(
    state: *mut c_void,
) -> &'a ModifyNodeCell<P> {
    // SAFETY: state is created by `ModifyNodeCell<P>::bridge` and the owning Rc is
    // held by both the CustomScan state and ResourceOwner for the call.
    unsafe { &*state.cast::<ModifyNodeCell<P>>() }
}

#[pg_guard]
unsafe extern "C-unwind" fn resolve_relation<P: LakebaseCustomModifyProvider>(
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
unsafe extern "C-unwind" fn wholerow_attno<P: LakebaseCustomModifyProvider>(
    state: *mut c_void,
) -> pg_sys::AttrNumber {
    let cell = unsafe { cell::<P>(state) };
    cell.wholerow_attno
}

#[pg_guard]
unsafe extern "C-unwind" fn insert<P: LakebaseCustomModifyProvider>(
    relation: *mut c_void,
    new_slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: i32,
) {
    let relation = NonNull::new(relation)
        .expect("resolved Lakebase relation state is non-NULL")
        .cast::<ResultRelationState<P>>();
    // SAFETY: the execution owns this boxed relation state and PostgreSQL
    // serializes callbacks for one ModifyTable node.
    unsafe {
        (&mut *relation.as_ptr())
            .insert(new_slot, MutationWriteContext { cid, options })
    }
    .report_unwrap();
}

fn map_outcome(outcome: MutationOutcome) -> LakebaseMutationResult {
    match outcome {
        MutationOutcome::Applied => LakebaseMutationResult {
            outcome: LakebaseMutationOutcome::Applied,
            modifying_cid: 0,
        },
        MutationOutcome::AlreadyModifiedInCurrentTransaction {
            modifying_command_id,
        } => LakebaseMutationResult {
            outcome: LakebaseMutationOutcome::SelfModified,
            modifying_cid: modifying_command_id,
        },
        MutationOutcome::Deleted => LakebaseMutationResult {
            outcome: LakebaseMutationOutcome::Deleted,
            modifying_cid: 0,
        },
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // C ABI mirrors PostgreSQL's callback context.
unsafe extern "C-unwind" fn update<P: LakebaseCustomModifyProvider>(
    relation: *mut c_void,
    tuple_id: *const pg_sys::ItemPointerData,
    old_slot: *mut pg_sys::TupleTableSlot,
    new_slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
) -> LakebaseMutationResult {
    if tuple_id.is_null() {
        pgrx::error!("Lakebase UPDATE received a NULL row identity");
    }
    // SAFETY: C supplies a callback-scoped ItemPointerData and Rust copies it.
    let tuple_id = unsafe { ItemPointer::from_raw(tuple_id.cast_mut()) };
    // SAFETY: PG supplies live snapshots for the duration of this callback.
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };
    let crosscheck_handle = (!crosscheck.is_null())
        .then(|| unsafe { SnapshotHandle::from_raw(crosscheck) });
    let context = MutationUpdateContext {
        cid,
        snapshot: &snapshot_handle,
        crosscheck: crosscheck_handle.as_ref(),
        wait,
    };
    let relation = NonNull::new(relation)
        .expect("resolved Lakebase relation state is non-NULL")
        .cast::<ResultRelationState<P>>();
    let outcome = unsafe {
        (&mut *relation.as_ptr()).update(tuple_id, old_slot, new_slot, context)
    }
    .report_unwrap();
    map_outcome(outcome)
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // C ABI mirrors PostgreSQL's callback context.
unsafe extern "C-unwind" fn delete_<P: LakebaseCustomModifyProvider>(
    relation: *mut c_void,
    tuple_id: *const pg_sys::ItemPointerData,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
    changing_partition: bool,
) -> LakebaseMutationResult {
    if tuple_id.is_null() {
        pgrx::error!("Lakebase DELETE received a NULL row identity");
    }
    // SAFETY: C supplies a callback-scoped ItemPointerData and Rust copies it.
    let tuple_id = unsafe { ItemPointer::from_raw(tuple_id.cast_mut()) };
    // SAFETY: PG supplies live snapshots for the duration of this callback.
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
    let relation = NonNull::new(relation)
        .expect("resolved Lakebase relation state is non-NULL")
        .cast::<ResultRelationState<P>>();
    let outcome = unsafe { &mut *relation.as_ptr() }
        .delete(tuple_id, context)
        .report_unwrap();
    map_outcome(outcome)
}
