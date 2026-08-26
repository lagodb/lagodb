use std::cell::RefCell;
use std::ffi::c_void;
use std::mem;
use std::ptr::null_mut;

use pgrx::pg_sys;

use crate::worker::{self, INVALID_OID, WorkerKey};

#[derive(Clone, Debug)]
enum PendingActionKind {
    Wake(WorkerKey),
    ReconcileDatabase,
    WakeDatabaseWorkers,
    DropDatabase,
    RescanAll,
}

#[derive(Clone, Debug)]
struct PendingAction {
    database_oid: u32,
    nest_level: i32,
    kind: PendingActionKind,
}

thread_local! {
    static ACTIONS: RefCell<PendingActions> = const { RefCell::new(PendingActions::new()) };
}

pub(crate) fn init() {
    // SAFETY: callbacks have PostgreSQL's required ABI and remain loaded for
    // the postmaster/backend lifetime because lagodb_base is preloaded.
    unsafe {
        pg_sys::RegisterXactCallback(Some(xact_callback), null_mut());
        pg_sys::RegisterSubXactCallback(Some(subxact_callback), null_mut());
    }
}

pub(crate) fn request_wakeup(worker_id: i32) {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    push(
        database_oid,
        PendingActionKind::Wake(WorkerKey::new(database_oid, worker_id)),
    );
}

pub(crate) fn request_database_reconcile() {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    push(database_oid, PendingActionKind::ReconcileDatabase);
}

pub(crate) fn request_database_workers_wakeup(database_oid: u32) {
    push(database_oid, PendingActionKind::WakeDatabaseWorkers);
}

pub(crate) fn request_database_drop(database_oid: u32) {
    push(database_oid, PendingActionKind::DropDatabase);
}

pub(crate) fn request_global_reconcile() {
    push(INVALID_OID, PendingActionKind::RescanAll);
}

fn push(database_oid: u32, kind: PendingActionKind) {
    let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    ACTIONS.with(|actions| {
        actions.borrow_mut().record(database_oid, nest_level, kind);
    });
}

struct PendingActions {
    actions: Vec<PendingAction>,
}

impl PendingActions {
    const fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    fn record(
        &mut self,
        database_oid: u32,
        nest_level: i32,
        kind: PendingActionKind,
    ) {
        if self.actions.iter().any(|action| {
            action.database_oid == database_oid
                && action.nest_level == nest_level
                && same_action(&action.kind, &kind)
        }) {
            return;
        }
        self.actions.push(PendingAction {
            database_oid,
            nest_level,
            kind,
        });
    }

    fn apply(&mut self, committed: bool) -> bool {
        let actions = mem::take(&mut self.actions);
        let mut needs_supervisor_wake = false;
        for action in actions {
            needs_supervisor_wake |= action.apply(committed);
        }
        needs_supervisor_wake
    }

    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    fn abort_subtransaction(&mut self, level: i32) {
        let aborted: Vec<_> = self
            .actions
            .extract_if(.., |action| action.nest_level >= level)
            .collect();
        let parent_level = level.saturating_sub(1);
        for action in aborted {
            match action.kind {
                PendingActionKind::ReconcileDatabase => {
                    self.record(
                        action.database_oid,
                        parent_level,
                        PendingActionKind::ReconcileDatabase,
                    );
                }
                PendingActionKind::WakeDatabaseWorkers => {}
                PendingActionKind::RescanAll => {
                    self.record(
                        action.database_oid,
                        parent_level,
                        PendingActionKind::RescanAll,
                    );
                }
                PendingActionKind::DropDatabase => {
                    self.record(
                        action.database_oid,
                        parent_level,
                        PendingActionKind::ReconcileDatabase,
                    );
                    self.record(
                        INVALID_OID,
                        parent_level,
                        PendingActionKind::RescanAll,
                    );
                }
                PendingActionKind::Wake(_) => {}
            }
        }
    }

    fn commit_subtransaction(&mut self, level: i32) {
        let parent_level = level.saturating_sub(1);
        for action in self
            .actions
            .iter_mut()
            .filter(|action| action.nest_level >= level)
        {
            action.nest_level = parent_level;
        }
    }
}

impl PendingAction {
    fn apply(self, committed: bool) -> bool {
        match self.kind {
            PendingActionKind::Wake(worker) if committed => {
                worker::wake_worker(worker)
            }
            PendingActionKind::Wake(_) => false,
            PendingActionKind::ReconcileDatabase => {
                worker::request_database_reconcile(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers if committed => {
                worker::wake_database_workers(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers => false,
            PendingActionKind::DropDatabase if committed => {
                worker::request_full_rescan()
            }
            PendingActionKind::DropDatabase => {
                worker::request_database_reconcile(self.database_oid)
                    | worker::request_full_rescan()
            }
            PendingActionKind::RescanAll => worker::request_full_rescan(),
        }
    }
}

fn same_action(left: &PendingActionKind, right: &PendingActionKind) -> bool {
    match (left, right) {
        (PendingActionKind::Wake(a), PendingActionKind::Wake(b)) => a == b,
        (
            PendingActionKind::ReconcileDatabase,
            PendingActionKind::ReconcileDatabase,
        )
        | (
            PendingActionKind::WakeDatabaseWorkers,
            PendingActionKind::WakeDatabaseWorkers,
        )
        | (PendingActionKind::DropDatabase, PendingActionKind::DropDatabase)
        | (PendingActionKind::RescanAll, PendingActionKind::RescanAll) => true,
        _ => false,
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    use pg_sys::XactEvent::*;

    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => apply(true),
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => apply(false),
        XACT_EVENT_PRE_PREPARE
            if !ACTIONS.with(|actions| actions.borrow().is_empty()) =>
        {
            crate::error::LagodbError::PreparedTransactionWithRuntimeActions.report();
        }
        _ => {}
    }
}

fn apply(committed: bool) {
    if ACTIONS.with(|actions| actions.borrow_mut().apply(committed)) {
        worker::signal_supervisor();
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut c_void,
) {
    use pg_sys::SubXactEvent::*;

    let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    ACTIONS.with(|actions| match event {
        SUBXACT_EVENT_ABORT_SUB => {
            actions.borrow_mut().abort_subtransaction(level);
        }
        SUBXACT_EVENT_COMMIT_SUB => {
            actions.borrow_mut().commit_subtransaction(level);
        }
        _ => {}
    });
}
