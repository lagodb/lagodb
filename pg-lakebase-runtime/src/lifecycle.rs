use std::cell::RefCell;

use pgrx::pg_sys;

use crate::error::LakebaseResult;
use crate::runtime;

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkerActionKey {
    extension_oid: u32,
    worker_name: String,
}

impl WorkerActionKey {
    fn new(extension_oid: u32, worker_name: &str) -> Self {
        Self {
            extension_oid,
            worker_name: worker_name.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
enum PendingActionKind {
    ReserveRegistration(runtime::RegistrationReservation),
    Wake(WorkerActionKey),
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
    // the postmaster/backend lifetime because pg_lakebase_runtime is preloaded.
    unsafe {
        pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(subxact_callback), std::ptr::null_mut());
    }
}

pub(crate) fn reserve_registration(
    extension_oid: u32,
    worker_name: &str,
) -> LakebaseResult<()> {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let reservation =
        runtime::reserve_registration(database_oid, extension_oid, worker_name)?;
    push(
        database_oid,
        PendingActionKind::ReserveRegistration(reservation),
    );
    Ok(())
}

pub(crate) fn request_wakeup(extension_oid: u32, worker_name: &str) {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    push(
        database_oid,
        PendingActionKind::Wake(WorkerActionKey::new(extension_oid, worker_name)),
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
    push(0, PendingActionKind::RescanAll);
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
        let actions = std::mem::take(&mut self.actions);
        let mut needs_signal = false;
        for action in actions {
            needs_signal |= action.apply(committed);
        }
        needs_signal
    }

    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    fn abort_subtransaction(&mut self, level: i32) -> bool {
        let aborted: Vec<_> = self
            .actions
            .extract_if(.., |action| action.nest_level >= level)
            .collect();
        let parent_level = level.saturating_sub(1);
        let mut needs_signal = false;
        for action in aborted {
            match action.kind {
                PendingActionKind::ReserveRegistration(reservation) => {
                    needs_signal |= runtime::finish_registration(
                        reservation,
                        runtime::RegistrationCompletion::Abort,
                    );
                }
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
                    self.record(0, parent_level, PendingActionKind::RescanAll);
                }
                PendingActionKind::Wake(_) => {}
            }
        }
        needs_signal
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
            PendingActionKind::ReserveRegistration(reservation) => {
                runtime::finish_registration(
                    reservation,
                    if committed {
                        runtime::RegistrationCompletion::Commit
                    } else {
                        runtime::RegistrationCompletion::Abort
                    },
                )
            }
            PendingActionKind::Wake(worker) if committed => {
                runtime::wake_worker(
                    self.database_oid,
                    worker.extension_oid,
                    &worker.worker_name,
                )
            }
            PendingActionKind::Wake(_) => false,
            PendingActionKind::ReconcileDatabase => {
                runtime::request_database_reconcile(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers if committed => {
                runtime::wake_database_workers(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers => false,
            PendingActionKind::DropDatabase if committed => {
                runtime::request_full_rescan()
            }
            PendingActionKind::DropDatabase => {
                runtime::request_database_reconcile(self.database_oid)
                    | runtime::request_full_rescan()
            }
            PendingActionKind::RescanAll => runtime::request_full_rescan(),
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
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::XactEvent::*;

    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => apply(true),
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => apply(false),
        XACT_EVENT_PRE_PREPARE
            if !ACTIONS.with(|actions| actions.borrow().is_empty()) =>
        {
            crate::error::LakebaseError::PreparedTransactionWithRuntimeActions
                .report();
        }
        _ => {}
    }
}

fn apply(committed: bool) {
    if ACTIONS.with(|actions| actions.borrow_mut().apply(committed)) {
        runtime::signal_launcher();
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::SubXactEvent::*;

    let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    ACTIONS.with(|actions| match event {
        SUBXACT_EVENT_ABORT_SUB => {
            if actions.borrow_mut().abort_subtransaction(level) {
                runtime::signal_launcher();
            }
        }
        SUBXACT_EVENT_COMMIT_SUB => {
            actions.borrow_mut().commit_subtransaction(level);
        }
        _ => {}
    });
}
