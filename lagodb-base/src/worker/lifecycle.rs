use std::cell::RefCell;
use std::ffi::{CStr, c_void};
use std::mem;
use std::ptr::null_mut;

use pgrx::pg_sys;

use super::lock::DatabaseLifecycleLock;
use super::{INVALID_OID, WorkerError, WorkerKey, registry};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

pub(super) fn stage_worker_wakeup(worker_id: i32) {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    push(
        database_oid,
        PendingActionKind::Wake(WorkerKey::new(database_oid, worker_id)),
    );
}

pub(super) fn stage_database_reconcile() {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    push(database_oid, PendingActionKind::ReconcileDatabase);
}

fn stage_database_workers_wakeup(database_oid: u32) {
    push(database_oid, PendingActionKind::WakeDatabaseWorkers);
}

fn stage_database_drop(database_oid: u32) {
    push(database_oid, PendingActionKind::DropDatabase);
}

fn stage_global_reconcile() {
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
                && action.kind == kind
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
                super::wake_worker(worker)
            }
            PendingActionKind::Wake(_) => false,
            PendingActionKind::ReconcileDatabase => {
                super::request_database_reconcile(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers if committed => {
                super::wake_database_workers(self.database_oid)
            }
            PendingActionKind::WakeDatabaseWorkers => false,
            PendingActionKind::DropDatabase if committed => {
                super::request_full_rescan()
            }
            PendingActionKind::DropDatabase => {
                super::request_database_reconcile(self.database_oid)
                    | super::request_full_rescan()
            }
            PendingActionKind::RescanAll => super::request_full_rescan(),
        }
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
            WorkerError::PreparedTransactionWithRuntimeActions.report();
        }
        _ => {}
    }
}

fn apply(committed: bool) {
    if ACTIONS.with(|actions| actions.borrow_mut().apply(committed)) {
        super::signal_supervisor();
    }
}

pub(crate) unsafe fn preflight(node: *mut pg_sys::Node) {
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_DropdbStmt => {
            let statement = node.cast::<pg_sys::DropdbStmt>();
            let database_oid =
                unsafe { pg_sys::get_database_oid((*statement).dbname, true) };
            if database_oid != pg_sys::InvalidOid {
                DatabaseLifecycleLock::new(database_oid.to_u32()).acquire_drop();
                stage_database_drop(database_oid.to_u32());
                super::prepare_database_drop(database_oid.to_u32());
            }
        }
        pg_sys::NodeTag::T_CreatedbStmt => stage_global_reconcile(),
        pg_sys::NodeTag::T_AlterDatabaseSetStmt => {
            let statement = unsafe { &*node.cast::<pg_sys::AlterDatabaseSetStmt>() };
            let database_oid =
                unsafe { pg_sys::get_database_oid(statement.dbname, false) };
            stage_database_workers_wakeup(database_oid.to_u32());
        }
        _ => {}
    }
}

pub(crate) fn drop_extension_workers(
    extension_oid: pg_sys::Oid,
) -> Result<(), WorkerError> {
    // SAFETY: PostgreSQL invokes OAT_DROP before deleting the pg_extension
    // catalog tuple, so the extension OID is still resolvable here.
    let extension_name = unsafe { pg_sys::get_extension_name(extension_oid) };
    if extension_name.is_null() {
        return Ok(());
    }
    // SAFETY: PostgreSQL returned a non-null NUL-terminated extension name.
    let extension_name = unsafe { CStr::from_ptr(extension_name) };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let runtime_oid =
        unsafe { pg_sys::get_extension_oid(c"lagodb_base".as_ptr(), true) };
    if runtime_oid == pg_sys::InvalidOid {
        return Ok(());
    }
    if extension_oid == runtime_oid {
        DatabaseLifecycleLock::new(database_oid).acquire_drop();
        stage_database_reconcile();
        super::prepare_database_drop(database_oid);
    } else {
        let has_registrations =
            registry::extension_has_registrations(extension_name)?;
        if !has_registrations {
            return Ok(());
        }
        registry::delete_extension_registrations(extension_name)?;
        DatabaseLifecycleLock::new(database_oid).acquire_drop();
        stage_database_reconcile();
        super::prepare_extension_drop(database_oid, extension_oid.to_u32());
    }
    Ok(())
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
