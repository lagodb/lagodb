//! Owned cleanup transferred after transaction commit, executed after locks.

use std::cell::RefCell;
use std::ffi::c_void;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::ptr;

use pgrx::{PgTryBuilder, pg_guard, pg_sys};

use super::init_resource_manager;
use crate::diag::{PgErrorReport, report_warning};

thread_local! {
    static COMMITTED: RefCell<Vec<Box<dyn FnOnce()>>> = const { RefCell::new(Vec::new()) };
}

/// Bridges successful transaction cleanup to ResourceOwner's release phase.
///
/// Callers transfer work only from the top-level commit callback. Captured
/// state must be owned and independent of snapshots, relation handles and
/// other resources released by PostgreSQL before this phase.
/// Normal cleanup includes commits during backend exit, when PostgreSQL drops
/// temporary tables in a new transaction with proc_exit_inprogress already set.
pub struct CommittedCleanup;

impl CommittedCleanup {
    /// Registered once by the shared resource-manager initialization.
    pub(super) fn register_callback() {
        // SAFETY: PostgreSQL owns the callback registration for this backend;
        // the callback uses backend-local state and does not need an argument.
        unsafe {
            pg_sys::RegisterResourceReleaseCallback(
                Some(release_committed_cleanup_callback),
                ptr::null_mut(),
            );
        }
    }

    /// Transfer owned work from a top-level transaction commit callback.
    ///
    /// Providers that manage a transaction-wide collection can transfer it
    /// directly here. Individual pending deletes use the transaction cleanup
    /// adapter. Work executes after the top transaction owner releases locks;
    /// errors are reported as warnings at this ResourceOwner callback boundary.
    pub fn defer(cleanup: impl FnOnce() + 'static) {
        init_resource_manager();
        COMMITTED.with(|pending| pending.borrow_mut().push(Box::new(cleanup)));
    }

    /// Called only for the top transaction owner at AFTER_LOCKS.
    fn release(is_commit: bool) {
        // Detach before invoking provider code; no registry borrow crosses I/O.
        let pending = COMMITTED.with(|pending| mem::take(&mut *pending.borrow_mut()));
        if !is_commit {
            return;
        }
        for cleanup in pending {
            // As with owner fallback cleanup, an ERROR here cannot undo the
            // committed transaction. Report it at this core callback boundary
            // and continue with the remaining independent cleanup actions.
            PgTryBuilder::new(AssertUnwindSafe(cleanup))
                .catch_others(|error| {
                    report_warning(format_args!(
                        "error during committed cleanup after lock release: {}",
                        PgErrorReport::from_caught(error),
                    ));
                })
                .execute();
        }
    }
}

/// Normal committed cleanup has its own entry point so owner fallback's
/// proc_exit_inprogress policy cannot suppress temporary-table retirement.
#[pg_guard]
unsafe extern "C-unwind" fn release_committed_cleanup_callback(
    phase: pg_sys::ResourceReleasePhase::Type,
    is_commit: bool,
    is_top_level: bool,
    _arg: *mut c_void,
) {
    if phase != pg_sys::ResourceReleasePhase::RESOURCE_RELEASE_AFTER_LOCKS
        || !is_top_level
    {
        return;
    }

    // PG passes isTopLevel to child owners too. CurrentResourceOwner is set
    // to the owner being released, so only the actual top owner drains work.
    // SAFETY: PostgreSQL sets these backend-local globals during owner release.
    if unsafe { pg_sys::CurrentResourceOwner == pg_sys::TopTransactionResourceOwner }
    {
        CommittedCleanup::release(is_commit);
    }
}
