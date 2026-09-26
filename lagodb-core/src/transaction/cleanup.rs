//! Transaction-scoped cleanup helpers.
//!
//! This module builds on [`super::TransactionResource`] for common cleanup
//! patterns that several access methods may share.  It intentionally models
//! cleanup timing, not any concrete storage implementation or WAL policy.

use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::PgReportError;
use crate::resource::CommittedCleanup;

use super::{TransactionResource, TransactionResult, register_resource};

/// When a transaction-scoped cleanup action should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupTiming {
    /// Run after the top-level transaction commits and releases its locks.
    OnCommit,
    /// Run when the transaction or registering subtransaction aborts.
    OnAbort,
}

/// A pending delete action bound to transaction outcome.
///
/// Implementations own the concrete deletion policy: which storage API to use,
/// whether WAL is needed, and how to report best-effort failures.  The core
/// framework only handles transaction and subtransaction timing.
pub trait PendingDelete: Debug {
    /// Execute the delete operation.
    ///
    /// Commit cleanup runs after transaction locks and other owner resources
    /// are released. The action must own its storage capability and paths;
    /// it cannot depend on a snapshot, relation handle or executor borrow.
    fn execute(&self);

    /// The transaction outcome that should trigger this delete.
    ///
    /// Abort cleanup is the default because newly created staged objects most
    /// often need to be removed when their creating transaction fails.
    fn timing(&self) -> CleanupTiming {
        CleanupTiming::OnAbort
    }
}

/// Adapter that implements TransactionResource for a PendingDelete implementation.
#[derive(Debug)]
struct PendingDeleteResource {
    inner: RefCell<Option<Box<dyn PendingDelete>>>,
    timing: CleanupTiming,
    nest_level: Cell<i32>,
}

impl PendingDeleteResource {
    fn should_run_on(&self, timing: CleanupTiming) -> bool {
        self.timing == timing
    }

    fn take(&self) -> Box<dyn PendingDelete> {
        self.inner
            .borrow_mut()
            .take()
            .expect("pending delete is consumed once at its transaction outcome")
    }

    fn execute_abort(&self) {
        self.inner
            .borrow()
            .as_ref()
            .expect("abort cleanup retains its pending delete")
            .execute();
    }
}

impl TransactionResource for PendingDeleteResource {
    fn on_commit(&self) {
        if self.should_run_on(CleanupTiming::OnCommit) {
            let pending = self.take();
            CommittedCleanup::defer(move || pending.execute());
        }
    }

    fn on_abort(&self) {
        if self.should_run_on(CleanupTiming::OnAbort) {
            self.execute_abort();
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        if self.nest_level() >= current_nest_level
            && self.should_run_on(CleanupTiming::OnAbort)
        {
            self.execute_abort();
            // The outer subtransaction callback removes resources registered
            // at or above this aborted nesting level after this hook returns.
        }
    }

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        // Pending actions are backend-local trait objects and have no durable
        // representation in PostgreSQL's two-phase state. Allowing PREPARE
        // would silently lose both abort cleanup for newly created objects and
        // commit cleanup for retired objects when the backend exits.
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot prepare a transaction with pending external-object cleanup",
        ))
    }

    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }
}

/// Register a pending delete action.
///
/// The current transaction nesting level is captured so abort cleanup registered
/// inside a savepoint runs when that savepoint rolls back, while cleanup
/// promoted by `RELEASE SAVEPOINT` follows the parent transaction.
/// Commit actions transfer ownership at COMMIT and execute at the top-level
/// ResourceOwner's AFTER_LOCKS phase, without extending relation lock lifetime.
pub fn register_pending_delete(entry: Box<dyn PendingDelete>) {
    let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    let resource = Rc::new(PendingDeleteResource {
        timing: entry.timing(),
        inner: RefCell::new(Some(entry)),
        nest_level: Cell::new(nest_level),
    });

    register_resource(resource);
}
