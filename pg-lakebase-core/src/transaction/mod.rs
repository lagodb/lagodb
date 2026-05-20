//! Transaction lifecycle callbacks for custom Table Access Methods.
//!
//! This module implements a transaction-event framework built on PostgreSQL's
//! `RegisterXactCallback` and `RegisterSubXactCallback` hooks.  It is separate
//! from [`crate::resource`], which mirrors PostgreSQL's `ResourceOwner` release
//! callbacks.  The two mechanisms have different lifetimes and should not be
//! collapsed into one another:
//!
//! - transaction callbacks model top-level transaction and savepoint events;
//! - ResourceOwner callbacks model owner-scoped resource release during
//!   PostgreSQL cleanup phases.
//!
//! Use this module for transaction-scoped state, such as metadata updates,
//! staged publication, or cleanup actions that must react to commit, abort,
//! pre-commit, or subtransaction promotion/rollback.  Use [`crate::resource`] for
//! executor, portal, DML-frame, COPY, or other ResourceOwner-bound cleanup that
//! must run when PostgreSQL releases the owner even after ERROR/longjmp.
//!
//! The [`cleanup`] submodule provides higher-level helpers for common
//! transaction-scoped cleanup patterns.

use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use crate::diag::PgReportError;
use pgrx::pg_guard;
use pgrx::pg_sys;

pub mod cleanup;

pub type TransactionResult<T> = Result<T, PgReportError>;

/// Unified transaction resource callback interface.
///
/// This trait combines both top-level transaction and subtransaction events.
/// Implementations can choose which events to respond to.
pub trait TransactionResource: Debug {
    /// Called when the top-level transaction is committed.
    fn on_commit(&self) {}

    /// Called when the top-level transaction is aborted.
    fn on_abort(&self) {}

    /// Called during the pre-commit phase of the top-level transaction.
    ///
    /// Returning `Err` reports a PostgreSQL `ERROR` and aborts the transaction.
    /// The transaction framework is fail-fast here: later resources are not
    /// guaranteed to receive `on_pre_commit` after a resource fails.
    fn on_pre_commit(&self) -> TransactionResult<()> {
        Ok(())
    }

    /// Called when a subtransaction is committed (RELEASE SAVEPOINT).
    fn on_commit_sub(&self, _current_nest_level: i32) {}

    /// Called when a subtransaction is aborted (ROLLBACK TO SAVEPOINT).
    fn on_abort_sub(&self, _current_nest_level: i32) {}

    /// Get the transaction nesting level at which this resource was registered.
    fn nest_level(&self) -> i32;

    /// Update the transaction nesting level (used during subtransaction promotion).
    fn set_nest_level(&self, level: i32);
}

// Thread-local storage for unified transaction resources. Resources are invoked
// in registration order; the transaction framework does not assign priority to
// specific resource types. If a resource needs ordering, model it explicitly in
// the resource implementation or registration protocol.
thread_local! {
    static RESOURCES: RefCell<Vec<Rc<dyn TransactionResource>>> = const { RefCell::new(Vec::new()) };
    static CALLBACK_REGISTERED: RefCell<bool> = const { RefCell::new(false) };
}

/// Register a transaction resource.
///
/// Registered resources are invoked in the order they are registered for
/// transaction and subtransaction events.
pub fn register_resource(resource: Rc<dyn TransactionResource>) {
    init_callbacks();

    RESOURCES.with(|res| {
        res.borrow_mut().push(resource);
    });
}

/// Get the current number of registered transaction resources.
///
/// Useful for debugging and testing.
pub fn resource_count() -> usize {
    RESOURCES.with(|res| res.borrow().len())
}

/// Initialize the transaction callbacks.
///
/// This should be called during extension initialization (`_PG_init`).
/// Safe to call multiple times - will only register once.
pub fn init_callbacks() {
    CALLBACK_REGISTERED.with(|registered| {
        if *registered.borrow() {
            return;
        }

        unsafe {
            pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
            pg_sys::RegisterSubXactCallback(
                Some(subxact_callback),
                std::ptr::null_mut(),
            );
        }

        *registered.borrow_mut() = true;
    });
}

/// PostgreSQL transaction callback.
#[pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::XactEvent::*;

    let current_nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };

    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => {
            RESOURCES.with(|res| {
                let resources = std::mem::take(&mut *res.borrow_mut());
                for r in resources {
                    if r.nest_level() >= current_nest_level {
                        r.on_commit();
                    }
                }
            });
        }
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => {
            RESOURCES.with(|res| {
                let resources = std::mem::take(&mut *res.borrow_mut());
                for r in resources {
                    if r.nest_level() >= current_nest_level {
                        r.on_abort();
                    }
                }
            });
        }
        XACT_EVENT_PRE_COMMIT | XACT_EVENT_PARALLEL_PRE_COMMIT => {
            // Snapshot the resource list before invoking callbacks to avoid
            // holding the RefCell borrow during on_pre_commit(). A callback
            // may register new resources (e.g. metadata commit writes files
            // that register storage artifacts). New resources added during
            // this loop will NOT receive on_pre_commit in this round, but
            // they WILL receive the subsequent on_commit / on_abort.
            let snapshot: Vec<Rc<dyn TransactionResource>> =
                RESOURCES.with(|res| res.borrow().clone());
            for r in &snapshot {
                if r.nest_level() >= current_nest_level {
                    if let Err(error) = r.on_pre_commit() {
                        // PgReportError::report() raises PostgreSQL ERROR and
                        // does not return, so this aborts the pre-commit loop.
                        error.report();
                    }
                }
            }
        }
        _ => {}
    }
}

/// PostgreSQL subtransaction callback.
#[pg_guard]
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    use pg_sys::SubXactEvent::*;

    let current_nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };

    match event {
        SUBXACT_EVENT_COMMIT_SUB => {
            let snapshot: Vec<Rc<dyn TransactionResource>> =
                RESOURCES.with(|res| res.borrow().clone());
            for r in &snapshot {
                r.on_commit_sub(current_nest_level);
                if r.nest_level() >= current_nest_level {
                    r.set_nest_level(current_nest_level - 1);
                }
            }
        }
        SUBXACT_EVENT_ABORT_SUB => {
            let snapshot: Vec<Rc<dyn TransactionResource>> =
                RESOURCES.with(|res| res.borrow().clone());
            for r in &snapshot {
                r.on_abort_sub(current_nest_level);
            }
            RESOURCES.with(|res| {
                res.borrow_mut()
                    .retain(|r| r.nest_level() < current_nest_level);
            });
        }
        _ => {}
    }
}
