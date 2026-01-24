//! Transaction and Resource Management for custom Table Access Methods.
//!
//! This module implements a unified transaction resource management framework,
//! allowing extensions to register callbacks for transaction lifecycle events.
//!
//! # Background
//!
//! This framework is similar to PostgreSQL's `ResourceOwner` and `PendingRelDelete`
//! mechanisms. It handles two main scenarios:
//! 1. **Abort Cleanup**: Cleaning up resources (like files or memory) if a
//!    transaction fails.
//! 2. **Commit Cleanup**: Executing actions (like physical file deletion) only
//!    after a transaction successfully commits.
//!
//! It also provides a specific `PendingDelete` trait for the common use case
//! of managing storage cleanup.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                Transaction Lifecycle                          │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Operation (e.g. CREATE or DROP)                              │
//! │      │                                                        │
//! │      ▼                                                        │
//! │  1. Perform action & register_pending_delete()                │
//! │      │  (mark at_commit=false for CREATE, true for DROP)      │
//! │      ▼                                                        │
//! ├──────┼────────────────────────────────────────────────────────┤
//! │      │                                                        │
//! │   COMMIT                         ABORT                        │
//! │      │                              │                         │
//! │      ▼                              ▼                         │
//! │  Execute items where            Execute items where           │
//! │  at_commit == true              at_commit == false            │
//! │                                                               │
//! │  (e.g. Delete dropped table)    (e.g. Cleanup failed create)  │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::rc::Rc;

use pgrx::pg_guard;
use pgrx::pg_sys;

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
    fn on_pre_commit(&self) {}

    /// Called when a subtransaction is committed (RELEASE SAVEPOINT).
    fn on_commit_sub(&self, _current_nest_level: i32) {}

    /// Called when a subtransaction is aborted (ROLLBACK TO SAVEPOINT).
    fn on_abort_sub(&self, _current_nest_level: i32) {}

    /// Get the transaction nesting level at which this resource was registered.
    fn nest_level(&self) -> i32;

    /// Update the transaction nesting level (used during subtransaction promotion).
    fn set_nest_level(&self, level: i32);
}

/// A pending delete entry that can be executed when a transaction aborts or commits.
///
/// Implementations should clean up any storage resources that were created
/// during the transaction but need to be removed on failure (abort), or
/// resources that were marked for deletion but should only be physically
/// removed after successful completion (commit).
pub trait PendingDelete: Debug + Send {
    /// Execute the delete operation.
    ///
    /// This is called when the transaction reaches the specified state (commit or abort).
    /// Implementations should delete any storage files/directories that were registered.
    ///
    /// Errors during deletion are logged but do not prevent other pending
    /// deletes from being processed.
    fn execute(&self);

    /// Whether this delete should occur at commit time.
    ///
    /// - `true`: Execute on COMMIT (like DROP TABLE cleanup after commit)
    /// - `false`: Execute on ABORT (like CREATE TABLE cleanup on rollback)
    ///
    /// Default is `false` (execute on abort), which is the common case for
    /// cleaning up newly created storage on transaction failure.
    fn at_commit(&self) -> bool {
        false
    }
}

// Thread-local storage for unified transaction resources.
thread_local! {
    static RESOURCES: RefCell<Vec<Rc<dyn TransactionResource>>> = const { RefCell::new(Vec::new()) };
    static CALLBACK_REGISTERED: RefCell<bool> = const { RefCell::new(false) };
}

/// Register a transaction resource.
pub fn register_resource(resource: Rc<dyn TransactionResource>) {
    RESOURCES.with(|res| {
        res.borrow_mut().push(resource);
    });
}

/// Adapter that implements TransactionResource for a PendingDelete implementation.
#[derive(Debug)]
struct PendingDeleteAdapter {
    inner: Box<dyn PendingDelete>,
    nest_level: Cell<i32>,
}

impl TransactionResource for PendingDeleteAdapter {
    fn on_commit(&self) {
        if self.inner.at_commit() {
            self.inner.execute();
        }
    }

    fn on_abort(&self) {
        if !self.inner.at_commit() {
            self.inner.execute();
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        // If this item was supposed to be deleted on abort, do it now.
        if self.nest_level() >= current_nest_level {
            if !self.inner.at_commit() {
                self.inner.execute();
            }
        }
    }

    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }
}

/// Register a pending delete entry.
///
/// The entry will be processed at transaction end:
/// - If `entry.at_commit()` is false (default): executed on ABORT
/// - If `entry.at_commit()` is true: executed on COMMIT
///
/// This function captures the current transaction nesting level to correctly
/// handle subtransactions (SAVEPOINT/ROLLBACK TO).
pub fn register_pending_delete(entry: Box<dyn PendingDelete>) {
    let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
    let adapter = Rc::new(PendingDeleteAdapter {
        inner: entry,
        nest_level: Cell::new(nest_level),
    });

    register_resource(adapter);
}

/// Get the current number of registered transaction resources.
///
/// Useful for debugging and testing.
pub fn pending_delete_size() -> usize {
    RESOURCES.with(|res| res.borrow().len())
}

/// Initialize the transaction callbacks.
///
/// This should be called during extension initialization (`_PG_init`).
/// Safe to call multiple times - will only register once.
pub fn init_xact_callback() {
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
            RESOURCES.with(|res| {
                for r in res.borrow().iter() {
                    if r.nest_level() >= current_nest_level {
                        r.on_pre_commit();
                    }
                }
            });
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
            RESOURCES.with(|res| {
                for r in res.borrow().iter() {
                    r.on_commit_sub(current_nest_level);
                    if r.nest_level() >= current_nest_level {
                        // Promote to parent level
                        r.set_nest_level(current_nest_level - 1);
                    }
                }
            });
        }
        SUBXACT_EVENT_ABORT_SUB => {
            RESOURCES.with(|res| {
                let mut borrow = res.borrow_mut();
                for r in borrow.iter() {
                    r.on_abort_sub(current_nest_level);
                }
                borrow.retain(|r| r.nest_level() < current_nest_level);
            });
        }
        _ => {}
    }
}
