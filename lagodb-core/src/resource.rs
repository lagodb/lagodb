//! ResourceOwner-scoped cleanup callbacks.
//!
//! This module intentionally mirrors PostgreSQL's `ResourceOwner` release
//! callback mechanism (`RegisterResourceReleaseCallback`), not PostgreSQL's
//! transaction callback mechanism.  In PostgreSQL 17, `ResourceOwner` is used
//! for query/owner-lifespan resources and is released through
//! `ResourceOwnerRelease(phase, isCommit, isTopLevel)`, while xact/subxact
//! callbacks are separate transaction-event notifications registered via
//! `RegisterXactCallback` and `RegisterSubXactCallback`.
//!
//! Keep this separate from [`crate::transaction`].  Transaction callbacks are
//! appropriate for transaction-scoped state that needs pre-commit, commit,
//! abort, or savepoint event handling.  ResourceOwner callbacks are appropriate
//! for frame-, portal-, executor-, or other owner-scoped resources that must be
//! cleaned up if PostgreSQL unwinds past normal Rust control flow, such as
//! ERROR during mutation or COPY.
//!
//! # Example
//!
//! ```rust,no_run
//! use lagodb_core::resource::{remember_resource, forget_resource};
//!
//! // In some operation
//! let handle = remember_resource(|| {
//!     // Cleanup logic
//!     println!("Cleaning up resource");
//! });
//!
//! // If operation succeeds
//! forget_resource(handle);
//! ```

use std::cell::{Cell, RefCell};
use std::panic::AssertUnwindSafe;

use pgrx::pg_sys;
use pgrx::{PgTryBuilder, pg_guard};

/// A handle to a registered resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceHandle(u64);

struct ResourceEntry {
    id: u64,
    owner: pg_sys::ResourceOwner,
    callback: Box<dyn FnOnce() + 'static>,
}

thread_local! {
    static RESOURCES: RefCell<Vec<ResourceEntry>> = const { RefCell::new(Vec::new()) };
    static NEXT_ID: Cell<u64> = const { Cell::new(1) };
    static CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

/// Register a resource release callback.
///
/// The callback will be called when the current `ResourceOwner` is released (e.g. transaction end),
/// unless `forget_resource` is called first.
///
/// This is typically used to cleanup resources (like open files, memory, or external handles)
/// that must be released if a transaction aborts, but might be handed off or explicitly closed
/// if the transaction commits.
///
/// Note: If the transaction commits and the resource hasn't been forgotten, the callback
/// WILL still run, and a warning will be logged, implying a resource leak if explicit
/// cleanup was expected.
///
/// # Panics
///
/// Panics when PostgreSQL's `CurrentResourceOwner` is null. PostgreSQL uses a
/// null current owner outside a transaction and inside a failed transaction.
/// An auxiliary-process `ResourceOwner` satisfies this precondition; the
/// caller does not need to be in a user transaction.
pub fn remember_resource<F>(callback: F) -> ResourceHandle
where
    F: FnOnce() + 'static,
{
    init_resource_manager();

    // Capture the current resource owner.
    let owner = unsafe { pg_sys::CurrentResourceOwner };

    if owner.is_null() {
        panic!(
            "remember_resource requires PostgreSQL's CurrentResourceOwner to be set"
        );
    }

    // Generate a unique ID
    let id = NEXT_ID.with(|n| {
        let current = n.get();
        n.set(current + 1);
        current
    });
    let handle = ResourceHandle(id);

    RESOURCES.with(|resources| {
        resources.borrow_mut().push(ResourceEntry {
            id,
            owner,
            callback: Box::new(callback),
        });
    });

    handle
}

/// Forget a registered resource.
///
/// Returns `true` if the resource was found and forgotten.
/// Returns `false` if the resource was not found (already triggered or never existed).
pub fn forget_resource(handle: ResourceHandle) -> bool {
    RESOURCES.with(|resources| {
        let mut vec = resources.borrow_mut();
        if let Some(pos) = vec.iter().position(|e| e.id == handle.0) {
            vec.swap_remove(pos);
            true
        } else {
            false
        }
    })
}

/// Initialize the resource manager callback.
///
/// This should be called in `_PG_init` or whenever the extension is initialized.
/// Safe to call multiple times.
pub fn init_resource_manager() {
    CALLBACK_REGISTERED.with(|registered| {
        if registered.get() {
            return;
        }

        unsafe {
            pg_sys::RegisterResourceReleaseCallback(
                Some(release_resource_callback),
                std::ptr::null_mut(),
            );
        }

        registered.set(true);
    });
}

/// usage: `release_resource_callback(phase, is_commit, is_top_level, arg)`
#[pg_guard]
unsafe extern "C-unwind" fn release_resource_callback(
    phase: pg_sys::ResourceReleasePhase::Type,
    is_commit: bool,
    is_top_level: bool,
    _arg: *mut std::ffi::c_void,
) {
    // Only process during post-lock phase, similar to C++ implementation
    if phase != pg_sys::ResourceReleasePhase::RESOURCE_RELEASE_AFTER_LOCKS {
        return;
    }

    // Check if process exit is in progress
    // SAFETY: proc_exit_inprogress is a PostgreSQL global variable
    if unsafe { pg_sys::proc_exit_inprogress } {
        return;
    }

    // SAFETY: CurrentResourceOwner is set by PostgreSQL during transaction
    let current_owner = unsafe { pg_sys::CurrentResourceOwner };

    if is_commit && !is_top_level {
        let parent = unsafe { pg_sys::ResourceOwnerGetParent(current_owner) };
        if !parent.is_null() {
            RESOURCES.with(|resources| {
                for entry in resources.borrow_mut().iter_mut() {
                    if entry.owner == current_owner {
                        entry.owner = parent;
                    }
                }
            });
        }
        return;
    }

    // Extract matching resources in a single pass to avoid double borrowing
    // RefCell during callback execution.
    let to_execute: Vec<(u64, Box<dyn FnOnce() + 'static>)> =
        RESOURCES.with(|resources| {
            let mut vec = resources.borrow_mut();
            let mut extracted = Vec::new();
            let mut i = 0;
            while i < vec.len() {
                if vec[i].owner == current_owner {
                    let entry = vec.swap_remove(i);
                    extracted.push((entry.id, entry.callback));
                } else {
                    i += 1;
                }
            }
            extracted
        });

    for (id, callback) in to_execute {
        if is_commit {
            // Log warning as per C++ implementation ("pax resource leaks")
            // This is useful to detect resources that weren't explicitly handled/forgotten on success.
            crate::diag::report_warning(format_args!(
                "resource leak detected for resource handle {:?} (owner={:?})",
                id, current_owner
            ));
        }

        PgTryBuilder::new(AssertUnwindSafe(move || {
            callback();
        }))
        .catch_others(|err| {
            crate::diag::report_warning(format_args!(
                "error during resource cleanup for handle {:?}: {}",
                id,
                crate::diag::PgErrorReport::from_caught(err)
            ));
        })
        .execute();
    }
}
