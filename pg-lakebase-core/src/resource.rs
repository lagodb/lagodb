//! Resource Manager mechanism for custom resource cleanup.
//!
//! This module implements a mechanism similar to C++'s `PaxResourceContext`
//! for managing resource cleanup tied to `ResourceOwner`.
//!
//! # Example
//!
//! ```rust,no_run
//! use pg_lakebase_core::resource::{remember_resource, forget_resource};
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
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pgrx::pg_guard;
use pgrx::pg_sys;

/// A handle to a registered resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceHandle(u64);

struct ResourceEntry {
    owner: pg_sys::ResourceOwner,
    callback: Box<dyn FnOnce() + 'static>,
}

thread_local! {
    static RESOURCES: RefCell<HashMap<u64, ResourceEntry>> = RefCell::new(HashMap::new());
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
pub fn remember_resource<F>(callback: F) -> ResourceHandle
where
    F: FnOnce() + 'static,
{
    // Capture the current resource owner.
    let owner = unsafe { pg_sys::CurrentResourceOwner };

    // Ensure we have a valid resource owner
    debug_assert!(
        !owner.is_null(),
        "CurrentResourceOwner is NULL - remember_resource called outside of a transaction?"
    );

    // Generate a unique ID
    let id = NEXT_ID.with(|n| {
        let current = n.get();
        n.set(current + 1);
        current
    });
    let handle = ResourceHandle(id);

    RESOURCES.with(|resources| {
        resources.borrow_mut().insert(
            id,
            ResourceEntry {
                owner,
                callback: Box::new(callback),
            },
        );
    });

    handle
}

/// Forget a registered resource.
///
/// Returns `true` if the resource was found and forgotten.
/// Returns `false` if the resource was not found (already triggered or never existed).
pub fn forget_resource(handle: ResourceHandle) -> bool {
    RESOURCES.with(|resources| resources.borrow_mut().remove(&handle.0).is_some())
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
    _is_top_level: bool,
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

    // Identify resources belonging to the current owner
    // We must extract them to avoid double borrowing RefCell during callback execution
    let to_execute: Vec<(u64, Box<dyn FnOnce() + 'static>)> =
        RESOURCES.with(|resources| {
            let mut map = resources.borrow_mut();
            let mut matched_ids = Vec::new();

            // Find IDs first
            for (id, entry) in map.iter() {
                if entry.owner == current_owner {
                    matched_ids.push(*id);
                }
            }

            // Remove and collect
            let mut extracted = Vec::new();
            for id in matched_ids {
                if let Some(entry) = map.remove(&id) {
                    extracted.push((id, entry.callback));
                }
            }
            extracted
        });

    for (id, callback) in to_execute {
        if is_commit {
            // Log warning as per C++ implementation ("pax resource leaks")
            // This is useful to detect resources that weren't explicitly handled/forgotten on success.
            crate::diag::report_warning(&format!(
                "resource leak detected for resource handle {:?} (owner={:?})",
                id, current_owner
            ));
        }

        // Execute cleanup
        if let Err(e) = catch_unwind(AssertUnwindSafe(callback)) {
            crate::diag::report_warning(&format!(
                "panic during resource cleanup for handle {:?}: {:?}",
                id, e
            ));
        }
    }
}
