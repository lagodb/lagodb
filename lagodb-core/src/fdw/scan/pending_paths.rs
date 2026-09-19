//! Ownership boundary for ForeignPaths not yet submitted to PostgreSQL.

use core::mem::size_of;
use core::ptr::{self, NonNull};

use pgrx::pg_sys;

/// Foreign paths that are fully constructed but not yet owned by PostgreSQL.
///
/// PostgreSQL may free a path while accepting it, so complete and partial
/// siblings must be constructed before either pointer is published.
pub(super) struct PendingForeignPaths {
    complete: NonNull<pg_sys::ForeignPath>,
    partial: Option<NonNull<pg_sys::ForeignPath>>,
}

impl PendingForeignPaths {
    pub(super) fn new(complete: NonNull<pg_sys::ForeignPath>) -> Self {
        Self {
            complete,
            partial: None,
        }
    }

    /// Construct the partial sibling while the complete path is still owned
    /// by this object.
    ///
    /// # Safety
    ///
    /// `self.complete` must point to a fully initialized `ForeignPath`
    /// allocated in the current planner memory context. `workers` must be
    /// positive.
    pub(super) unsafe fn add_parallel_sibling(&mut self, workers: i32) {
        // SAFETY: PostgreSQL allocation reports OOM with ERROR instead of
        // returning NULL. Both objects live in the planner memory context.
        let partial = unsafe {
            NonNull::new_unchecked(
                pg_sys::palloc0(size_of::<pg_sys::ForeignPath>())
                    .cast::<pg_sys::ForeignPath>(),
            )
        };
        // SAFETY: the source and destination are distinct, valid allocations
        // for one initialized ForeignPath.
        unsafe {
            ptr::copy_nonoverlapping(self.complete.as_ptr(), partial.as_ptr(), 1);
        }
        // SAFETY: `partial` remains exclusively owned by this object until
        // `publish` transfers it to PostgreSQL.
        let partial_path = unsafe { &mut (*partial.as_ptr()).path };
        partial_path.parallel_aware = true;
        partial_path.parallel_safe = true;
        partial_path.parallel_workers = workers;
        let mut divisor = workers as f64;
        if unsafe { pg_sys::parallel_leader_participation } {
            divisor += (1.0 - 0.3 * workers as f64).max(0.0);
        }
        partial_path.rows /= divisor;
        partial_path.total_cost = partial_path.startup_cost
            + (partial_path.total_cost - partial_path.startup_cost) / divisor;
        self.partial = Some(partial);
    }

    /// Transfer every constructed path to PostgreSQL without touching either
    /// pointer after its corresponding add call.
    ///
    /// # Safety
    ///
    /// `baserel` must be the live planner relation for both paths. Neither path
    /// may have been published previously.
    pub(super) unsafe fn publish(self, baserel: *mut pg_sys::RelOptInfo) {
        let Self { complete, partial } = self;
        unsafe { pg_sys::add_path(baserel, complete.as_ptr().cast()) };
        if let Some(partial) = partial {
            unsafe {
                pg_sys::add_partial_path(baserel, partial.as_ptr().cast());
            }
        }
    }
}
