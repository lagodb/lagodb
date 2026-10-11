//! ResourceOwner and executor ownership for the once-only startup transition.

use std::cell::{Cell, UnsafeCell};
use std::ptr;
use std::rc::Rc;

use lagodb_query::datafusion::{
    PendingQueryExecution, QueryExecution, QueryExecutionError,
};
use pgrx::pg_sys;

use super::QueryHostError;

/// Single-threaded owner shared by the CustomScan and its ResourceOwner callback.
///
/// PostgreSQL may release a failed portal without calling EndCustomScan. Both
/// paths take the owned value, so only the first performs cleanup. The pending
/// binding is consumed at startup; the row path borrows only the started slot.
/// These slots are private and at most one is populated. After startup failure
/// or release both are empty. No execution object moves out except for cleanup.
/// Startup publishes its address from this cell's stable Rc allocation. All
/// live borrows use that pointer; the owning Option is accessed again only to
/// take the value during final cleanup, when no further row access is allowed.
///
/// Executor and ResourceOwner callbacks are serialized on the backend thread.
/// Each borrow must end before control returns to PostgreSQL for error cleanup.
/// pgrx converts ERROR from protected outbound calls into a Rust unwind before
/// reporting it at the inbound callback boundary, releasing executor borrows.
pub(super) struct QueryExecutionCell {
    pending: UnsafeCell<Option<PendingQueryExecution>>,
    execution: UnsafeCell<Option<QueryExecution>>,
    execution_ptr: Cell<*mut QueryExecution>,
}

impl QueryExecutionCell {
    pub(super) fn new(pending: PendingQueryExecution) -> Self {
        Self {
            pending: UnsafeCell::new(Some(pending)),
            execution: UnsafeCell::new(None),
            execution_ptr: Cell::new(ptr::null_mut()),
        }
    }

    /// # Safety
    /// The caller must be the active backend callback for this CustomScan;
    /// no executor or ResourceOwner callback may access the cell concurrently.
    /// The returned pointer is borrowed from this Rc allocation. It must only
    /// be used while the owner remains live and before close or abort. Borrows
    /// through it must not overlap other execution or cleanup callbacks.
    pub(super) unsafe fn start(
        self: &Rc<Self>,
    ) -> Result<*mut QueryExecution, QueryHostError> {
        let pending = unsafe { (&mut *self.pending.get()).take() }.ok_or(
            QueryHostError::ExecutorContract("query startup has no pending binding"),
        )?;
        // start owns failure cleanup. Publish only after preparation succeeds;
        // the host switches to its row method table after this returns.
        let execution = pending.start()?;
        let execution = unsafe { (&mut *self.execution.get()).insert(execution) };
        let execution_ptr = ptr::from_mut(execution);
        self.execution_ptr.set(execution_ptr);
        Ok(execution_ptr)
    }

    /// Inspect either lifecycle phase without starting an unexecuted node.
    ///
    /// # Safety
    /// The caller must uphold the serialized callback contract of `start`.
    pub(super) unsafe fn with_ref<R>(
        &self,
        pending: impl FnOnce(&PendingQueryExecution) -> R,
        started: impl FnOnce(&QueryExecution) -> R,
    ) -> Option<R> {
        if let Some(execution) = unsafe { self.execution_ptr.get().as_ref() } {
            Some(started(execution))
        } else {
            unsafe { (&*self.pending.get()).as_ref().map(pending) }
        }
    }

    /// # Safety
    /// The callback contract of `start` applies; `changed_parameters` is the
    /// live PlanState bitmap supplied by PostgreSQL, or NULL.
    pub(super) unsafe fn rescan(
        &self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) -> Result<(), QueryHostError> {
        // Initial dynamic binding happens on the first run. A pre-start rescan
        // must not select data or prepare a physical plan.
        if unsafe { (&*self.pending.get()).is_some() } {
            return Ok(());
        }
        let execution = unsafe { self.execution_ptr.get().as_mut() }.ok_or(
            QueryHostError::ExecutorContract(
                "query-offload execution was already released",
            ),
        )?;
        unsafe { execution.rescan(changed_parameters) }.map_err(QueryHostError::from)
    }

    pub(super) fn close(&self) -> Result<(), QueryExecutionError> {
        self.execution_ptr.set(ptr::null_mut());
        // SAFETY: normal End and ResourceOwner cleanup are serialized. Taking
        // ownership before cleanup also leaves fallback Drop nothing to release.
        if let Some(execution) = unsafe { (&mut *self.execution.get()).take() } {
            execution.close()
        } else {
            unsafe { (&mut *self.pending.get()).take() }
                .map_or(Ok(()), PendingQueryExecution::close)
        }
    }

    pub(super) fn abort(&self) {
        self.execution_ptr.set(ptr::null_mut());
        // After ERROR, evaluator state belongs to PostgreSQL's context cleanup;
        // neither phase may invoke normal PG evaluator shutdown.
        if let Some(execution) = unsafe { (&mut *self.execution.get()).take() } {
            execution.abort();
        } else if let Some(pending) = unsafe { (&mut *self.pending.get()).take() } {
            pending.abort();
        }
    }
}

impl Drop for QueryExecutionCell {
    fn drop(&mut self) {
        // The CustomScan context callback uses abort before releasing its Rc.
        // This normal-close fallback also covers unwinding before registration
        // of the ResourceOwner callback, as the former QueryExecution Drop did.
        let _ = self.close();
    }
}
