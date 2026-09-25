//! Command-owned relation order and cross-transaction maintenance storage.
//!
//! Only relation order is retained: AM, relkind and provider ownership are
//! resolved from locked relations at execution time. The Portal child owns
//! expanded nodes, lists and buffer strategies until this plan is dropped.

use pgrx::pg_sys;

use super::MaintenanceCommand;
use super::bridge::MaintenanceBridge;

/// Ordered relation plan with storage that survives maintenance transactions.
pub struct MaintenancePlan {
    pub(super) command: MaintenanceCommand,
    pub(super) relations: Box<[*mut pg_sys::VacuumRelation]>,
    context: pg_sys::MemoryContext,
}

impl MaintenancePlan {
    /// Expand prepared command targets in PostgreSQL's original order.
    ///
    /// # Safety
    /// The statement and its referenced names and column lists must remain live
    /// through execution. PortalContext must outlive this command, and all C
    /// calls while the plan is live must use PostgreSQL ERROR protection. The
    /// command must have been prepared from this statement before admission.
    pub unsafe fn expand(
        stmt: *mut pg_sys::VacuumStmt,
        command: MaintenanceCommand,
    ) -> Self {
        // SAFETY: the live Portal survives this command's transaction changes;
        // the static name also avoids allocating a context name in the Portal.
        let context = unsafe {
            pg_sys::AllocSetContextCreateExtended(
                pg_sys::PortalContext,
                c"lagodb maintenance command".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        };
        // Establish ownership before expansion so guarded ERROR also drops it.
        let mut plan = Self {
            command,
            relations: Box::new([]),
            context,
        };
        // SAFETY: the executor allocates the expansion in this owned context.
        // Borrowed names and column lists retain the input statement's lifetime.
        unsafe {
            let expanded = MaintenanceBridge::expand_relations(
                stmt,
                &mut plan.command.params,
                context,
            );
            let count = pg_sys::list_length(expanded);
            let mut relations = Vec::with_capacity(count as usize);
            for index in 0..count {
                relations.push(
                    pg_sys::list_nth(expanded, index)
                        .cast::<pg_sys::VacuumRelation>(),
                );
            }
            plan.relations = relations.into_boxed_slice();
        }
        plan
    }

    /// # Safety
    /// `ring_size_kb` must come from command preparation. The returned strategy
    /// may only be used while this plan remains live on the backend thread.
    pub(super) unsafe fn buffer_strategy(
        &self,
        ring_size_kb: i32,
    ) -> pg_sys::BufferAccessStrategy {
        // SAFETY: this plan owns the allocation through all maintenance calls.
        unsafe { MaintenanceBridge::buffer_strategy(ring_size_kb, self.context) }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }
}

impl Drop for MaintenancePlan {
    fn drop(&mut self) {
        // SAFETY: guarded calls unwind before PostgreSQL destroys the Portal.
        // The plan is the sole owner and dies before control leaves the router.
        // If allocation failed while its context was current, switch to the
        // Portal rather than an entry transaction context that may be gone.
        unsafe {
            if pg_sys::CurrentMemoryContext == self.context {
                pg_sys::MemoryContextSwitchTo(pg_sys::PortalContext);
            }
            pg_sys::MemoryContextDelete(self.context);
        }
    }
}
