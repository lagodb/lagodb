//! Command-owned relation split and cross-transaction maintenance storage.
//!
//! Expansion classifies each relation from its catalog access method without
//! holding a target lock. The classification only selects an executor: a
//! provider relation is resolved again from its locked `Relation`, and a native
//! relation that became provider-owned fails closed inside PostgreSQL. The
//! Portal child owns expanded nodes, lists and statements until this plan drops.

use std::ptr;

use pgrx::pg_sys;

use super::MaintenanceCommand;
use super::bridge::MaintenanceBridge;
use crate::backend::MemoryContextHandle;

/// Relations split by executor, in PostgreSQL's expansion order.
pub struct MaintenancePlan {
    pub(super) command: MaintenanceCommand,
    pub(super) stmt: *mut pg_sys::VacuumStmt,
    pub(super) native: Box<[*mut pg_sys::VacuumRelation]>,
    pub(super) provider: Box<[*mut pg_sys::VacuumRelation]>,
    context: MemoryContextHandle,
}

impl MaintenancePlan {
    /// Expand prepared command targets and split them by access method.
    ///
    /// # Safety
    /// The statement and its referenced names, options and column lists must
    /// remain live through execution. PortalContext must outlive this command,
    /// and all C calls while the plan is live must use PostgreSQL ERROR
    /// protection. The command must have been prepared from this statement
    /// before admission. The classifier must protect any FFI calls that can
    /// raise ERROR.
    pub unsafe fn expand(
        stmt: *mut pg_sys::VacuumStmt,
        command: MaintenanceCommand,
        mut is_provider_am: impl FnMut(pg_sys::Oid) -> bool,
    ) -> Self {
        // SAFETY: the live Portal survives this command's transaction changes;
        // the static name also avoids allocating a context name in the Portal.
        let context = unsafe {
            MemoryContextHandle::create(
                pg_sys::PortalContext,
                c"lagodb maintenance command",
            )
        };
        // Establish ownership before expansion so guarded ERROR also drops it.
        let mut plan = Self {
            command,
            stmt,
            native: Box::new([]),
            provider: Box::new([]),
            context,
        };
        // SAFETY: the executor allocates the expansion in this owned context.
        // Borrowed names and column lists retain the input statement's lifetime.
        unsafe {
            let expanded = MaintenanceBridge::expand_relations(
                stmt,
                &mut plan.command.params,
                context.as_ptr(),
            );
            let count = pg_sys::list_length(expanded);
            let mut native = Vec::with_capacity(count as usize);
            let mut provider = Vec::new();
            for index in 0..count {
                let relation = pg_sys::list_nth(expanded, index)
                    .cast::<pg_sys::VacuumRelation>();
                // A relation dropped since expansion has no access method and
                // stays native: PostgreSQL applies its own skip behavior.
                if is_provider_am(MaintenanceBridge::relation_am((*relation).oid)) {
                    provider.push(relation);
                } else {
                    native.push(relation);
                }
            }
            plan.native = native.into_boxed_slice();
            plan.provider = provider.into_boxed_slice();
        }
        plan
    }

    /// PostgreSQL's statement for the native relations, with every option.
    ///
    /// # Safety
    /// The plan must describe the live command; the result lives as long as
    /// this plan and must not be used after it drops.
    pub(super) unsafe fn native_stmt(&self) -> *mut pg_sys::VacuumStmt {
        // SAFETY: the list and the statement are owned by this plan's context.
        unsafe {
            let relations = self.relation_list(&self.native);
            MaintenanceBridge::vacuum_stmt(
                self.stmt,
                relations,
                self.context.as_ptr(),
            )
        }
    }

    /// PostgreSQL's ANALYZE statement for relations that finished the rewrite.
    ///
    /// # Safety
    /// As for [`Self::native_stmt`].
    pub(super) unsafe fn analyze_stmt(
        &self,
        relations: &[*mut pg_sys::VacuumRelation],
    ) -> *mut pg_sys::VacuumStmt {
        // SAFETY: the list and the statement are owned by this plan's context.
        unsafe {
            let relations = self.relation_list(relations);
            MaintenanceBridge::analyze_stmt(
                self.stmt,
                relations,
                self.context.as_ptr(),
            )
        }
    }

    /// PostgreSQL keeps these nodes across its transactions, so the list must
    /// survive them too. Expanded nodes carry explicit OIDs, which PostgreSQL
    /// does not expand a second time.
    unsafe fn relation_list(
        &self,
        relations: &[*mut pg_sys::VacuumRelation],
    ) -> *mut pg_sys::List {
        // SAFETY: this plan's context is live; Drop handles an allocation
        // ERROR raised while it is current.
        unsafe {
            let previous = pg_sys::MemoryContextSwitchTo(self.context.as_ptr());
            let mut list = ptr::null_mut();
            for relation in relations {
                list = pg_sys::lappend(list, relation.cast());
            }
            pg_sys::MemoryContextSwitchTo(previous);
            list
        }
    }
}

impl Drop for MaintenancePlan {
    fn drop(&mut self) {
        // SAFETY: guarded calls unwind before PostgreSQL destroys the Portal.
        // The plan is the sole owner and dies before control leaves the router.
        // If allocation failed while its context was current, switch to the
        // Portal rather than an entry transaction context that may be gone.
        unsafe {
            if pg_sys::CurrentMemoryContext == self.context.as_ptr() {
                pg_sys::MemoryContextSwitchTo(pg_sys::PortalContext);
            }
            self.context.delete();
        }
    }
}
