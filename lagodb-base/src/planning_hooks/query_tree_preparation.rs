//! One query-tree traversal for provider preparation.

use std::ffi::c_void;
use std::ptr;

use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::CallbackErrorReport;
use pgrx::pg_sys;

use crate::descriptor_registry::DescriptorSnapshot;

use super::registry::StoredModifyPlanner;

pub(super) struct QueryTreePreparation {
    modify_planners: DescriptorSnapshot<StoredModifyPlanner>,
    error: Option<PgReportError>,
}

impl QueryTreePreparation {
    pub(super) fn new(
        modify_planners: DescriptorSnapshot<StoredModifyPlanner>,
    ) -> Self {
        Self {
            modify_planners,
            error: None,
        }
    }

    /// Prepare this Query, then visit its nested queries and CTEs.
    ///
    /// # Safety
    /// `parse` is live rewrite-complete planner input with relation locks held.
    /// Registered callbacks only prepare the supplied Query; PostgreSQL owns
    /// all nodes throughout this synchronous walk on the backend thread.
    pub(super) unsafe fn prepare_tree(
        &mut self,
        parse: *mut pg_sys::Query,
    ) -> Result<(), PgReportError> {
        // Decided once, before touching the Query or walking VALUES.
        if self.modify_planners.is_empty() {
            return Ok(());
        }
        // SAFETY: the caller supplies the live, locked query tree.
        unsafe { self.prepare_query(parse) }
    }

    /// # Safety
    /// `parse` is a live Query in the tree supplied to `prepare_tree`.
    unsafe fn prepare_query(
        &mut self,
        parse: *mut pg_sys::Query,
    ) -> Result<(), PgReportError> {
        self.modify_planners.try_for_each(|descriptor| {
            let mut error = CallbackErrorReport::default();
            // SAFETY: registration validated this single-Query callback. The
            // Query and error record stay live through its synchronous call.
            let status = unsafe {
                (descriptor.prepare_query)(descriptor.context, parse, &mut error)
            };
            // SAFETY: a failed callback populated `error` with text owned by
            // the active PostgreSQL context for this synchronous call.
            unsafe { error.into_result(status, "modify query preparation callback") }
        })?;

        // SAFETY: all Query-local mutations are complete before PG reads its
        // expression lists. The walk borrows this state synchronously; default
        // flags include RTE subqueries, sublinks, and CTEs.
        unsafe {
            pg_sys::query_tree_walker(
                parse,
                Some(Self::visit_node),
                ptr::from_mut(self).cast(),
                0,
            );
        }
        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    unsafe extern "C-unwind" fn visit_node(
        node: *mut pg_sys::Node,
        context: *mut c_void,
    ) -> bool {
        // PostgreSQL calls expression walkers with null for absent expressions.
        if node.is_null() {
            return false;
        }
        // SAFETY: PostgreSQL supplies live nodes and the caller's borrowed
        // preparation state. A true result stops traversal and propagates the
        // stored error through Result to the planner's existing FFI boundary.
        unsafe {
            if (*node).type_ == pg_sys::NodeTag::T_Query {
                let preparation = &mut *context.cast::<Self>();
                if let Err(error) = preparation.prepare_query(node.cast()) {
                    preparation.error = Some(error);
                    return true;
                }
                return false;
            }
            pg_sys::expression_tree_walker(node, Some(Self::visit_node), context)
        }
    }
}
