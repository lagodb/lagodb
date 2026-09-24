//! Query preparation and catalog-stage storage semantics for provider-owned
//! partitioned tables.

use std::mem::size_of;

use pgrx::PgSqlErrorCode;
use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use crate::customscan::error::CustomScanError;
use crate::customscan::provider::{RelationContext, find_matching_provider};
use crate::diag::PgReportError;
use crate::expr::relation::PlanRelationResolver;
use crate::handles::RelationGuard;
use crate::table_provider::TableProviderRouter;

unsafe extern "C-unwind" {
    fn lagodb_partitioned_table_estimate_size(
        relation: pg_sys::Relation,
        attr_widths: *mut i32,
        pages: *mut pg_sys::BlockNumber,
        tuples: *mut f64,
        allvisfrac: *mut f64,
    );
}

/// Shared partitioned table preparation for reads and modification targets.
pub struct ProviderPartitionedTablePlanner {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    query_rte: *mut pg_sys::RangeTblEntry,
}

impl ProviderPartitionedTablePlanner {
    /// Disable PostgreSQL leaf expansion for provider-owned partitioned tables
    /// in this Query.
    ///
    /// Ownership comes from the runtime table-provider descriptor, independent
    /// of the DSO-local Custom ModifyTable registry. Query relkind stays intact.
    ///
    /// # Safety
    /// `parse` is live rewrite-complete planner input with rewriter locks held.
    /// The runtime calls this for each Query before standard planning.
    pub unsafe fn prepare_query(
        parse: *mut pg_sys::Query,
    ) -> Result<(), PgReportError> {
        // SAFETY: PostgreSQL owns this query and its range table for planning.
        unsafe {
            let range_table = (*parse).rtable;
            for index in 0..pg_sys::list_length(range_table) {
                let rte = pg_sys::list_nth(range_table, index)
                    .cast::<pg_sys::RangeTblEntry>();
                if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION
                    || (*rte).relkind as u8 != pg_sys::RELKIND_PARTITIONED_TABLE
                {
                    continue;
                }
                let access_method = pg_sys::get_rel_relam((*rte).relid);
                let owns_partitioned_table =
                    TableProviderRouter::owns_partitioned_table(access_method)
                        .map_err(|error| {
                            PgReportError::from_message(
                                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                                error.to_string(),
                            )
                        })?;
                if owns_partitioned_table {
                    (*rte).inh = false;
                }
            }
        }
        Ok(())
    }

    /// Supply physical estimates at get_relation_info, then let PostgreSQL's
    /// plain-relation sizing compute restrictions, width and qual cost for all
    /// baserels before it totals pages and costs any index or CustomPath.
    ///
    /// # Safety
    ///
    /// Called with the live get_relation_info arguments after catalog fields
    /// and attr_widths are initialized. The rewriter's relation lock is held.
    pub(super) unsafe fn prepare(
        root: *mut pg_sys::PlannerInfo,
        relation_oid: pg_sys::Oid,
        rel: *mut pg_sys::RelOptInfo,
    ) -> Result<(), CustomScanError> {
        let query_rte =
            unsafe { PlanRelationResolver::new(root).query_rte((*rel).relid) };
        if unsafe { (*query_rte).relkind } as u8 != pg_sys::RELKIND_PARTITIONED_TABLE
        {
            return Ok(());
        }
        let context = RelationContext::from_ref(unsafe { &*query_rte });
        let Some(provider) = find_matching_provider(&context)? else {
            return Ok(());
        };
        if !provider.owns_partitioned_table() {
            return Ok(());
        }
        let planner = Self {
            root,
            rel,
            query_rte,
        };
        unsafe { planner.estimate_storage(relation_oid)? };
        unsafe { planner.install_storage_view() };
        Ok(())
    }

    unsafe fn estimate_storage(
        &self,
        relation_oid: pg_sys::Oid,
    ) -> Result<(), CustomScanError> {
        let relation = RelationGuard::open_table(relation_oid, pg_sys::NoLock as _)
            .map_err(PgReportError::from_pg_error)?;
        // SAFETY: PG allocated attr_widths for min_attr..max_attr. As in
        // plancat.c, the TableAM receives the pointer to attribute zero. The
        // relation guard stays outside the C ERROR boundary.
        unsafe {
            let rel = &mut *self.rel;
            pg_guard_ffi_boundary(|| {
                lagodb_partitioned_table_estimate_size(
                    relation.as_raw(),
                    rel.attr_widths.offset(-(rel.min_attr as isize)),
                    &mut rel.pages,
                    &mut rel.tuples,
                    &mut rel.allvisfrac,
                );
            });
        }
        Ok(())
    }

    unsafe fn install_storage_view(&self) {
        // Present provider partitioned tables as plain relations during standard
        // planning. Restrict our view to simple_rte_array: the
        // Query RTE retains catalog identity for provider matching, ModifyTable,
        // query offload admission, and setrefs' final executor range table.
        // A shallow copy borrows immutable Query fields in the same planner
        // context; only relkind and inh differ. ERROR cleanup frees it normally.
        unsafe {
            let view = pg_sys::palloc(size_of::<pg_sys::RangeTblEntry>())
                .cast::<pg_sys::RangeTblEntry>();
            view.write(*self.query_rte);
            (*view).relkind = pg_sys::RELKIND_RELATION as _;
            (*view).inh = false;
            *(*self.root)
                .simple_rte_array
                .add((*self.rel).relid as usize) = view;
        }
    }
}
