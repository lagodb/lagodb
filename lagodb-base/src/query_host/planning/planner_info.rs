//! Planner state whose `PlannerInfo` field differs across PostgreSQL versions.

use pgrx::pg_sys;

pub(super) trait PlannerInfoExt {
    /// Relids of the query's final join. PostgreSQL 16 added outer-join relids
    /// to join relids and names this set `all_query_rels`; before that it is
    /// `all_baserels`, which `make_one_rel` sets before any path hook runs.
    fn final_join_relids(&self) -> pg_sys::Relids;

    /// The preprocessed GROUP BY clauses. Before PostgreSQL 16 the planner
    /// stores them back into `parse->groupClause`.
    ///
    /// # Safety
    ///
    /// `parse` must point to the live Query being planned.
    unsafe fn processed_group_clause(&self) -> *mut pg_sys::List;
}

impl PlannerInfoExt for pg_sys::PlannerInfo {
    #[inline]
    fn final_join_relids(&self) -> pg_sys::Relids {
        #[cfg(feature = "pg15")]
        let relids = self.all_baserels;
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        let relids = self.all_query_rels;
        relids
    }

    #[inline]
    unsafe fn processed_group_clause(&self) -> *mut pg_sys::List {
        #[cfg(feature = "pg15")]
        let clauses = unsafe { (*self.parse).groupClause };
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        let clauses = self.processed_groupClause;
        clauses
    }
}
