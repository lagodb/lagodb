//! Planner callbacks: `amcostestimate`, and the per-index facts
//! `get_relation_info` collects (`amcanreturn`, `amgettreeheight`).

use std::ffi::c_int;
use std::{mem, ptr};

use pgrx::{PgList, pg_guard, pg_sys};

use super::IndexAmResult;
use crate::diag::ReportableError;
use crate::handles::RelationHandle;

/// `amcostestimate`.
pub trait IndexPlanning {
    fn estimate_cost(ctx: &IndexCostContext<'_>) -> IndexAmResult<IndexCost>;
}

/// `amcanreturn`.
pub trait IndexReturnable {
    /// `attno` is 1-based.
    fn can_return(index: &RelationHandle<'_>, attno: i32) -> IndexAmResult<bool>;
}

/// `amgettreeheight`.
#[cfg(feature = "pg18")]
pub trait IndexTreeHeight {
    fn tree_height(index: &RelationHandle<'_>) -> IndexAmResult<i32>;
}

#[derive(Debug, Clone, Copy)]
pub struct IndexCost {
    pub startup: pg_sys::Cost,
    pub total: pg_sys::Cost,
    pub selectivity: pg_sys::Selectivity,
    pub correlation: f64,
    pub pages: f64,
}

impl IndexCost {
    pub fn from_generic(costs: &pg_sys::GenericCosts) -> Self {
        Self {
            startup: costs.indexStartupCost,
            total: costs.indexTotalCost,
            selectivity: costs.indexSelectivity,
            correlation: costs.indexCorrelation,
            pages: costs.numIndexPages,
        }
    }
}

pub struct IndexCostContext<'a> {
    root: *mut pg_sys::PlannerInfo,
    path: &'a pg_sys::IndexPath,
    loop_count: f64,
}

impl<'a> IndexCostContext<'a> {
    #[inline]
    pub fn root(&self) -> *mut pg_sys::PlannerInfo {
        self.root
    }

    #[inline]
    pub fn path(&self) -> &'a pg_sys::IndexPath {
        self.path
    }

    #[inline]
    pub fn index(&self) -> &'a pg_sys::IndexOptInfo {
        // SAFETY: every IndexPath names its index.
        unsafe { &*self.path.indexinfo }
    }

    #[inline]
    pub fn loop_count(&self) -> f64 {
        self.loop_count
    }

    /// The path's index clauses (`IndexClause` nodes).
    pub fn index_clauses(
        &self,
    ) -> impl Iterator<Item = &'a pg_sys::IndexClause> + 'a {
        // SAFETY: `indexclauses` is a List of IndexClause owned by the
        // planner for the call; PgList::from_pg does not take ownership.
        let clauses =
            unsafe { PgList::<pg_sys::IndexClause>::from_pg(self.path.indexclauses) };
        (0..clauses.len()).map(move |position| {
            let clause = clauses
                .get_ptr(position)
                .expect("position is below the list length");
            // SAFETY: as above.
            unsafe { &*clause }
        })
    }

    /// `genericcostestimate`; `None` derives the index tuples fetched from
    /// selectivity.
    pub fn generic_costs(
        &self,
        num_index_tuples: Option<f64>,
    ) -> pg_sys::GenericCosts {
        // SAFETY: GenericCosts is plain data; PostgreSQL reads only
        // numIndexTuples and fills the rest.
        let mut costs: pg_sys::GenericCosts = unsafe { mem::zeroed() };
        costs.numIndexTuples = num_index_tuples.unwrap_or(0.0);
        // SAFETY: the planner arguments are those of this callback.
        unsafe {
            pg_sys::genericcostestimate(
                self.root,
                ptr::from_ref(self.path).cast_mut(),
                self.loop_count,
                &mut costs,
            );
        }
        costs
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // PG IndexAmRoutine::amcostestimate ABI.
pub(super) extern "C-unwind" fn amcostestimate<A: IndexPlanning>(
    root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    startup: *mut pg_sys::Cost,
    total: *mut pg_sys::Cost,
    selectivity: *mut pg_sys::Selectivity,
    correlation: *mut f64,
    pages: *mut f64,
) {
    // SAFETY: the planner passes a live path and output locations.
    unsafe {
        let ctx = IndexCostContext {
            root,
            path: &*path,
            loop_count,
        };
        let cost = A::estimate_cost(&ctx).report_unwrap();
        *startup = cost.startup;
        *total = cost.total;
        *selectivity = cost.selectivity;
        *correlation = cost.correlation;
        *pages = cost.pages;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn amcanreturn<A: IndexReturnable>(
    index: pg_sys::Relation,
    attno: c_int,
) -> bool {
    // SAFETY: the planner passes an open index.
    let index = unsafe { RelationHandle::from_raw(index) };
    A::can_return(&index, attno).report_unwrap()
}

#[cfg(feature = "pg18")]
#[pg_guard]
pub(super) extern "C-unwind" fn amgettreeheight<A: IndexTreeHeight>(
    index: pg_sys::Relation,
) -> i32 {
    // SAFETY: the planner passes an open index.
    let index = unsafe { RelationHandle::from_raw(index) };
    A::tree_height(&index).report_unwrap()
}
