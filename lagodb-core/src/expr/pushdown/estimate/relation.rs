//! PostgreSQL planner statistics for the filters a provider applies to one
//! base relation.

use core::ffi::c_int;
use core::ptr;

use pgrx::{error, pg_sys};

use crate::expr::contract::PushdownCosting;
use crate::expr::pg::expr_list;
use crate::expr::pushdown::RowFilter;

use super::{FilterEstimate, PruningEstimate, StatisticsPruningEstimate};

/// PostgreSQL planner statistics for the filters a provider applies to one
/// base relation.
#[derive(Debug, Clone, Copy)]
pub struct RelationFilterEstimator {
    root: *mut pg_sys::PlannerInfo,
    relid: pg_sys::Index,
}

impl RelationFilterEstimator {
    /// # Safety
    ///
    /// `root` must be live planner state whose range table holds base
    /// relation `relid`, and every expression passed to an estimate must be a
    /// live planner-owned qual of that relation.
    #[inline]
    pub unsafe fn new(root: *mut pg_sys::PlannerInfo, relid: pg_sys::Index) -> Self {
        Self { root, relid }
    }

    /// Selectivity and evaluation cost of `exprs` as one implicit AND.
    pub(super) fn filters(
        &self,
        exprs: impl IntoIterator<Item = *mut pg_sys::Expr>,
    ) -> FilterEstimate {
        let clauses = expr_list(exprs);
        if clauses.is_null() {
            return FilterEstimate::NONE;
        }
        let selectivity = self.clauselist_selectivity(clauses);
        let mut cost = pg_sys::QualCost::default();
        unsafe { pg_sys::cost_qual_eval(&mut cost, clauses, self.root) };
        // The list owns only its cells; the expressions stay planner-owned.
        unsafe { pg_sys::list_free(clauses) };
        FilterEstimate {
            selectivity,
            startup_cost: cost.startup,
            per_tuple_cost: cost.per_tuple,
        }
    }

    /// Evaluation cost of the filters a provider executes, each a pushed
    /// expression and the part of it, if any, the provider evaluates on each
    /// row it reads, as `cost_index` charges the quals an index handles apart
    /// from its qpquals (costsize.c).
    ///
    /// The rest of a filter is bound once per scan and then compared with
    /// each storage unit's statistics. Its startup plus one per-row
    /// evaluation stands in for the binding, which walks the whole set, and
    /// is all startup: `index_other_operands_eval_cost` likewise charges
    /// once-per-scan work to startup (selfuncs.c), and a rescan binds again
    /// with the path's startup. The comparison per pruned unit is not
    /// charged, as path costing charges nothing per file. A derived part
    /// shares the pushed expression's nodes, and `cost_qual_eval` charges no
    /// Boolean node, so the rest costs the difference of the two.
    pub(super) fn provider_filter_cost(
        &self,
        filters: impl IntoIterator<Item = (*mut pg_sys::Expr, RowFilter)>,
    ) -> pg_sys::QualCost {
        let mut total = pg_sys::QualCost::default();
        for (pushed_expr, row_filter) in filters {
            let pushed = self.qual_cost(pushed_expr);
            let per_row = match row_filter {
                RowFilter::Whole => pushed.per_tuple,
                RowFilter::Part(expr) => self.qual_cost(expr).per_tuple,
                RowFilter::Nothing => 0.0,
            };
            total.startup += pushed.startup + (pushed.per_tuple - per_row);
            total.per_tuple += per_row;
        }
        total
    }

    /// Evaluation cost of one expression; `cost_qual_eval` sums the same walk
    /// over each qual of a list.
    fn qual_cost(&self, expr: *mut pg_sys::Expr) -> pg_sys::QualCost {
        let mut cost = pg_sys::QualCost::default();
        unsafe { pg_sys::cost_qual_eval_node(&mut cost, expr.cast(), self.root) };
        cost
    }

    /// Selectivity of `exprs` as one implicit AND, 1 without any.
    pub(super) fn selectivity(
        &self,
        exprs: impl IntoIterator<Item = *mut pg_sys::Expr>,
    ) -> f64 {
        self.list_selectivity(expr_list(exprs)).unwrap_or(1.0)
    }

    /// Selectivity of the layout-pruned filters among `filters`, the share of
    /// storage units their layout leaves to scan.
    pub fn layout_selectivity(
        &self,
        filters: impl IntoIterator<Item = (*mut pg_sys::Expr, PushdownCosting)>,
    ) -> f64 {
        self.selectivity(filters.into_iter().filter_map(|(expr, costing)| {
            (costing == PushdownCosting::LayoutPruning).then_some(expr)
        }))
    }

    /// Scan share left by the costed pruning filters among `filters` on a
    /// relation of `units` pruning units.
    pub fn pruning(
        &self,
        filters: impl IntoIterator<Item = (*mut pg_sys::Expr, PushdownCosting)>,
        units: Option<f64>,
    ) -> PruningEstimate {
        let mut layout = ptr::null_mut();
        let mut statistics = ptr::null_mut();
        let mut columns = ptr::null_mut();
        for (expr, costing) in filters {
            match costing {
                PushdownCosting::UncostedBestEffort => {}
                PushdownCosting::LayoutPruning => {
                    layout = unsafe { pg_sys::lappend(layout, expr.cast()) };
                }
                PushdownCosting::StatisticsPruning => unsafe {
                    pg_sys::pull_varattnos(expr.cast(), self.relid, &mut columns);
                    statistics = pg_sys::lappend(statistics, expr.cast());
                },
            }
        }
        let layout_selectivity = self.list_selectivity(layout).unwrap_or(1.0);
        let statistics = self.list_selectivity(statistics).map(|selectivity| {
            StatisticsPruningEstimate {
                selectivity,
                correlation: self.largest_correlation(columns),
            }
        });
        unsafe { pg_sys::bms_free(columns) };
        PruningEstimate {
            layout_selectivity,
            statistics,
            units,
        }
    }

    /// Selectivity of a non-empty implicit-AND `clauses` list, which this call
    /// frees.
    fn list_selectivity(&self, clauses: *mut pg_sys::List) -> Option<f64> {
        if clauses.is_null() {
            return None;
        }
        let selectivity = self.clauselist_selectivity(clauses);
        // The list owns only its cells; the expressions stay planner-owned.
        unsafe { pg_sys::list_free(clauses) };
        Some(selectivity)
    }

    fn clauselist_selectivity(&self, clauses: *mut pg_sys::List) -> f64 {
        unsafe {
            pg_sys::clauselist_selectivity(
                self.root,
                clauses,
                self.relid as c_int,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        }
        .clamp(0.0, 1.0)
    }

    /// `brincostestimate` takes the largest absolute correlation among the
    /// qual columns, because the scan can use every qual equally, and 0 when
    /// none has a correlation statistic.
    fn largest_correlation(&self, columns: *mut pg_sys::Bitmapset) -> f64 {
        let rte = unsafe { *(*self.root).simple_rte_array.add(self.relid as usize) };
        let mut largest: f64 = 0.0;
        let mut member = -1;
        loop {
            member = unsafe { pg_sys::bms_next_member(columns, member) };
            if member < 0 {
                return largest;
            }
            let attno = (member + pg_sys::FirstLowInvalidHeapAttributeNumber)
                as pg_sys::AttrNumber;
            if let Some(correlation) = self.column_correlation(rte, attno) {
                largest = largest.max(correlation);
            }
        }
    }

    /// Absolute ANALYZE correlation of one column, looked up as
    /// `brincostestimate` does for a simple index column.
    fn column_correlation(
        &self,
        rte: *mut pg_sys::RangeTblEntry,
        attno: pg_sys::AttrNumber,
    ) -> Option<f64> {
        let mut vardata = pg_sys::VariableStatData::default();
        let hooked = unsafe { pg_sys::get_relation_stats_hook }
            .is_some_and(|hook| unsafe { hook(self.root, rte, attno, &mut vardata) });
        if !hooked {
            let tuple = unsafe {
                pg_sys::SearchSysCache3(
                    pg_sys::SysCacheIdentifier::STATRELATTINH as c_int,
                    pg_sys::Datum::from((*rte).relid),
                    pg_sys::Datum::from(attno),
                    pg_sys::Datum::from(false),
                )
            };
            let correlation = Self::tuple_correlation(tuple);
            if !tuple.is_null() {
                unsafe { pg_sys::ReleaseSysCache(tuple) };
            }
            return correlation;
        }
        // The hook took control of acquiring a stats tuple. As in
        // `brincostestimate`, a tuple it supplies must come with a freefunc,
        // and `ReleaseVariableStats` calls it only for a valid tuple.
        let tuple = vardata.statsTuple;
        if tuple.is_null() {
            return None;
        }
        let Some(release) = vardata.freefunc else {
            error!("no function provided to release variable stats with");
        };
        let correlation = Self::tuple_correlation(tuple);
        unsafe { release(tuple) };
        correlation
    }

    /// The correlation slot of one `pg_statistic` tuple, if it has one.
    fn tuple_correlation(tuple: pg_sys::HeapTuple) -> Option<f64> {
        if tuple.is_null() {
            return None;
        }
        let mut slot = pg_sys::AttStatsSlot::default();
        let found = unsafe {
            pg_sys::get_attstatsslot(
                &mut slot,
                tuple,
                pg_sys::STATISTIC_KIND_CORRELATION as c_int,
                pg_sys::InvalidOid,
                pg_sys::ATTSTATSSLOT_NUMBERS as c_int,
            )
        };
        found.then(|| {
            let correlation = if slot.nnumbers > 0 {
                f64::from(unsafe { *slot.numbers }).abs()
            } else {
                0.0
            };
            unsafe { pg_sys::free_attstatsslot(&mut slot) };
            correlation
        })
    }
}
