//! PostgreSQL statistics estimates for provider-planned filters.

mod path;
mod relation;

pub use path::PathFilterEstimates;
pub use relation::RelationFilterEstimator;

/// PostgreSQL's selectivity and evaluation cost for provider-side filters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FilterEstimate {
    /// Fraction of input rows expected to survive the filters.
    pub selectivity: f64,
    /// One-time expression evaluation cost.
    pub startup_cost: f64,
    /// Expression evaluation cost charged for each input row.
    pub per_tuple_cost: f64,
}

impl FilterEstimate {
    pub const NONE: Self = Self {
        selectivity: 1.0,
        startup_cost: 0.0,
        per_tuple_cost: 0.0,
    };
}

/// Share of a relation that costed pruning filters leave to scan.
///
/// Layout and statistics pruning are priced apart and their shares multiply,
/// as `clauselist_selectivity` multiplies independent clauses: the layout
/// keeps whole units by partition value, and statistics pruning then skips
/// units among those it keeps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PruningEstimate {
    /// Selectivity of the layout-pruned filters, the share of units whose
    /// layout can hold qualifying rows.
    layout_selectivity: f64,
    /// The statistics-pruned filters, `None` without any.
    statistics: Option<StatisticsPruningEstimate>,
    /// See [`super::FilterPushdownPlanner::pruning_units`].
    units: Option<f64>,
}

/// Statistics-pruned filters priced as `brincostestimate` prices a BRIN scan.
#[derive(Debug, Clone, Copy, PartialEq)]
struct StatisticsPruningEstimate {
    selectivity: f64,
    /// Largest absolute ANALYZE correlation among the filters' columns, or 0
    /// when none has one, which `brincostestimate` also assumes.
    correlation: f64,
}

impl PruningEstimate {
    pub const NONE: Self = Self {
        layout_selectivity: 1.0,
        statistics: None,
        units: None,
    };

    /// Share of the relation's storage units the layout-pruned filters leave
    /// to scan. A relation whose statistics do not follow its physical order
    /// prices its scan by this alone, as if statistics pruned nothing.
    #[inline]
    pub const fn layout_fraction(self) -> f64 {
        self.layout_selectivity
    }

    /// Share of the relation's storage units all costed filters leave to
    /// scan, the statistics-pruned share taken among the units the layout
    /// leaves.
    pub fn scanned_fraction(self) -> f64 {
        let layout = self.layout_selectivity;
        self.statistics.map_or(layout, |statistics| {
            // The layout leaves at least one unit to scan.
            let kept_units = self.units.map(|units| (units * layout).max(1.0));
            layout * statistics.scanned_fraction(kept_units)
        })
    }
}

impl StatisticsPruningEstimate {
    /// Share of `units` the filters leave to scan. The qualifying rows fill at
    /// least `ceil(units × selectivity)` units, which spread over
    /// `1 / correlation` times as many, up to every unit once the columns are
    /// uncorrelated with physical order (selfuncs.c, `brincostestimate`).
    fn scanned_fraction(self, units: Option<f64>) -> f64 {
        if self.correlation < 1.0e-10 {
            return 1.0;
        }
        match units {
            Some(units) => {
                ((units * self.selectivity).ceil() / self.correlation).min(units)
                    / units
            }
            None => (self.selectivity / self.correlation).min(1.0),
        }
    }
}
