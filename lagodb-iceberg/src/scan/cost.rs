//! Path-stage cost of a scan through the shared Iceberg reader.

use lagodb_core::expr::pushdown::{PathFilterEstimates, PruningEstimate};
use pgrx::pg_sys;

use crate::config::{scan_cpu_cost_multiplier, scan_min_fraction};

/// Share of an Iceberg relation one scan reads, as the managed-table, FDW and
/// query-offload cost models charge it, floored by
/// `lagodb_iceberg.scan_min_fraction`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ScanVolume {
    fraction: f64,
}

impl ScanVolume {
    /// A managed table's ANALYZE samples rows in data-file order, so the
    /// correlation of the pruned columns prices min/max file and row-group
    /// pruning.
    pub(crate) fn managed(pruning: PruningEstimate) -> Self {
        Self::floored(pruning.scanned_fraction(), scan_min_fraction())
    }

    /// A foreign table's ANALYZE keeps a reservoir sample whose order is not
    /// data-file order. Its correlation is unknown, so min/max pruning is
    /// priced as pruning nothing, as `brincostestimate` prices a column
    /// without one. Only partition values prune: `layout_fraction` is the
    /// share of the relation they leave to scan.
    pub(crate) fn foreign(layout_fraction: f64) -> Self {
        Self::floored(layout_fraction, scan_min_fraction())
    }

    fn floored(fraction: f64, min_fraction: f64) -> Self {
        Self {
            fraction: fraction.max(min_fraction),
        }
    }

    #[inline]
    pub(crate) const fn fraction(self) -> f64 {
        self.fraction
    }
}

/// The reader's share of a PostgreSQL path over an Iceberg relation; the
/// adapter adds its page reads and source startup.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ReaderScanCost {
    /// Filter evaluation charged once per scan.
    pub(crate) startup: f64,
    /// Decoding the rows the reader reads and evaluating its per-row filters
    /// on each of them.
    pub(crate) run: f64,
    /// Rows the reader returns: those its per-row filters keep, never more
    /// than it reads.
    pub(crate) returned_tuples: f64,
}

impl ReaderScanCost {
    pub(crate) fn new(
        volume: ScanVolume,
        relation_tuples: f64,
        filters: &PathFilterEstimates<'_>,
    ) -> Self {
        let filter_cost = filters.provider_filter_cost();
        let read_tuples = relation_tuples * volume.fraction();
        // SAFETY: PostgreSQL initializes planner cost GUCs before path
        // generation, the only caller of scan costing.
        let cpu_tuple_cost = unsafe { pg_sys::cpu_tuple_cost };
        Self {
            startup: filter_cost.startup,
            run: read_tuples
                * (cpu_tuple_cost + filter_cost.per_tuple)
                * scan_cpu_cost_multiplier(),
            returned_tuples: (relation_tuples * filters.returned_selectivity())
                .min(read_tuples),
        }
    }
}
