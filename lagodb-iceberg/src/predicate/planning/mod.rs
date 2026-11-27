//! Relation-scoped PostgreSQL filter pushdown planning for Iceberg tables.

mod admission;
mod costing;
mod planner;

pub(crate) use planner::IcebergFilterPlanner;

/// Relation-to-Iceberg storage invariants established by the owning table adapter.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PredicateStorageContract {
    /// Managed tables keep PostgreSQL relation columns synchronized with the
    /// current Iceberg schema, pad `character(n)` values on write, and expose
    /// manifest-derived pruning units.
    Managed { pruning_units: f64 },
    /// Foreign tables bind an external Iceberg table whose current schema can
    /// diverge from a historical foreign-table declaration until execution
    /// validates the read contract, and whose `string` columns carry no
    /// blank-padding invariant for `character(n)`.
    Foreign,
}

impl PredicateStorageContract {
    #[inline]
    const fn allows_padded_bpchar(self) -> bool {
        matches!(self, Self::Managed { .. })
    }

    #[inline]
    const fn pruning_units(self) -> Option<f64> {
        match self {
            Self::Managed { pruning_units } => Some(pruning_units),
            // Foreign scans price layout pruning alone (`ScanVolume::foreign`),
            // which needs no unit count.
            Self::Foreign => None,
        }
    }
}
