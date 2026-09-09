//! Planning-local table-scan identity and estimate tables.

use lagodb_core::query_contract::{ScanCost, ScanId};
use pgrx::pg_sys;

/// Planner scan catalog indexed directly by PostgreSQL RTI.
///
/// RTI zero remains the unused sentinel. The catalog is planning-local: the
/// serialized fragment stores only [`ScanId`] and therefore does not retain
/// `PlannerInfo` identity or confuse equal relation OIDs in later self joins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanCatalog {
    by_rti: Box<[Option<ScanId>]>,
}

impl ScanCatalog {
    pub fn for_relation(rti: pg_sys::Index) -> Self {
        let rti = rti as usize;
        let mut by_rti = vec![None; rti + 1];
        by_rti[rti] = Some(ScanId::from_index(0));
        Self {
            by_rti: by_rti.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn scan_for_rti(&self, rti: pg_sys::Index) -> Option<ScanId> {
        self.by_rti.get(rti as usize).copied().flatten()
    }
}

/// Dense scan estimates indexed directly by fragment-local [`ScanId`].
#[derive(Debug, Clone, PartialEq)]
pub struct ScanCostTable {
    by_scan: Box<[ScanCost]>,
}

impl ScanCostTable {
    pub fn from_dense(costs: Box<[ScanCost]>) -> Self {
        Self { by_scan: costs }
    }

    #[inline]
    pub fn cost(&self, scan: ScanId) -> Option<ScanCost> {
        self.by_scan.get(scan.index()).copied()
    }
}
