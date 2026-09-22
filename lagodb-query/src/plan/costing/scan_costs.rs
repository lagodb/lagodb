//! Dense table-scan estimates consumed by plan costing.

use lagodb_core::query_contract::{ScanCost, ScanId};

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
