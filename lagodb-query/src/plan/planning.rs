//! Planning-local source identity and estimate tables.

use lagodb_core::query_contract::{SourceEstimate, SourceId};
use pgrx::pg_sys;

/// Construction errors for dense planning-local source tables.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SourceCatalogError {
    #[error("range-table index zero cannot identify a query source")]
    InvalidRangeTableIndex,
    #[error("the S1M source identity must be zero, found {index}")]
    InvalidSingleSourceIdentity { index: usize },
}

/// One-source planner catalog indexed directly by PostgreSQL RTI.
///
/// RTI zero remains the unused sentinel. The catalog is planning-local: the
/// serialized fragment stores only [`SourceId`] and therefore does not retain
/// `PlannerInfo` identity or confuse equal relation OIDs in later self joins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCatalog {
    by_rti: Box<[Option<SourceId>]>,
}

impl SourceCatalog {
    pub fn for_single_source(rti: pg_sys::Index) -> Result<Self, SourceCatalogError> {
        let rti = rti as usize;
        if rti == 0 {
            return Err(SourceCatalogError::InvalidRangeTableIndex);
        }
        let mut by_rti = vec![None; rti + 1];
        by_rti[rti] = Some(SourceId::from_index(0));
        Ok(Self {
            by_rti: by_rti.into_boxed_slice(),
        })
    }

    #[inline]
    pub fn source_for_rti(&self, rti: pg_sys::Index) -> Option<SourceId> {
        self.by_rti.get(rti as usize).copied().flatten()
    }

    #[inline]
    pub const fn source_count(&self) -> usize {
        1
    }
}

/// Dense source estimates indexed directly by fragment-local [`SourceId`].
#[derive(Debug, Clone, PartialEq)]
pub struct SourceEstimateTable {
    by_source: Box<[SourceEstimate]>,
}

impl SourceEstimateTable {
    pub fn for_single_source(
        source: SourceId,
        estimate: SourceEstimate,
    ) -> Result<Self, SourceCatalogError> {
        if source.index() != 0 {
            return Err(SourceCatalogError::InvalidSingleSourceIdentity {
                index: source.index(),
            });
        }
        Ok(Self {
            by_source: Box::new([estimate]),
        })
    }

    #[inline]
    pub fn estimate(&self, source: SourceId) -> Option<SourceEstimate> {
        self.by_source.get(source.index()).copied()
    }
}
