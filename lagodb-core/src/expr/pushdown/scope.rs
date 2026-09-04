//! Explicit PostgreSQL expression scopes used by the shared normalizer.

use pgrx::pg_sys;

use crate::expr::RuntimeValueSource;
use crate::query_contract::ScanId;

/// One fragment-local source indexed by PostgreSQL range-table index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceEntry {
    scan: ScanId,
}

impl SourceEntry {
    pub const fn new(scan: ScanId) -> Self {
        Self { scan }
    }

    #[inline]
    pub const fn scan(self) -> ScanId {
        self.scan
    }
}

/// Relation CustomScan/FDW scope.  The relation is always source zero because
/// a relation predicate is planned independently of any query fragment.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RelationExpressionScope {
    scan_relid: core::ffi::c_int,
}

impl RelationExpressionScope {
    pub(crate) const fn new(scan_relid: core::ffi::c_int) -> Self {
        Self { scan_relid }
    }

    pub(crate) const fn resolve_var(self, varno: core::ffi::c_int) -> VarResolution {
        if varno == self.scan_relid {
            VarResolution::Column(ScanId::from_index(0))
        } else {
            VarResolution::OuterValue
        }
    }

    pub(crate) const fn resolve_param(
        self,
        kind: pg_sys::ParamKind::Type,
    ) -> Option<RuntimeValueSource> {
        match kind {
            pg_sys::ParamKind::PARAM_EXTERN => {
                Some(RuntimeValueSource::ExternalParam)
            }
            pg_sys::ParamKind::PARAM_EXEC => Some(RuntimeValueSource::ExecParam),
            _ => None,
        }
    }
}

/// Query-fragment scope.  The boxed RTI-indexed table makes resolution a
/// bounds-checked array access during planning; no relation-OID lookup or map
/// is carried into execution.
#[derive(Debug, Clone)]
pub struct QueryExpressionScope {
    sources: Box<[Option<SourceEntry>]>,
}

impl QueryExpressionScope {
    pub fn new(sources: Box<[Option<SourceEntry>]>) -> Self {
        Self { sources }
    }

    pub fn for_relation(rti: pg_sys::Index, scan: ScanId) -> Self {
        let mut sources = vec![None; rti as usize + 1];
        sources[rti as usize] = Some(SourceEntry::new(scan));
        Self::new(sources.into_boxed_slice())
    }

    pub(crate) fn resolve_var(&self, varno: core::ffi::c_int) -> Option<ScanId> {
        usize::try_from(varno)
            .ok()
            .and_then(|index| self.sources.get(index))
            .and_then(|entry| *entry)
            .map(SourceEntry::scan)
    }
}

pub(crate) enum VarResolution {
    Column(ScanId),
    OuterValue,
}
