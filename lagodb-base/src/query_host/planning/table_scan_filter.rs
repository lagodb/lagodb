//! Query table-scan predicate placement and diagnostics.

mod deparse;

use std::ffi::CString;

use pgrx::pg_sys;

use crate::query_host::error::QueryHostError;

use super::expression::ScopedRuntimeBinding;
use deparse::FilterDeparser;

/// PostgreSQL source identity for a ScanNode predicate. The exact expression
/// is owned and always executed by the ScanNode; provider negotiation may only
/// use this source tree to reduce file input.
pub(super) struct TableScanFilter {
    source_expression: *mut pg_sys::Node,
}

impl TableScanFilter {
    pub(super) const fn new(source_expression: *mut pg_sys::Node) -> Self {
        Self { source_expression }
    }

    pub(super) const fn source_expression(&self) -> *mut pg_sys::Node {
        self.source_expression
    }

    pub(super) unsafe fn explain_texts(
        &self,
        pruning_expression: Option<*mut pg_sys::Expr>,
        root: *mut pg_sys::PlannerInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
        runtime_bindings: &[ScopedRuntimeBinding],
    ) -> Result<(CString, Option<CString>), QueryHostError> {
        let mut deparser = unsafe {
            FilterDeparser::new(
                runtime_bindings,
                root,
                range_table_index,
                range_table_entry,
            )
        };
        let exact = unsafe { deparser.deparse(self.source_expression) }?;
        let pruning = pruning_expression
            .map(|expression| unsafe { deparser.deparse(expression.cast()) })
            .transpose()?;
        Ok((exact, pruning))
    }
}
