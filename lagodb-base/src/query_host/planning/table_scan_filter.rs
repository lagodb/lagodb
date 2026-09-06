//! Query table-scan predicate placement and diagnostics.

use std::ffi::{CString, c_void};

use lagodb_core::expr::explain::deparse_and_join;
use lagodb_query::plan::ExecutionExpr;
use pgrx::pg_sys;

use crate::query_host::error::QueryHostError;

/// The exact expression is always executed by DataFusion. Pruning candidates
/// are negotiated by the provider and may only reduce file input.
pub(super) struct TableScanFilter {
    exact_residual: ExecutionExpr,
    source_expression: *mut pg_sys::Node,
}

impl TableScanFilter {
    pub(super) fn new(
        exact_residual: ExecutionExpr,
        source_expression: *mut pg_sys::Node,
    ) -> Self {
        Self {
            exact_residual,
            source_expression,
        }
    }

    pub(super) const fn exact_residual(&self) -> &ExecutionExpr {
        &self.exact_residual
    }

    pub(super) const fn source_expression(&self) -> *mut pg_sys::Node {
        self.source_expression
    }

    unsafe fn deparse(
        source_expression: *mut pg_sys::Node,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Result<CString, QueryHostError> {
        let expression = unsafe {
            pg_sys::copyObjectImpl(source_expression.cast::<c_void>())
                .cast::<pg_sys::Node>()
        };
        unsafe {
            pg_sys::ChangeVarNodes(expression, range_table_index as i32, 1, 0);
        }
        let range_table_entry = unsafe { &*range_table_entry };
        let alias = unsafe { &*range_table_entry.eref };
        let context = unsafe {
            pg_sys::deparse_context_for(alias.aliasname, range_table_entry.relid)
        };
        let expressions = if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_List
        {
            let list = expression.cast::<pg_sys::List>();
            let count = unsafe { pg_sys::list_length(list) };
            (0..count)
                .map(|index| unsafe { pg_sys::list_nth(list, index) }.cast())
                .collect::<Vec<*mut pg_sys::Expr>>()
        } else {
            vec![expression.cast()]
        };
        unsafe { deparse_and_join(context, expressions) }.ok_or_else(|| {
            QueryHostError::invalid_plan(
                "PostgreSQL could not deparse a table-scan filter",
            )
        })
    }

    pub(super) unsafe fn explain_texts(
        &self,
        pruning_expression: Option<*mut pg_sys::Expr>,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Result<(CString, Option<CString>), QueryHostError> {
        let exact = unsafe {
            Self::deparse(
                self.source_expression,
                range_table_index,
                range_table_entry,
            )
        }?;
        let pruning = pruning_expression
            .map(|expression| unsafe {
                Self::deparse(expression.cast(), range_table_index, range_table_entry)
            })
            .transpose()?;
        Ok((exact, pruning))
    }
}
