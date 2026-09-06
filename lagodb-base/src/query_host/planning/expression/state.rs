//! Transactional expression-planning state and dense source/input catalogs.

use core::ffi::c_int;
use std::mem;

use lagodb_core::expr::{
    ColumnRef, ExprType, RuntimeValueExpr, RuntimeValueId, RuntimeValueSpec,
};
use lagodb_core::query_contract::{OutputId, ScanId};
use pgrx::pg_sys;

use super::{PlanCheckpoint, PredicateDomain, QueryExpressionPlanner};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::query_host::planning) enum ExpressionDecline {
    InvalidShape,
    UnsupportedNode(pg_sys::NodeTag),
    UnsupportedType(pg_sys::Oid),
    UnsupportedSemantics,
    UnsupportedSource,
    UnsupportedRuntimeSource,
    UnsupportedPostgresFallback,
}

pub(in crate::query_host::planning) type ExpressionPlanResult<T> =
    Result<T, ExpressionDecline>;

/// Query-local RTI to ScanId mapping. PostgreSQL attribute numbers remain the
/// inner dense index in `QueryExpressionPlanner::columns_by_scan`.
pub(in crate::query_host::planning) struct ExpressionSourceCatalog {
    sources: Box<[Option<ScanId>]>,
}

impl ExpressionSourceCatalog {
    pub(in crate::query_host::planning) fn for_relation(
        rti: pg_sys::Index,
        scan: ScanId,
    ) -> Self {
        let mut sources = vec![None; rti as usize + 1];
        sources[rti as usize] = Some(scan);
        Self {
            sources: sources.into_boxed_slice(),
        }
    }

    pub(super) fn resolve(&self, varno: c_int) -> Option<ScanId> {
        usize::try_from(varno)
            .ok()
            .and_then(|index| self.sources.get(index))
            .copied()
            .flatten()
    }
}

pub(in crate::query_host::planning) trait OutputCatalog {
    /// # Safety
    ///
    /// `expression` must be a live planner-owned expression node.
    unsafe fn resolve_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<OutputId>;
}

#[derive(Clone, Copy)]
pub(in crate::query_host::planning) struct ExpressionScope<'a> {
    outputs: Option<&'a dyn OutputCatalog>,
    predicate: Option<PredicateDomain>,
    scalar_domain: PredicateDomain,
}

impl<'a> ExpressionScope<'a> {
    pub(in crate::query_host::planning) const fn scalar() -> Self {
        Self {
            outputs: None,
            predicate: None,
            scalar_domain: PredicateDomain::Exact,
        }
    }

    pub(in crate::query_host::planning) const fn predicate(
        domain: PredicateDomain,
    ) -> Self {
        Self {
            outputs: None,
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(in crate::query_host::planning) const fn output_predicate(
        outputs: &'a dyn OutputCatalog,
        domain: PredicateDomain,
    ) -> Self {
        Self {
            outputs: Some(outputs),
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(super) const fn as_scalar(self) -> Self {
        Self {
            outputs: self.outputs,
            predicate: None,
            scalar_domain: self.scalar_domain,
        }
    }

    pub(super) const fn with_predicate(self, domain: PredicateDomain) -> Self {
        Self {
            outputs: self.outputs,
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(super) const fn outputs(self) -> Option<&'a dyn OutputCatalog> {
        self.outputs
    }

    pub(super) const fn predicate_domain(self) -> Option<PredicateDomain> {
        self.predicate
    }

    pub(super) const fn scalar_domain(self) -> PredicateDomain {
        self.scalar_domain
    }
}

impl QueryExpressionPlanner {
    pub(super) fn attempt<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> ExpressionPlanResult<T>,
    ) -> ExpressionPlanResult<T> {
        let checkpoint = PlanCheckpoint {
            runtime_count: self.runtime_specs.len(),
            columns_by_scan: self.columns_by_scan.clone(),
        };
        let result = operation(self);
        if result.is_err() {
            self.runtime_exprs.truncate(checkpoint.runtime_count);
            self.runtime_specs.truncate(checkpoint.runtime_count);
            self.columns_by_scan = checkpoint.columns_by_scan;
        }
        result
    }

    pub(super) fn record_column(
        &mut self,
        column: ColumnRef,
    ) -> ExpressionPlanResult<()> {
        let index = usize::try_from(column.attno)
            .ok()
            .and_then(|attno| attno.checked_sub(1))
            .ok_or(ExpressionDecline::InvalidShape)?;
        if self.columns_by_scan.len() <= column.scan.index() {
            self.columns_by_scan
                .resize_with(column.scan.index() + 1, Vec::new);
        }
        let columns = &mut self.columns_by_scan[column.scan.index()];
        if columns.len() <= index {
            columns.resize(index + 1, None);
        }
        if let Some(existing) = columns[index] {
            if existing.declared_type != column.declared_type {
                return Err(ExpressionDecline::UnsupportedSemantics);
            }
        } else {
            columns[index] = Some(column);
        }
        Ok(())
    }

    pub(super) fn push_runtime(
        &mut self,
        expression: *mut pg_sys::Expr,
        spec: RuntimeValueSpec,
    ) -> RuntimeValueId {
        let id = RuntimeValueId::from_index(self.runtime_specs.len());
        self.runtime_specs.push(spec);
        self.runtime_exprs
            .push(RuntimeValueExpr::new(expression, spec));
        id
    }

    pub(in crate::query_host::planning) fn columns(&self) -> Vec<ColumnRef> {
        self.columns_by_scan
            .iter()
            .flatten()
            .filter_map(|column| *column)
            .collect()
    }

    pub(in crate::query_host::planning) fn take_runtime_layout(
        &mut self,
    ) -> Box<[RuntimeValueSpec]> {
        mem::take(&mut self.runtime_specs).into_boxed_slice()
    }

    pub(in crate::query_host::planning) fn take_runtime_exprs(
        &mut self,
    ) -> Vec<RuntimeValueExpr> {
        mem::take(&mut self.runtime_exprs)
    }

    pub(in crate::query_host::planning) fn expr_type(
        expression: *mut pg_sys::Expr,
    ) -> ExprType {
        ExprType {
            type_oid: unsafe { pg_sys::exprType(expression.cast()) },
            typmod: unsafe { pg_sys::exprTypmod(expression.cast()) },
            collation: unsafe { pg_sys::exprCollation(expression.cast()) },
        }
    }
}
