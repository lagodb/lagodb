//! Transactional expression-planning state and dense source/input catalogs.

use core::ffi::c_int;
use std::mem;

use lagodb_core::expr::{
    ColumnRef, ExprType, RuntimeValueExpr, RuntimeValueId, RuntimeValueSpec,
};
use lagodb_core::query_contract::{OutputId, ScanId};
use pgrx::pg_sys;

use super::{
    ColumnRegistration, PlanCheckpoint, PredicateDomain, QueryExpressionPlanner,
};

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

/// One PostgreSQL planner scope with its dense RTI-to-ScanId mapping.
struct ExpressionSourceScope {
    root: *mut pg_sys::PlannerInfo,
    sources: Box<[Option<ScanId>]>,
    runtime_outer_relids: *mut pg_sys::Bitmapset,
}

/// Query-local source catalog. RTIs are only unique inside one PlannerInfo;
/// ScanIds remain unique across the complete offloaded relation tree.
pub(in crate::query_host::planning) struct ExpressionSourceCatalog {
    scopes: Vec<ExpressionSourceScope>,
}

impl ExpressionSourceCatalog {
    pub(in crate::query_host::planning) fn for_relations(
        root: *mut pg_sys::PlannerInfo,
        relations: &[(pg_sys::Index, ScanId)],
        runtime_outer_relids: *mut pg_sys::Bitmapset,
    ) -> Option<Self> {
        let mut catalog = Self { scopes: Vec::new() };
        catalog.add_relations(root, relations, runtime_outer_relids)?;
        Some(catalog)
    }

    pub(in crate::query_host::planning) fn add_relations(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        relations: &[(pg_sys::Index, ScanId)],
        runtime_outer_relids: *mut pg_sys::Bitmapset,
    ) -> Option<()> {
        if root.is_null() || self.scopes.iter().any(|scope| scope.root == root) {
            return None;
        }
        let maximum = relations.iter().map(|(rti, _)| *rti as usize).max()?;
        let mut sources = vec![None; maximum + 1];
        for &(rti, scan) in relations {
            if rti == 0 {
                return None;
            }
            let entry = &mut sources[rti as usize];
            if entry.replace(scan).is_some() {
                return None;
            }
        }
        self.scopes.push(ExpressionSourceScope {
            root,
            sources: sources.into_boxed_slice(),
            runtime_outer_relids,
        });
        Some(())
    }

    pub(super) fn resolve(
        &self,
        root: *mut pg_sys::PlannerInfo,
        varno: c_int,
    ) -> Option<ScanId> {
        let scope = self.scopes.iter().find(|scope| scope.root == root)?;
        usize::try_from(varno)
            .ok()
            .and_then(|index| scope.sources.get(index))
            .copied()
            .flatten()
    }

    pub(super) fn is_runtime_outer(
        &self,
        root: *mut pg_sys::PlannerInfo,
        varno: c_int,
    ) -> bool {
        varno > 0
            && self
                .scopes
                .iter()
                .find(|scope| scope.root == root)
                .is_some_and(|scope| unsafe {
                    pg_sys::bms_is_member(varno, scope.runtime_outer_relids)
                })
    }
}

#[derive(Clone, Copy)]
pub(in crate::query_host::planning) struct ResolvedOutput {
    pub(in crate::query_host::planning) output: OutputId,
    pub(in crate::query_host::planning) execution_type: ExprType,
}

pub(in crate::query_host::planning) trait OutputCatalog {
    /// # Safety
    ///
    /// `expression` must be a live planner-owned expression node.
    unsafe fn resolve_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<ResolvedOutput>;
}

#[derive(Clone, Copy)]
pub(in crate::query_host::planning) struct ExpressionScope<'a> {
    source_root: *mut pg_sys::PlannerInfo,
    outputs: Option<&'a dyn OutputCatalog>,
    predicate: Option<PredicateDomain>,
    scalar_domain: PredicateDomain,
}

impl<'a> ExpressionScope<'a> {
    pub(in crate::query_host::planning) const fn scalar(
        source_root: *mut pg_sys::PlannerInfo,
    ) -> Self {
        Self {
            source_root,
            outputs: None,
            predicate: None,
            scalar_domain: PredicateDomain::Exact,
        }
    }

    pub(in crate::query_host::planning) const fn predicate(
        source_root: *mut pg_sys::PlannerInfo,
        domain: PredicateDomain,
    ) -> Self {
        Self {
            source_root,
            outputs: None,
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(in crate::query_host::planning) const fn output_predicate(
        source_root: *mut pg_sys::PlannerInfo,
        outputs: &'a dyn OutputCatalog,
        domain: PredicateDomain,
    ) -> Self {
        Self {
            source_root,
            outputs: Some(outputs),
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(super) const fn as_scalar(self) -> Self {
        Self {
            source_root: self.source_root,
            outputs: self.outputs,
            predicate: None,
            scalar_domain: self.scalar_domain,
        }
    }

    pub(super) const fn with_predicate(self, domain: PredicateDomain) -> Self {
        Self {
            source_root: self.source_root,
            outputs: self.outputs,
            predicate: Some(domain),
            scalar_domain: domain,
        }
    }

    pub(super) const fn outputs(self) -> Option<&'a dyn OutputCatalog> {
        self.outputs
    }

    pub(super) const fn source_root(self) -> *mut pg_sys::PlannerInfo {
        self.source_root
    }

    pub(super) const fn predicate_domain(self) -> Option<PredicateDomain> {
        self.predicate
    }

    pub(super) const fn scalar_domain(self) -> PredicateDomain {
        self.scalar_domain
    }
}

impl QueryExpressionPlanner {
    pub(in crate::query_host::planning) fn attempt<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> ExpressionPlanResult<T>,
    ) -> ExpressionPlanResult<T> {
        let checkpoint = PlanCheckpoint {
            runtime_count: self.runtime_specs.len(),
            column_registration_count: self.column_registrations.len(),
        };
        let result = operation(self);
        if result.is_err() {
            self.runtime_exprs.truncate(checkpoint.runtime_count);
            self.runtime_specs.truncate(checkpoint.runtime_count);
            while self.column_registrations.len()
                > checkpoint.column_registration_count
            {
                let registration = self
                    .column_registrations
                    .pop()
                    .expect("registration length was checked above");
                self.columns_by_scan[registration.scan.index()][registration.index] =
                    None;
            }
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
            self.column_registrations.push(ColumnRegistration {
                scan: column.scan,
                index,
            });
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

    pub(in crate::query_host::planning) fn columns_for_scan(
        &self,
        scan: ScanId,
    ) -> Vec<ColumnRef> {
        self.columns_by_scan
            .get(scan.index())
            .into_iter()
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
