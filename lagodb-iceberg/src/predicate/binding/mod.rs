//! Executor-time value binding and bound Iceberg pruning/reader predicates.

mod binder;
mod datum;

use iceberg_lite::expr::Predicate;
use lagodb_core::expr::RuntimeValueBindings;
use lagodb_core::expr::pushdown::{FilterBindResult, PredicatePlan};

use crate::predicate::error::IcebergFilterError;
use crate::predicate::ir::IcebergPlannedPredicate;

use self::binder::PredicateBinder;

pub(crate) struct IcebergBoundPredicate {
    schema_id: i32,
    pruning: Predicate,
    row_filter: RowFilter,
}

/// What the Arrow reader evaluates for one bound predicate.
enum RowFilter {
    /// The pruning predicate as a whole; the tree holds no set.
    Whole,
    /// The tree with its sets projected out.
    Projected(Predicate),
    /// Nothing safe remains once its sets are projected out.
    Skipped,
}

impl RowFilter {
    fn into_plan(self, pruning: &Predicate) -> PredicatePlan<Predicate> {
        match self {
            Self::Whole => PredicatePlan::Exact(pruning.clone()),
            Self::Projected(predicate) => PredicatePlan::Partial(predicate),
            Self::Skipped => PredicatePlan::Unsupported,
        }
    }
}

impl IcebergBoundPredicate {
    const fn new(schema_id: i32, pruning: Predicate, row_filter: RowFilter) -> Self {
        Self {
            schema_id,
            pruning,
            row_filter,
        }
    }

    pub(crate) fn bind(
        predicate: &IcebergPlannedPredicate,
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<Self>, IcebergFilterError> {
        PredicateBinder::new(values).bind(predicate.schema_id(), predicate.root())
    }

    pub(crate) fn validate_schema<'a>(
        predicates: impl IntoIterator<Item = &'a Self>,
        execution_schema_id: i32,
    ) -> Result<(), IcebergFilterError> {
        for predicate in predicates {
            if predicate.schema_id != execution_schema_id {
                return Err(IcebergFilterError::SchemaMismatch {
                    planned: predicate.schema_id,
                    execution: execution_schema_id,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn conjoin_pruning<'a>(
        predicates: impl IntoIterator<Item = &'a Self>,
    ) -> Option<Predicate> {
        Self::conjoin_predicates(predicates.into_iter().map(|bound| &bound.pruning))
    }

    /// Conjoin the predicates the Arrow reader evaluates. A set stays with file
    /// pruning; `PredicateCapabilities` records why.
    ///
    /// iceberg-lite, like iceberg-rust, evaluates this filter on each file's
    /// own column types, before type promotion, and casts the literal down to
    /// the file's type (`try_cast_literal`). On a file written before an
    /// `int` -> `long`, `float` -> `double`, decimal precision or `date` ->
    /// `timestamp` promotion, an out-of-range literal becomes NULL and a double
    /// literal is rounded to real, so the filter can drop rows PostgreSQL
    /// keeps. Only foreign tables are exposed: managed tables reject
    /// `ALTER COLUMN TYPE`. The defect is upstream's; merge the fix into
    /// iceberg-lite once it lands rather than gating promoted columns here.
    ///
    /// https://github.com/apache/iceberg-rust/issues/3122
    /// https://github.com/apache/iceberg-rust/pull/3540
    pub(crate) fn conjoin_row_filter<'a>(
        predicates: impl IntoIterator<Item = &'a Self>,
    ) -> Option<Predicate> {
        Self::conjoin_predicates(predicates.into_iter().filter_map(Self::row_filter))
    }

    fn row_filter(&self) -> Option<&Predicate> {
        match &self.row_filter {
            RowFilter::Whole => Some(&self.pruning),
            RowFilter::Projected(predicate) => Some(predicate),
            RowFilter::Skipped => None,
        }
    }

    fn conjoin_predicates<'a>(
        mut predicates: impl Iterator<Item = &'a Predicate>,
    ) -> Option<Predicate> {
        let first = predicates.next()?.clone();
        Some(
            predicates.fold(first, |left, right| Predicate::and(left, right.clone())),
        )
    }
}
