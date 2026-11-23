//! Logical node for the NULL-sensitive anti SubPlans: `x NOT IN (subquery)`
//! and `x IS NULL OR x NOT IN (subquery)`.
//!
//! PostgreSQL's hashed SubPlan reads the whole subquery into its table, noting
//! whether it held any row and any NULL key (`buildSubPlanHash`), and only
//! then decides each outer row (`ExecHashSubPlan`). This node keeps that shape
//! through DataFusion's logical optimizer: the subquery is the build input and
//! the outer relation is probed. DataFusion's own null-aware anti join builds
//! the outer relation instead, and its anti Mark lowering becomes a nested
//! loop, so neither can hash the subquery or run under the adaptive join.

use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, DataFusionError, Result};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
};

use crate::plan::NullSensitiveAntiMode;

/// Outer rows of a NULL-sensitive anti SubPlan, keyed by one equality.
///
/// The output is the outer relation's schema. Predicates stay above: the
/// default `prevent_predicate_push_down_columns` names every column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::datafusion) struct NullSensitiveAntiJoin {
    mode: NullSensitiveAntiMode,
    outer: Arc<LogicalPlan>,
    subquery: Arc<LogicalPlan>,
    outer_key: Expr,
    subquery_key: Expr,
}

impl NullSensitiveAntiJoin {
    pub(super) fn plan(
        mode: NullSensitiveAntiMode,
        outer: LogicalPlan,
        subquery: LogicalPlan,
        outer_key: Expr,
        subquery_key: Expr,
    ) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Self {
                mode,
                outer: Arc::new(outer),
                subquery: Arc::new(subquery),
                outer_key,
                subquery_key,
            }),
        })
    }

    pub(in crate::datafusion) const fn mode(&self) -> NullSensitiveAntiMode {
        self.mode
    }

    pub(in crate::datafusion) const fn outer_key(&self) -> &Expr {
        &self.outer_key
    }

    pub(in crate::datafusion) const fn subquery_key(&self) -> &Expr {
        &self.subquery_key
    }
}

/// Plans are ordered only to satisfy `UserDefinedLogicalNodeCore`, as
/// DataFusion's own nodes are.
impl PartialOrd for NullSensitiveAntiJoin {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (
            self.mode,
            &self.outer_key,
            &self.subquery_key,
            &self.outer,
            &self.subquery,
        )
            .partial_cmp(&(
                other.mode,
                &other.outer_key,
                &other.subquery_key,
                &other.outer,
                &other.subquery,
            ))
    }
}

impl UserDefinedLogicalNodeCore for NullSensitiveAntiJoin {
    fn name(&self) -> &str {
        "NullSensitiveAntiJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.outer, &self.subquery]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.outer.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.outer_key.clone(), self.subquery_key.clone()]
    }

    fn fmt_for_explain(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "NullSensitiveAntiJoin: mode={:?}, on=[{} = {}]",
            self.mode, self.outer_key, self.subquery_key,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Ok([outer_key, subquery_key]), Ok([outer, subquery])) = (
            <[Expr; 2]>::try_from(exprs),
            <[LogicalPlan; 2]>::try_from(inputs),
        ) else {
            return Err(DataFusionError::Internal(
                "NullSensitiveAntiJoin takes two keys and two inputs".to_owned(),
            ));
        };
        Ok(Self {
            mode: self.mode,
            outer: Arc::new(outer),
            subquery: Arc::new(subquery),
            outer_key,
            subquery_key,
        })
    }

    /// The output is the outer input, so the parent's columns are the outer
    /// input's. Projection pruning adds each key's columns from
    /// `expressions` itself.
    fn necessary_children_exprs(
        &self,
        output_columns: &[usize],
    ) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec(), Vec::new()])
    }
}
