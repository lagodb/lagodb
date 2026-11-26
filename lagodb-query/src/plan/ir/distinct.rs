//! Query-level DISTINCT semantics and IR node.

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;

use super::{ExecutionExpr, QueryNode, QueryPlanError, RowEstimate, ScalarSemantics};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctExpr {
    expression: ExecutionExpr,
    result_type: ExprType,
    output: OutputId,
}

impl DistinctExpr {
    pub fn try_new(
        expression: ExecutionExpr,
        result_type: ExprType,
        output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        let expression_type = expression
            .result_type_hint()
            .ok_or(QueryPlanError::UnsupportedDistinctKey)?;
        if !matches!(&expression, ExecutionExpr::Column(_))
            || expression_type != result_type
            || !ScalarSemantics::Exact.supports_hash_group_key(result_type)
        {
            return Err(QueryPlanError::UnsupportedDistinctKey);
        }
        Ok(Self {
            expression,
            result_type,
            output,
        })
    }

    #[inline]
    pub const fn expression(&self) -> &ExecutionExpr {
        &self.expression
    }

    #[inline]
    pub const fn result_type(&self) -> ExprType {
        self.result_type
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctNode {
    input: Box<QueryNode>,
    keys: Box<[DistinctExpr]>,
    estimated_rows: RowEstimate,
}

impl DistinctNode {
    pub fn new(
        input: QueryNode,
        keys: Box<[DistinctExpr]>,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        if keys.is_empty() {
            return Err(QueryPlanError::EmptyDistinct);
        }
        Ok(Self {
            input: Box::new(input),
            keys,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn keys(&self) -> &[DistinctExpr] {
        &self.keys
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows.get()
    }
}
