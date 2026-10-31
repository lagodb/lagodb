//! Query-level ordering over materialized query outputs.

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;

use super::ordering::{OrderingSemantics, SortDirection};
use super::{QueryNode, QueryPlanError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortExpr {
    output: OutputId,
    result_type: ExprType,
    direction: SortDirection,
    nulls_first: bool,
}

impl SortExpr {
    pub fn try_new(
        output: OutputId,
        result_type: ExprType,
        direction: SortDirection,
        nulls_first: bool,
    ) -> Result<Self, QueryPlanError> {
        if !OrderingSemantics::supports_query_sort(result_type) {
            return Err(QueryPlanError::UnsupportedSortKey);
        }
        Ok(Self {
            output,
            result_type,
            direction,
            nulls_first,
        })
    }

    #[inline]
    pub const fn output(self) -> OutputId {
        self.output
    }

    #[inline]
    pub const fn result_type(self) -> ExprType {
        self.result_type
    }

    #[inline]
    pub const fn direction(self) -> SortDirection {
        self.direction
    }

    #[inline]
    pub const fn nulls_first(self) -> bool {
        self.nulls_first
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortNode {
    input: Box<QueryNode>,
    keys: Box<[SortExpr]>,
}

impl SortNode {
    pub fn new(
        input: QueryNode,
        keys: Box<[SortExpr]>,
    ) -> Result<Self, QueryPlanError> {
        if keys.is_empty() {
            return Err(QueryPlanError::EmptySort);
        }
        Ok(Self {
            input: Box::new(input),
            keys,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn keys(&self) -> &[SortExpr] {
        &self.keys
    }
}
