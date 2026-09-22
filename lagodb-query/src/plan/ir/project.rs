//! Explicit query output identities and expressions.

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;

use super::{ExecutionExpr, QueryNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectExpr {
    expression: ExecutionExpr,
    result_type: ExprType,
    output: OutputId,
    nullable: bool,
}

impl ProjectExpr {
    pub const fn new(
        expression: ExecutionExpr,
        result_type: ExprType,
        output: OutputId,
        nullable: bool,
    ) -> Self {
        Self {
            expression,
            result_type,
            output,
            nullable,
        }
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

    #[inline]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectNode {
    input: Box<QueryNode>,
    expressions: Box<[ProjectExpr]>,
}

impl ProjectNode {
    pub fn new(input: QueryNode, expressions: Box<[ProjectExpr]>) -> Self {
        Self {
            input: Box::new(input),
            expressions,
        }
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn expressions(&self) -> &[ProjectExpr] {
        &self.expressions
    }

    /// Whether this node only forwards each semantic output under the same
    /// identity. Such a projection does not add a user-visible plan operation.
    pub fn is_identity(&self) -> bool {
        !self.expressions.is_empty()
            && self.expressions.iter().all(|expression| {
                matches!(
                    expression.expression(),
                    ExecutionExpr::Output(input) if *input == expression.output()
                )
            })
    }
}
