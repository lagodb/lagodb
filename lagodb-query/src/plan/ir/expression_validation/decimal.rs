//! Fixed decimal typing of NUMERIC expressions in decoded query plans.
//!
//! Every NUMERIC expression keeps two types: PostgreSQL's value type, which
//! arithmetic leaves unconstrained (`numeric`), and the fixed Arrow decimal
//! execution type proven from its operands. These checks re-derive the
//! execution type from the expression structure, so a decoded plan cannot
//! claim a type whose values its operands could overflow. A comparison
//! operand may instead execute as an integer or Float64 ([`NumericRepr`]),
//! and HAVING `+ - *` over an approximate Float64 result computes in Float64.

use lagodb_core::expr::{ExprType, RuntimeValueId, RuntimeValueSource};
use pgrx::pg_sys;

use super::{NodeFacts, PlanExpressionValidator};
use crate::plan::ir::{
    DecimalOperator, ExecutionExpr, FixedDecimalType, NumericRepr, QueryPlanError,
    ScalarFunctionKind, ScalarSemantics,
};

impl PlanExpressionValidator<'_> {
    pub(super) fn validate_decimal_arithmetic(
        &self,
        operator: DecimalOperator,
        operands: [&ExecutionExpr; 2],
        result_type: FixedDecimalType,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<ExprType, QueryPlanError> {
        let [left, right] = operands.map(|operand| {
            self.validate_numeric(operand, facts, domain)?;
            operand
                .decimal_type()
                .ok_or(QueryPlanError::UnsupportedPredicate)
        });
        if left?.arithmetic(operator, right?) != Some(result_type) {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(Self::numeric_type())
    }

    pub(super) fn validate_integer_to_decimal(
        &self,
        value: &ExecutionExpr,
        result_type: FixedDecimalType,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<ExprType, QueryPlanError> {
        let input_type = self.expression_type(value, facts, domain)?;
        // An integral NUMERIC the engine holds as an integer, such as
        // `extract(year ...)`, converts as that integer.
        let image = if input_type.type_oid == pg_sys::NUMERICOID {
            match self.numeric_repr(value, facts) {
                Some(NumericRepr::Integer(image)) => Some(image),
                _ => None,
            }
        } else if input_type.typmod == -1
            && input_type.collation == pg_sys::InvalidOid
        {
            FixedDecimalType::for_integer(input_type.type_oid)
        } else {
            None
        };
        if image != Some(result_type) {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(Self::numeric_type())
    }

    /// HAVING `+ - *` computed in Float64 because an operand is an
    /// approximate Float64 result ([`NumericRepr::Float64`]); the other
    /// operand may be in any NUMERIC representation and is converted to
    /// Float64. Division declines: PostgreSQL raises on a zero divisor, where
    /// Float64 returns an infinity or NaN.
    pub(super) fn validate_float_numeric_arithmetic(
        &self,
        kind: ScalarFunctionKind,
        arguments: &[ExecutionExpr],
        input_collation: pg_sys::Oid,
        result_type: ExprType,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<ExprType, QueryPlanError> {
        let [left, right] = arguments else {
            return Err(QueryPlanError::UnsupportedPredicate);
        };
        if domain != ScalarSemantics::Having
            || kind == ScalarFunctionKind::Divide
            || input_collation != pg_sys::InvalidOid
            || result_type != Self::numeric_type()
        {
            return Err(QueryPlanError::UnsupportedPredicate);
        }
        self.expression_type(left, facts, domain)?;
        self.expression_type(right, facts, domain)?;
        let reprs = [
            self.numeric_repr(left, facts),
            self.numeric_repr(right, facts),
        ];
        if reprs.contains(&None) || !reprs.contains(&Some(NumericRepr::Float64)) {
            return Err(QueryPlanError::UnsupportedPredicate);
        }
        Ok(result_type)
    }

    /// The bound is a NUMERIC PARAM_EXEC; the planner admits only those an
    /// InitPlan sets, which no plan field records. The operand must have the
    /// fixed type the bound is placed in.
    pub(super) fn validate_decimal_bound_comparison(
        &self,
        operand: &ExecutionExpr,
        bound: RuntimeValueId,
        operand_type: FixedDecimalType,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<ExprType, QueryPlanError> {
        let spec = self
            .runtime_values
            .values()
            .get(bound.index())
            .ok_or(QueryPlanError::RuntimeValueOutOfBounds)?;
        if spec.value_type.type_oid != pg_sys::NUMERICOID
            || spec.value_type.collation != pg_sys::InvalidOid
            || spec.source_kind != RuntimeValueSource::ExecParam
        {
            return Err(QueryPlanError::UnsupportedRuntimeValueType);
        }
        self.validate_numeric(operand, facts, domain)?;
        if operand.decimal_type() != Some(operand_type) {
            return Err(QueryPlanError::UnsupportedPredicate);
        }
        Ok(Self::boolean_type())
    }

    /// CASE/COALESCE branches that all have fixed types execute in their
    /// common type. PostgreSQL keeps a typmod only when every branch shares
    /// it, in which case the common type is that typmod's type.
    pub(super) fn validate_decimal_branches<'e>(
        &self,
        branches: impl Iterator<Item = &'e ExecutionExpr>,
        common: FixedDecimalType,
        result_type: ExprType,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<(), QueryPlanError> {
        for branch in branches {
            self.validate_numeric(branch, facts, domain)?;
        }
        if result_type.collation != pg_sys::InvalidOid
            || (result_type.typmod != -1
                && FixedDecimalType::for_type(result_type) != Some(common))
        {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(())
    }

    /// Aggregate semantics are selected by the fixed execution type of a
    /// NUMERIC argument, such as the product of two bounded columns.
    pub(super) fn aggregate_argument_type(
        &self,
        argument: &ExecutionExpr,
        input: &NodeFacts,
    ) -> Result<ExprType, QueryPlanError> {
        let value_type =
            self.expression_type(argument, input, ScalarSemantics::Exact)?;
        if value_type.type_oid != pg_sys::NUMERICOID {
            return Ok(value_type);
        }
        Ok(argument
            .decimal_type()
            .map_or(value_type, FixedDecimalType::value_type))
    }

    /// The engine representation in which a NUMERIC comparison operand
    /// compares ([`ExecutionExpr::numeric_repr`]). The operand's validation
    /// has already resolved the output or runtime value it reads.
    pub(super) fn numeric_repr(
        &self,
        operand: &ExecutionExpr,
        facts: &NodeFacts,
    ) -> Option<NumericRepr> {
        operand.numeric_repr(|leaf| match *leaf {
            ExecutionExpr::Output(output) => facts
                .outputs
                .get(output.index())
                .and_then(|fact| *fact)
                .map(|fact| fact.execution_type),
            ExecutionExpr::Value(value) => self
                .runtime_values
                .values()
                .get(value.index())
                .map(|spec| spec.value_type),
            _ => None,
        })
    }

    fn validate_numeric(
        &self,
        expression: &ExecutionExpr,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<ExprType, QueryPlanError> {
        let value_type = self.expression_type(expression, facts, domain)?;
        if value_type.type_oid != pg_sys::NUMERICOID
            || value_type.collation != pg_sys::InvalidOid
        {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(value_type)
    }

    const fn numeric_type() -> ExprType {
        ExprType {
            type_oid: pg_sys::NUMERICOID,
            typmod: -1,
            collation: pg_sys::InvalidOid,
        }
    }
}
