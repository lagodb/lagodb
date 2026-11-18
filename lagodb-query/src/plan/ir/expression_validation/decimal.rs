//! Fixed decimal typing of NUMERIC expressions in decoded query plans.
//!
//! Every NUMERIC expression keeps two types: PostgreSQL's value type, which
//! arithmetic leaves unconstrained (`numeric`), and the fixed Arrow decimal
//! execution type proven from its operands. These checks re-derive the
//! execution type from the expression structure, so a decoded plan cannot
//! claim a type whose values its operands could overflow.

use lagodb_core::expr::{ExprType, RuntimeValueId, RuntimeValueSource};
use pgrx::pg_sys;

use super::{NodeFacts, PlanExpressionValidator};
use crate::plan::ir::{
    DecimalOperator, ExecutionExpr, FixedDecimalType, QueryPlanError,
};

impl PlanExpressionValidator<'_> {
    pub(super) fn validate_decimal_arithmetic(
        &self,
        operator: DecimalOperator,
        operands: [&ExecutionExpr; 2],
        result_type: FixedDecimalType,
        facts: &NodeFacts,
        allow_outputs: bool,
    ) -> Result<ExprType, QueryPlanError> {
        let [left, right] = operands.map(|operand| {
            self.validate_numeric(operand, facts, allow_outputs)?;
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
        allow_outputs: bool,
    ) -> Result<ExprType, QueryPlanError> {
        let input_type = self.expression_type(value, facts, allow_outputs)?;
        if input_type.typmod != -1
            || input_type.collation != pg_sys::InvalidOid
            || FixedDecimalType::for_integer(input_type.type_oid) != Some(result_type)
        {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(Self::numeric_type())
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
        allow_outputs: bool,
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
        let value_type = self.validate_numeric(operand, facts, allow_outputs)?;
        // An Output is typed by its aggregate's execution type.
        let actual = match operand {
            ExecutionExpr::Output(_) => FixedDecimalType::for_type(value_type),
            operand => operand.decimal_type(),
        };
        if actual != Some(operand_type) {
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
        allow_outputs: bool,
    ) -> Result<(), QueryPlanError> {
        for branch in branches {
            self.validate_numeric(branch, facts, allow_outputs)?;
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
        let value_type = self.expression_type(argument, input, false)?;
        if value_type.type_oid != pg_sys::NUMERICOID {
            return Ok(value_type);
        }
        Ok(argument
            .decimal_type()
            .map_or(value_type, FixedDecimalType::value_type))
    }

    pub(super) fn has_decimal_representation(
        expression: &ExecutionExpr,
        decimal: FixedDecimalType,
    ) -> bool {
        matches!(expression, ExecutionExpr::Output(_))
            || expression.decimal_type() == Some(decimal)
    }

    fn validate_numeric(
        &self,
        expression: &ExecutionExpr,
        facts: &NodeFacts,
        allow_outputs: bool,
    ) -> Result<ExprType, QueryPlanError> {
        let value_type = self.expression_type(expression, facts, allow_outputs)?;
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
