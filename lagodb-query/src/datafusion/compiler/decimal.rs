//! Fixed decimal expressions lowered with explicit Arrow types.
//!
//! Planning proved one Decimal128 type for every NUMERIC engine value, equal
//! to the type DataFusion derives for it. Each arithmetic result is still
//! fixed to its planned type: DataFusion's simplifier replaces a non-NULL
//! `A * 0` with the bare zero literal and its narrower type. A cast to the
//! type the expression already has is removed by physical planning.

use arrow_buffer::i256;
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, Operator, binary_expr};
use lagodb_core::datum::{NumericRounding, NumericScaledValue};
use lagodb_core::expr::{RuntimeValue, RuntimeValueId, RuntimeValueSource};
use pgrx::pg_sys;

use crate::plan::{ComparisonKind, DecimalOperator, ExecutionExpr, FixedDecimalType};

use super::DataFusionPlanError;
use super::expression::DataFusionExpressionCompiler;

impl DataFusionExpressionCompiler<'_> {
    /// A constant bound exactly in its planned type.
    pub(super) fn decimal_value(
        &self,
        value: RuntimeValueId,
        decimal_type: FixedDecimalType,
    ) -> Result<Expr, DataFusionPlanError> {
        let Some(value) = self.bound_value(value)? else {
            return Ok(Self::typed_value(decimal_type.data_type()));
        };
        let invalid = Self::invalid_numeric(value);
        let metadata = value.metadata();
        if metadata.value_type.type_oid != pg_sys::NUMERICOID
            || metadata.source_kind != RuntimeValueSource::Constant
        {
            return Err(invalid());
        }
        let coefficient = if value.is_null() {
            None
        } else {
            // SAFETY: RuntimeValueState evaluated this NUMERIC constant in the
            // current executor context and retains it for plan compilation.
            match unsafe {
                decimal_type
                    .domain()
                    .bind_datum(value.datum(), NumericRounding::Floor)
            } {
                Ok(NumericScaledValue::Finite {
                    coefficient,
                    exact: true,
                }) => Some(
                    // A declared Decimal128 domain keeps coefficients in i128.
                    i256::from_be_bytes(coefficient)
                        .to_i128()
                        .ok_or_else(&invalid)?,
                ),
                _ => return Err(invalid()),
            }
        };
        Ok(Expr::Literal(
            Self::decimal_scalar(coefficient, decimal_type),
            None,
        ))
    }

    pub(super) fn decimal_arithmetic(
        &self,
        operator: DecimalOperator,
        left: &ExecutionExpr,
        right: &ExecutionExpr,
        result_type: FixedDecimalType,
    ) -> Result<Expr, DataFusionPlanError> {
        let left = self.decimal_operand(left)?;
        let right = self.decimal_operand(right)?;
        let operator = match operator {
            DecimalOperator::Add => Operator::Plus,
            DecimalOperator::Subtract => Operator::Minus,
            DecimalOperator::Multiply => Operator::Multiply,
            DecimalOperator::Divide => Operator::Divide,
        };
        Ok(cast(
            binary_expr(left, operator, right),
            result_type.data_type(),
        ))
    }

    /// Arrow's integer-to-decimal cast is exact for the integer's own digits.
    pub(super) fn integer_to_decimal(
        &self,
        value: &ExecutionExpr,
        result_type: FixedDecimalType,
    ) -> Result<Expr, DataFusionPlanError> {
        Ok(cast(self.compile(value)?, result_type.data_type()))
    }

    /// Compile one CASE/COALESCE branch in the conditional's common type,
    /// when it has one.
    pub(super) fn compile_branch(
        &self,
        branch: &ExecutionExpr,
        common: Option<FixedDecimalType>,
    ) -> Result<Expr, DataFusionPlanError> {
        let compiled = self.compile(branch)?;
        Ok(match (common, branch.decimal_type()) {
            (Some(common), Some(branch)) if branch != common => {
                cast(compiled, common.data_type())
            }
            _ => compiled,
        })
    }

    /// Compile the operands of a comparison. Fixed decimal operands of two
    /// types compare in their lossless common type, so DataFusion's decimal
    /// comparison coercion never chooses the width or scale. A fixed decimal
    /// compared with an operand of no fixed type compares in Float64:
    /// validation admits that pair only in HAVING, against an approximate
    /// Float64 result (`ScalarSemantics::numeric_repr_comparison`).
    pub(super) fn comparison_operands(
        &self,
        left: &ExecutionExpr,
        right: &ExecutionExpr,
    ) -> Result<(Expr, Expr), DataFusionPlanError> {
        match (left.decimal_type(), right.decimal_type()) {
            (Some(left_type), Some(right_type)) if left_type != right_type => {
                let common = left_type
                    .common(right_type)
                    .expect("validated decimal comparison has a common type")
                    .data_type();
                Ok((
                    self.compile_cast(left, &common)?,
                    self.compile_cast(right, &common)?,
                ))
            }
            (Some(_), None) => Ok((
                self.compile_cast(left, &DataType::Float64)?,
                self.compile(right)?,
            )),
            (None, Some(_)) => Ok((
                self.compile(left)?,
                self.compile_cast(right, &DataType::Float64)?,
            )),
            _ => Ok((self.compile(left)?, self.compile(right)?)),
        }
    }

    /// Compile a fixed decimal value cast to `data_type`, a lossless wider
    /// decimal or Float64. An integer in its decimal image is cast from the
    /// integer itself, one cast as DataFusion's own coercion applies.
    fn compile_cast(
        &self,
        value: &ExecutionExpr,
        data_type: &DataType,
    ) -> Result<Expr, DataFusionPlanError> {
        let value = match value {
            ExecutionExpr::IntegerToDecimal { value, .. } => &**value,
            value => value,
        };
        Ok(cast(self.compile(value)?, data_type.clone()))
    }

    /// Place an InitPlan's NUMERIC value in the domain of the operand's values
    /// ([`FixedDecimalType::value_domain`]). A value between two domain values
    /// is rounded in the direction that keeps the ordering (`<` and `>=` up,
    /// `<=` and `>` down) and decides equality outright; a value outside the
    /// domain or i128 decides the comparison for every non-NULL operand.
    pub(super) fn decimal_bound_comparison(
        &self,
        kind: ComparisonKind,
        operand: &ExecutionExpr,
        bound: RuntimeValueId,
        operand_type: FixedDecimalType,
    ) -> Result<Expr, DataFusionPlanError> {
        let operand = self.compile(operand)?;
        let Some(value) = self.bound_value(bound)? else {
            let placeholder = Self::typed_value(operand_type.data_type());
            return Ok(Self::comparison(kind, operand, placeholder));
        };
        let invalid = Self::invalid_numeric(value);
        let metadata = value.metadata();
        if metadata.value_type.type_oid != pg_sys::NUMERICOID
            || metadata.source_kind != RuntimeValueSource::ExecParam
        {
            return Err(invalid());
        }
        let literal = |coefficient| {
            Expr::Literal(Self::decimal_scalar(coefficient, operand_type), None)
        };
        if value.is_null() {
            return Ok(Self::comparison(kind, operand, literal(None)));
        }
        let rounding = match kind {
            ComparisonKind::Less | ComparisonKind::GreaterEqual => {
                NumericRounding::Ceiling
            }
            ComparisonKind::LessEqual
            | ComparisonKind::Greater
            | ComparisonKind::Equal
            | ComparisonKind::NotEqual => NumericRounding::Floor,
        };
        // SAFETY: RuntimeValueState evaluated this InitPlan parameter in the
        // current executor context and retains it for plan compilation.
        let bound = unsafe {
            operand_type
                .value_domain()
                .bind_datum(value.datum(), rounding)
        }
        .map_err(|_| invalid())?;
        let matches = match bound {
            NumericScaledValue::Finite { coefficient, exact } => {
                let coefficient = i256::from_be_bytes(coefficient);
                match coefficient.to_i128() {
                    Some(coefficient)
                        if exact
                            || !matches!(
                                kind,
                                ComparisonKind::Equal | ComparisonKind::NotEqual
                            ) =>
                    {
                        return Ok(Self::comparison(
                            kind,
                            operand,
                            literal(Some(coefficient)),
                        ));
                    }
                    // No operand value equals a value between two domain values.
                    Some(_) => kind == ComparisonKind::NotEqual,
                    // A 39-digit coefficient beyond i128 lies outside every
                    // operand value.
                    None => Self::outside_matches(kind, !coefficient.is_negative()),
                }
            }
            NumericScaledValue::Above => Self::outside_matches(kind, true),
            NumericScaledValue::Below => Self::outside_matches(kind, false),
        };
        // The same reflexive comparisons as StrictTrue/StrictFalse: a fixed
        // result for every non-NULL operand, UNKNOWN for NULL.
        Ok(if matches {
            operand.clone().eq(operand)
        } else {
            operand.clone().gt(operand)
        })
    }

    /// Whether every non-NULL operand satisfies `operand <kind> bound` for a
    /// bound above (or below) every operand value.
    fn outside_matches(kind: ComparisonKind, above: bool) -> bool {
        if above {
            matches!(
                kind,
                ComparisonKind::NotEqual
                    | ComparisonKind::Less
                    | ComparisonKind::LessEqual
            )
        } else {
            matches!(
                kind,
                ComparisonKind::NotEqual
                    | ComparisonKind::Greater
                    | ComparisonKind::GreaterEqual
            )
        }
    }

    /// Compile an operand planning proved a fixed decimal.
    fn decimal_operand(
        &self,
        operand: &ExecutionExpr,
    ) -> Result<Expr, DataFusionPlanError> {
        operand
            .decimal_type()
            .ok_or(DataFusionPlanError::UntypedDecimalOperand)?;
        self.compile(operand)
    }

    fn decimal_scalar(
        coefficient: Option<i128>,
        decimal_type: FixedDecimalType,
    ) -> ScalarValue {
        ScalarValue::Decimal128(
            coefficient,
            decimal_type.precision(),
            decimal_type.arrow_scale(),
        )
    }

    fn invalid_numeric(value: RuntimeValue) -> impl Fn() -> DataFusionPlanError {
        let oid = value.metadata().value_type.type_oid;
        move || DataFusionPlanError::InvalidRuntimeValue { oid }
    }
}
