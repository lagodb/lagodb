//! Fixed decimal expressions lowered with explicit Arrow types.
//!
//! Planning proved one Arrow decimal type for every NUMERIC engine value.
//! Each operand is cast to its type in the result's width, so DataFusion's
//! implicit decimal coercion never selects another width or scale, and each
//! arithmetic result is fixed to its planned type: DataFusion's simplifier
//! replaces a non-NULL `A * 0` with the bare zero literal and its narrower
//! type. A cast to the type the expression already has is removed by physical
//! planning.

use arrow_buffer::i256;
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
                }) => Some(coefficient),
                _ => return Err(invalid()),
            }
        };
        Ok(Expr::Literal(
            Self::decimal_scalar(coefficient, decimal_type).ok_or_else(invalid)?,
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
        let left = self.decimal_operand(left, result_type)?;
        let right = self.decimal_operand(right, result_type)?;
        let operator = match operator {
            DecimalOperator::Add => Operator::Plus,
            DecimalOperator::Subtract => Operator::Minus,
            DecimalOperator::Multiply => Operator::Multiply,
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

    /// Place an InitPlan's NUMERIC value in the operand's domain. A value
    /// between two domain values is rounded in the direction that keeps the
    /// ordering (`<` and `>=` up, `<=` and `>` down) and decides equality
    /// outright; a value outside the domain decides the comparison for every
    /// non-NULL operand.
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
            Self::decimal_scalar(coefficient, operand_type)
                .map(|scalar| Expr::Literal(scalar, None))
                .ok_or_else(&invalid)
        };
        if value.is_null() {
            return Ok(Self::comparison(kind, operand, literal(None)?));
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
        let bound =
            unsafe { operand_type.domain().bind_datum(value.datum(), rounding) }
                .map_err(|_| invalid())?;
        let matches = match bound {
            NumericScaledValue::Finite { coefficient, exact }
                if exact
                    || !matches!(
                        kind,
                        ComparisonKind::Equal | ComparisonKind::NotEqual
                    ) =>
            {
                return Ok(Self::comparison(
                    kind,
                    operand,
                    literal(Some(coefficient))?,
                ));
            }
            // No operand value equals a value between two domain values.
            NumericScaledValue::Finite { .. } => kind == ComparisonKind::NotEqual,
            NumericScaledValue::Above => matches!(
                kind,
                ComparisonKind::NotEqual
                    | ComparisonKind::Less
                    | ComparisonKind::LessEqual
            ),
            NumericScaledValue::Below => matches!(
                kind,
                ComparisonKind::NotEqual
                    | ComparisonKind::Greater
                    | ComparisonKind::GreaterEqual
            ),
        };
        // The same reflexive comparisons as StrictTrue/StrictFalse: a fixed
        // result for every non-NULL operand, UNKNOWN for NULL.
        Ok(if matches {
            operand.clone().eq(operand)
        } else {
            operand.clone().gt(operand)
        })
    }

    /// Compile an operand in the Arrow width of the operation's result.
    fn decimal_operand(
        &self,
        operand: &ExecutionExpr,
        result_type: FixedDecimalType,
    ) -> Result<Expr, DataFusionPlanError> {
        let operand_type = operand
            .decimal_type()
            .ok_or(DataFusionPlanError::UntypedDecimalOperand)?;
        let compiled = self.compile(operand)?;
        Ok(
            if operand_type.is_decimal128() == result_type.is_decimal128() {
                compiled
            } else {
                cast(compiled, operand_type.data_type_in(result_type))
            },
        )
    }

    /// The scalar of a signed big-endian coefficient bound in `decimal_type`,
    /// whose domain keeps a Decimal128 coefficient within i128.
    fn decimal_scalar(
        coefficient: Option<[u8; 32]>,
        decimal_type: FixedDecimalType,
    ) -> Option<ScalarValue> {
        let coefficient = coefficient.map(i256::from_be_bytes);
        let (precision, scale) =
            (decimal_type.precision(), decimal_type.arrow_scale());
        Some(if decimal_type.is_decimal128() {
            let coefficient = match coefficient {
                Some(coefficient) => Some(coefficient.to_i128()?),
                None => None,
            };
            ScalarValue::Decimal128(coefficient, precision, scale)
        } else {
            ScalarValue::Decimal256(coefficient, precision, scale)
        })
    }

    fn invalid_numeric(value: RuntimeValue) -> impl Fn() -> DataFusionPlanError {
        let oid = value.metadata().value_type.type_oid;
        move || DataFusionPlanError::InvalidRuntimeValue { oid }
    }
}
