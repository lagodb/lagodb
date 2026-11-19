//! Plan-data codec for exact execution expressions.

use std::sync::Arc;

use lagodb_core::expr::{
    ColumnRef, ExprType, ExpressionPlanDataDecode, ExpressionPlanDataEncode,
    PgBpcharSemantics, PgComparisonOp,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::OutputId;

use super::QueryPlanDataError;
use super::expression_leaf::ExecutionLeafCodec;
use crate::plan::ir::{
    BooleanTestKind, CaseWhen, ComparisonKind, DecimalOperator, ExecutionExpr,
    FixedDecimalType, PostgresEvalExpr, PostgresEvalInput, PostgresExprVolatility,
    QueryPlanError, RelationValueRef, ScalarFunctionKind,
};

const SCALAR: i32 = 1;
const COMPARISON: i32 = 2;
const IS_NULL: i32 = 3;
const IS_NOT_NULL: i32 = 4;
const AND: i32 = 5;
const OR: i32 = 6;
const NOT: i32 = 7;
const POSTGRES: i32 = 8;
const FUNCTION: i32 = 9;
const BOOLEAN_TEST: i32 = 10;
const RELABEL: i32 = 11;
const CASE: i32 = 12;
const IN_LIST: i32 = 13;
const DECIMAL_VALUE: i32 = 14;
const STRICT_FALSE: i32 = 15;
const IS_NAN: i32 = 16;
const IS_NOT_NAN: i32 = 17;
const WIDEN_INTEGER: i32 = 18;
const STRICT_TRUE: i32 = 19;
const BPCHAR_VALUE: i32 = 20;
const DECIMAL_ARITHMETIC: i32 = 21;
const INTEGER_TO_DECIMAL: i32 = 22;
const DECIMAL_BOUND_COMPARISON: i32 = 23;
const DECIMAL_OUTPUT: i32 = 24;
const IMMUTABLE: i32 = 1;
const STABLE: i32 = 2;
const VOLATILE: i32 = 3;

pub(super) struct ExecutionExprCodec;

impl ExecutionExprCodec {
    pub(super) fn encode(expression: &ExecutionExpr, writer: &mut PlanDataWriter) {
        match expression {
            ExecutionExpr::StrictTrue(value) => {
                writer
                    .append_i32(STRICT_TRUE)
                    .append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::StrictFalse(value) => {
                writer
                    .append_i32(STRICT_FALSE)
                    .append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::Column(column) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_column(*column, writer);
            }
            ExecutionExpr::Value(value) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_value(*value, writer);
            }
            ExecutionExpr::DecimalValue {
                value,
                decimal_type,
            } => {
                writer.append_i32(DECIMAL_VALUE).append_count(value.index());
                Self::encode_decimal_type(*decimal_type, writer);
            }
            ExecutionExpr::DecimalArithmetic {
                operator,
                left,
                right,
                result_type,
            } => {
                writer
                    .append_i32(DECIMAL_ARITHMETIC)
                    .append_i32(operator.wire_id());
                Self::encode_decimal_type(*result_type, writer);
                writer
                    .append_nested(|record| Self::encode(left, record))
                    .append_nested(|record| Self::encode(right, record));
            }
            ExecutionExpr::IntegerToDecimal { value, result_type } => {
                writer.append_i32(INTEGER_TO_DECIMAL);
                Self::encode_decimal_type(*result_type, writer);
                writer.append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::DecimalBoundComparison {
                kind,
                operand,
                bound,
                operand_type,
            } => {
                writer
                    .append_i32(DECIMAL_BOUND_COMPARISON)
                    .append_i32(Self::comparison_wire_id(*kind))
                    .append_count(bound.index());
                Self::encode_decimal_type(*operand_type, writer);
                writer.append_nested(|record| Self::encode(operand, record));
            }
            ExecutionExpr::BpcharValue { value, semantics } => {
                writer
                    .append_i32(BPCHAR_VALUE)
                    .append_count(value.index())
                    .append_i32(semantics.typmod());
            }
            ExecutionExpr::Output(output) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_output(*output, writer);
            }
            ExecutionExpr::DecimalOutput {
                output,
                decimal_type,
            } => {
                writer
                    .append_i32(DECIMAL_OUTPUT)
                    .append_count(output.index());
                Self::encode_decimal_type(*decimal_type, writer);
            }
            ExecutionExpr::Comparison {
                operator,
                left,
                right,
            } => {
                writer.append_i32(COMPARISON);
                operator.encode_plan_data(writer);
                writer
                    .append_nested(|record| Self::encode(left, record))
                    .append_nested(|record| Self::encode(right, record));
            }
            ExecutionExpr::IsNull(value) | ExecutionExpr::IsNotNull(value) => {
                writer
                    .append_i32(if matches!(expression, ExecutionExpr::IsNull(_)) {
                        IS_NULL
                    } else {
                        IS_NOT_NULL
                    })
                    .append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::IsNan(value) | ExecutionExpr::IsNotNan(value) => {
                writer
                    .append_i32(if matches!(expression, ExecutionExpr::IsNan(_)) {
                        IS_NAN
                    } else {
                        IS_NOT_NAN
                    })
                    .append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::And(children) | ExecutionExpr::Or(children) => {
                writer
                    .append_i32(if matches!(expression, ExecutionExpr::And(_)) {
                        AND
                    } else {
                        OR
                    })
                    .append_count(children.len());
                for child in children {
                    writer.append_nested(|record| Self::encode(child, record));
                }
            }
            ExecutionExpr::Not(child) => {
                writer
                    .append_i32(NOT)
                    .append_nested(|record| Self::encode(child, record));
            }
            ExecutionExpr::BooleanTest { kind, value } => {
                writer
                    .append_i32(BOOLEAN_TEST)
                    .append_i32(kind.wire_id())
                    .append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::Relabel { value, result_type } => {
                writer.append_i32(RELABEL);
                result_type.encode_plan_data(writer);
                writer.append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::WidenInteger { value, result_type } => {
                writer.append_i32(WIDEN_INTEGER);
                result_type.encode_plan_data(writer);
                writer.append_nested(|record| Self::encode(value, record));
            }
            ExecutionExpr::Case {
                when_then,
                else_expr,
                result_type,
            } => {
                writer.append_i32(CASE);
                result_type.encode_plan_data(writer);
                writer.append_count(when_then.len());
                for branch in when_then {
                    writer.append_nested(|record| {
                        Self::encode(branch.when(), record);
                        Self::encode(branch.then(), record);
                    });
                }
                writer.append_bool(else_expr.is_some());
                if let Some(else_expr) = else_expr {
                    writer.append_nested(|record| Self::encode(else_expr, record));
                }
            }
            ExecutionExpr::InList {
                value,
                list,
                negated,
            } => {
                writer
                    .append_i32(IN_LIST)
                    .append_bool(*negated)
                    .append_nested(|record| Self::encode(value, record))
                    .append_count(list.len());
                for item in list {
                    writer.append_nested(|record| Self::encode(item, record));
                }
            }
            ExecutionExpr::Function {
                kind,
                arguments,
                input_collation,
                result_type,
            } => {
                writer.append_i32(FUNCTION).append_i32(kind.wire_id());
                writer.append_oid(*input_collation);
                result_type.encode_plan_data(writer);
                writer.append_count(arguments.len());
                for argument in arguments {
                    writer.append_nested(|record| Self::encode(argument, record));
                }
            }
            ExecutionExpr::Postgres(expression) => {
                writer
                    .append_i32(POSTGRES)
                    .append_cstr(expression.serialized())
                    .append_i32(match expression.volatility() {
                        PostgresExprVolatility::Immutable => IMMUTABLE,
                        PostgresExprVolatility::Stable => STABLE,
                        PostgresExprVolatility::Volatile => VOLATILE,
                    });
                expression.result_type().encode_plan_data(writer);
                writer.append_count(expression.inputs().len());
                for input in expression.inputs() {
                    writer.append_nested(|record| {
                        input.value_type().encode_plan_data(record);
                        Self::encode(input.expression(), record);
                    });
                }
            }
        }
    }

    pub(super) fn decode(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<ExecutionExpr, QueryPlanDataError> {
        let tag = reader.read_i32()?;
        Ok(match tag {
            STRICT_TRUE => {
                ExecutionExpr::StrictTrue(Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?))
            }
            STRICT_FALSE => {
                ExecutionExpr::StrictFalse(Box::new(reader.read_nested(
                    |record| Self::decode(record, runtime_value_count),
                )?))
            }
            SCALAR => ExecutionLeafCodec::decode(reader, runtime_value_count)?,
            COMPARISON => ExecutionExpr::Comparison {
                operator: PgComparisonOp::decode_plan_data(reader, ())?,
                left: Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?),
                right: Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?),
            },
            IS_NULL | IS_NOT_NULL => {
                let value = Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?);
                if tag == IS_NULL {
                    ExecutionExpr::IsNull(value)
                } else {
                    ExecutionExpr::IsNotNull(value)
                }
            }
            IS_NAN | IS_NOT_NAN => {
                let value = Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?);
                if tag == IS_NAN {
                    ExecutionExpr::IsNan(value)
                } else {
                    ExecutionExpr::IsNotNan(value)
                }
            }
            AND | OR => {
                let count = reader.read_count()?;
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?);
                }
                if tag == AND {
                    ExecutionExpr::And(children.into_boxed_slice())
                } else {
                    ExecutionExpr::Or(children.into_boxed_slice())
                }
            }
            NOT => {
                ExecutionExpr::Not(Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?))
            }
            BOOLEAN_TEST => {
                let raw_kind = reader.read_i32()?;
                let kind = BooleanTestKind::from_wire_id(raw_kind).ok_or(
                    QueryPlanDataError::UnknownExpressionKind { found: raw_kind },
                )?;
                ExecutionExpr::BooleanTest {
                    kind,
                    value: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                }
            }
            RELABEL => {
                let result_type = ExprType::decode_plan_data(reader, ())?;
                ExecutionExpr::Relabel {
                    value: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    result_type,
                }
            }
            WIDEN_INTEGER => {
                let result_type = ExprType::decode_plan_data(reader, ())?;
                ExecutionExpr::WidenInteger {
                    value: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    result_type,
                }
            }
            CASE => {
                let result_type = ExprType::decode_plan_data(reader, ())?;
                let count = reader.read_count()?;
                let mut when_then = Vec::with_capacity(count);
                for _ in 0..count {
                    when_then.push(reader.read_nested(|record| {
                        let when = Self::decode(record, runtime_value_count)?;
                        let then = Self::decode(record, runtime_value_count)?;
                        Ok::<_, QueryPlanDataError>(CaseWhen::new(when, then))
                    })?);
                }
                let else_expr = reader
                    .read_bool()?
                    .then(|| {
                        reader.read_nested(|record| {
                            Self::decode(record, runtime_value_count)
                        })
                    })
                    .transpose()?
                    .map(Box::new);
                ExecutionExpr::Case {
                    when_then: when_then.into_boxed_slice(),
                    else_expr,
                    result_type,
                }
            }
            IN_LIST => {
                let negated = reader.read_bool()?;
                let value = Box::new(reader.read_nested(|record| {
                    Self::decode(record, runtime_value_count)
                })?);
                let count = reader.read_count()?;
                let mut list = Vec::with_capacity(count);
                for _ in 0..count {
                    list.push(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?);
                }
                ExecutionExpr::InList {
                    value,
                    list: list.into_boxed_slice(),
                    negated,
                }
            }
            DECIMAL_VALUE => {
                let value = ExecutionLeafCodec::decode_runtime_value(
                    reader,
                    runtime_value_count,
                )?;
                ExecutionExpr::DecimalValue {
                    value,
                    decimal_type: Self::decode_decimal_type(reader)?,
                }
            }
            DECIMAL_OUTPUT => ExecutionExpr::DecimalOutput {
                output: OutputId::from_index(reader.read_count()?),
                decimal_type: Self::decode_decimal_type(reader)?,
            },
            DECIMAL_ARITHMETIC => {
                let raw_operator = reader.read_i32()?;
                let operator = DecimalOperator::from_wire_id(raw_operator).ok_or(
                    QueryPlanDataError::UnknownExpressionKind {
                        found: raw_operator,
                    },
                )?;
                let result_type = Self::decode_decimal_type(reader)?;
                ExecutionExpr::DecimalArithmetic {
                    operator,
                    left: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    right: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    result_type,
                }
            }
            INTEGER_TO_DECIMAL => {
                let result_type = Self::decode_decimal_type(reader)?;
                ExecutionExpr::IntegerToDecimal {
                    value: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    result_type,
                }
            }
            DECIMAL_BOUND_COMPARISON => {
                let raw_kind = reader.read_i32()?;
                let kind = Self::comparison_from_wire_id(raw_kind).ok_or(
                    QueryPlanDataError::UnknownExpressionKind { found: raw_kind },
                )?;
                let bound = ExecutionLeafCodec::decode_runtime_value(
                    reader,
                    runtime_value_count,
                )?;
                let operand_type = Self::decode_decimal_type(reader)?;
                ExecutionExpr::DecimalBoundComparison {
                    kind,
                    operand: Box::new(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?),
                    bound,
                    operand_type,
                }
            }
            BPCHAR_VALUE => {
                let value = ExecutionLeafCodec::decode_runtime_value(
                    reader,
                    runtime_value_count,
                )?;
                let semantics = PgBpcharSemantics::for_typmod(reader.read_i32()?)
                    .ok_or(QueryPlanError::UnsupportedRuntimeValueType)?;
                ExecutionExpr::BpcharValue { value, semantics }
            }
            FUNCTION => {
                let raw_kind = reader.read_i32()?;
                let kind = ScalarFunctionKind::from_wire_id(raw_kind).ok_or(
                    QueryPlanDataError::UnknownExpressionKind { found: raw_kind },
                )?;
                let input_collation = reader.read_oid()?;
                let result_type = ExprType::decode_plan_data(reader, ())?;
                let count = reader.read_count()?;
                let mut arguments = Vec::with_capacity(count);
                for _ in 0..count {
                    arguments.push(reader.read_nested(|record| {
                        Self::decode(record, runtime_value_count)
                    })?);
                }
                ExecutionExpr::Function {
                    kind,
                    arguments: arguments.into_boxed_slice(),
                    input_collation,
                    result_type,
                }
            }
            POSTGRES => {
                let serialized = Arc::from(reader.read_cstr()?);
                let volatility = match reader.read_i32()? {
                    IMMUTABLE => PostgresExprVolatility::Immutable,
                    STABLE => PostgresExprVolatility::Stable,
                    VOLATILE => PostgresExprVolatility::Volatile,
                    found => {
                        return Err(
                            QueryPlanDataError::UnknownExpressionVolatility { found },
                        );
                    }
                };
                let result_type = ExprType::decode_plan_data(reader, ())?;
                let count = reader.read_count()?;
                let mut inputs = Vec::with_capacity(count);
                for _ in 0..count {
                    inputs.push(reader.read_nested(|record| {
                        let value_type = ExprType::decode_plan_data(record, ())?;
                        let expression = Self::decode(record, runtime_value_count)?;
                        Ok::<_, QueryPlanDataError>(PostgresEvalInput::new(
                            expression, value_type,
                        ))
                    })?);
                }
                ExecutionExpr::Postgres(PostgresEvalExpr::new(
                    serialized,
                    inputs.into_boxed_slice(),
                    result_type,
                    volatility,
                ))
            }
            found => return Err(QueryPlanDataError::UnknownExpressionKind { found }),
        })
    }

    fn encode_decimal_type(
        decimal_type: FixedDecimalType,
        writer: &mut PlanDataWriter,
    ) {
        writer
            .append_i32(i32::from(decimal_type.precision()))
            .append_i32(i32::from(decimal_type.scale()));
    }

    fn decode_decimal_type(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<FixedDecimalType, QueryPlanDataError> {
        let precision = u8::try_from(reader.read_i32()?)
            .map_err(|_| QueryPlanError::UnsupportedRuntimeValueType)?;
        let scale = u8::try_from(reader.read_i32()?)
            .map_err(|_| QueryPlanError::UnsupportedRuntimeValueType)?;
        FixedDecimalType::new(precision, scale)
            .ok_or_else(|| QueryPlanError::UnsupportedRuntimeValueType.into())
    }

    const fn comparison_wire_id(kind: ComparisonKind) -> i32 {
        match kind {
            ComparisonKind::Equal => 1,
            ComparisonKind::NotEqual => 2,
            ComparisonKind::Less => 3,
            ComparisonKind::LessEqual => 4,
            ComparisonKind::Greater => 5,
            ComparisonKind::GreaterEqual => 6,
        }
    }

    const fn comparison_from_wire_id(id: i32) -> Option<ComparisonKind> {
        Some(match id {
            1 => ComparisonKind::Equal,
            2 => ComparisonKind::NotEqual,
            3 => ComparisonKind::Less,
            4 => ComparisonKind::LessEqual,
            5 => ComparisonKind::Greater,
            6 => ComparisonKind::GreaterEqual,
            _ => return None,
        })
    }

    pub(super) fn encode_column(column: ColumnRef, writer: &mut PlanDataWriter) {
        ExecutionLeafCodec::encode_column(column, writer);
    }

    pub(super) fn decode_column(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<ColumnRef, QueryPlanDataError> {
        ExecutionLeafCodec::decode_column(reader)
    }

    pub(super) fn encode_relation_value(
        value: RelationValueRef,
        writer: &mut PlanDataWriter,
    ) {
        ExecutionLeafCodec::encode_relation_value(value, writer);
    }

    pub(super) fn decode_relation_value(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<RelationValueRef, QueryPlanDataError> {
        ExecutionLeafCodec::decode_relation_value(reader)
    }
}
