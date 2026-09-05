//! Plan-data codec for exact execution expressions.

use std::sync::Arc;

use lagodb_core::expr::{
    ColumnRef, ExprType, ExpressionPlanDataDecode, ExpressionPlanDataEncode,
    PgComparisonOp,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

use super::leaf_codec::ExecutionLeafCodec;
use super::{
    BooleanTestKind, CaseWhen, ExecutionExpr, PostgresEvalExpr, PostgresEvalInput,
    PostgresExprVolatility, ScalarFunctionKind,
};
use crate::plan::QueryPlanDataError;

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
const IMMUTABLE: i32 = 1;
const STABLE: i32 = 2;
const VOLATILE: i32 = 3;

pub(in crate::plan) struct ExecutionExprCodec;

impl ExecutionExprCodec {
    pub(in crate::plan) fn encode(
        expression: &ExecutionExpr,
        writer: &mut PlanDataWriter,
    ) {
        match expression {
            ExecutionExpr::Column(column) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_column(*column, writer);
            }
            ExecutionExpr::Value(value) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_value(*value, writer);
            }
            ExecutionExpr::Output(output) => {
                writer.append_i32(SCALAR);
                ExecutionLeafCodec::encode_output(*output, writer);
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

    pub(in crate::plan) fn decode(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<ExecutionExpr, QueryPlanDataError> {
        let tag = reader.read_i32()?;
        Ok(match tag {
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

    pub(in crate::plan) fn encode_column(
        column: ColumnRef,
        writer: &mut PlanDataWriter,
    ) {
        ExecutionLeafCodec::encode_column(column, writer);
    }

    pub(in crate::plan) fn decode_column(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<ColumnRef, QueryPlanDataError> {
        ExecutionLeafCodec::decode_column(reader)
    }
}
