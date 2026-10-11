//! Exact query-expression lowering into DataFusion expressions.

use std::ops::Not;
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion::functions::math::expr_fn::isnan;
use datafusion::logical_expr::expr::{Case, InList, Placeholder};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, col};
use lagodb_core::expr::{ColumnRef, RuntimeValue, RuntimeValueLayout};
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

use crate::plan::{
    BooleanTestKind, ComparisonKind, ExecutionExpr, ExecutionScalarRepr,
    ScalarFunctionKind,
};

use super::DataFusionPlanError;
use crate::datafusion::integer_abs::PgIntegerAbsUdf;
use crate::datafusion::postgres_eval::{PgExprRuntime, PgExprSignature, PgExprUdf};
use crate::datafusion::scan_binding::ScanBindings;

/// The expression tree is lowered identically for admission and execution.
/// Planning binds leaves to their physical types without evaluating PG values
/// or constructing PostgreSQL UDF runtime state.
enum ExpressionBindings<'a> {
    Execution {
        values: &'a [RuntimeValue],
        postgres: PgExprRuntime,
    },
    Planning {
        values: &'a RuntimeValueLayout,
    },
}

pub(in crate::datafusion) struct DataFusionExpressionCompiler<'a> {
    scans: &'a ScanBindings,
    bindings: ExpressionBindings<'a>,
}

impl<'a> DataFusionExpressionCompiler<'a> {
    pub(super) const fn new(
        scans: &'a ScanBindings,
        values: &'a [RuntimeValue],
        postgres: PgExprRuntime,
    ) -> Self {
        Self {
            scans,
            bindings: ExpressionBindings::Execution { values, postgres },
        }
    }

    pub(super) const fn for_planning(
        scans: &'a ScanBindings,
        values: &'a RuntimeValueLayout,
    ) -> Self {
        Self {
            scans,
            bindings: ExpressionBindings::Planning { values },
        }
    }

    pub(in crate::datafusion) fn compile(
        &self,
        expression: &ExecutionExpr,
    ) -> Result<Expr, DataFusionPlanError> {
        match expression {
            ExecutionExpr::StrictTrue(value) => {
                let value = self.compile(value)?;
                // Arrow comparisons propagate NULL and compare every value,
                // including a NaN bit pattern, equal to itself.
                Ok(value.clone().eq(value))
            }
            ExecutionExpr::StrictFalse(value) => {
                let value = self.compile(value)?;
                // The same reflexive ordering makes self-greater-than false
                // for every non-NULL value while retaining UNKNOWN for NULL.
                Ok(value.clone().gt(value))
            }
            ExecutionExpr::Column(column) => self.column(column),
            ExecutionExpr::Value(value) => match &self.bindings {
                ExpressionBindings::Execution { values, .. } => values
                    .get(value.index())
                    .copied()
                    .ok_or(DataFusionPlanError::MissingRuntimeValue {
                        index: value.index(),
                    })
                    .and_then(Self::runtime_value)
                    .map(|value| Expr::Literal(value, None)),
                ExpressionBindings::Planning { values, .. } => {
                    let value_type = values.values()[value.index()].value_type;
                    let representation = ExecutionScalarRepr::for_runtime_value(
                        value_type,
                    )
                    .expect("validated runtime value has a physical representation");
                    Ok(Self::typed_value(representation.data_type()))
                }
            },
            ExecutionExpr::DecimalValue { value, semantics } => {
                match &self.bindings {
                    ExpressionBindings::Execution { values, .. } => values
                        .get(value.index())
                        .copied()
                        .ok_or(DataFusionPlanError::MissingRuntimeValue {
                            index: value.index(),
                        })
                        .and_then(|value| {
                            Self::decimal_runtime_value(value, *semantics)
                        })
                        .map(|value| Expr::Literal(value, None)),
                    ExpressionBindings::Planning { .. } => {
                        Ok(Self::typed_value(DataType::Decimal128(
                            semantics.precision(),
                            semantics.scale(),
                        )))
                    }
                }
            }
            ExecutionExpr::Output(output) => Ok(col(Self::output_name(*output))),
            ExecutionExpr::Comparison {
                operator,
                left,
                right,
            } => {
                let left = self.compile(left)?;
                let right = self.compile(right)?;
                Ok(
                    match operator
                        .builtin_signature()
                        .map(|signature| signature.kind())
                    {
                        Some(ComparisonKind::Equal) => left.eq(right),
                        Some(ComparisonKind::NotEqual) => left.not_eq(right),
                        Some(ComparisonKind::Less) => left.lt(right),
                        Some(ComparisonKind::LessEqual) => left.lt_eq(right),
                        Some(ComparisonKind::Greater) => left.gt(right),
                        Some(ComparisonKind::GreaterEqual) => left.gt_eq(right),
                        None => {
                            return Err(DataFusionPlanError::UnsupportedOperator {
                                oid: operator.opno,
                            });
                        }
                    },
                )
            }
            ExecutionExpr::IsNull(value) => Ok(self.compile(value)?.is_null()),
            ExecutionExpr::IsNotNull(value) => Ok(self.compile(value)?.is_not_null()),
            ExecutionExpr::IsNan(value) => Ok(isnan(self.compile(value)?)),
            ExecutionExpr::IsNotNan(value) => Ok(isnan(self.compile(value)?).not()),
            ExecutionExpr::BooleanTest { kind, value } => {
                let value = self.compile(value)?;
                Ok(match kind {
                    BooleanTestKind::IsTrue => value.is_true(),
                    BooleanTestKind::IsNotTrue => value.is_not_true(),
                    BooleanTestKind::IsFalse => value.is_false(),
                    BooleanTestKind::IsNotFalse => value.is_not_false(),
                    BooleanTestKind::IsUnknown => value.is_unknown(),
                    BooleanTestKind::IsNotUnknown => value.is_not_unknown(),
                })
            }
            ExecutionExpr::And(children) | ExecutionExpr::Or(children) => {
                let mut children = children.iter();
                let first = children.next().ok_or_else(|| {
                    DataFusionError::Plan("empty predicate".to_owned())
                })?;
                let mut result = self.compile(first)?;
                for child in children {
                    let child = self.compile(child)?;
                    result = if matches!(expression, ExecutionExpr::And(_)) {
                        result.and(child)
                    } else {
                        result.or(child)
                    };
                }
                Ok(result)
            }
            ExecutionExpr::Not(child) => Ok(self.compile(child)?.not()),
            ExecutionExpr::Relabel { value, .. } => self.compile(value),
            ExecutionExpr::WidenInteger { value, result_type } => {
                let representation =
                    ExecutionScalarRepr::for_runtime_value(*result_type).ok_or(
                        DataFusionPlanError::UnsupportedRuntimeType {
                            oid: result_type.type_oid,
                        },
                    )?;
                Ok(cast(self.compile(value)?, representation.data_type()))
            }
            ExecutionExpr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let when_then_expr = when_then
                    .iter()
                    .map(|branch| {
                        Ok((
                            Box::new(self.compile(branch.when())?),
                            Box::new(self.compile(branch.then())?),
                        ))
                    })
                    .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                let else_expr = else_expr
                    .as_deref()
                    .map(|expression| self.compile(expression).map(Box::new))
                    .transpose()?;
                Ok(Expr::Case(Case::new(None, when_then_expr, else_expr)))
            }
            ExecutionExpr::InList {
                value,
                list,
                negated,
            } => {
                let value = Box::new(self.compile(value)?);
                let list = list
                    .iter()
                    .map(|item| self.compile(item))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expr::InList(InList::new(value, list, *negated)))
            }
            ExecutionExpr::Function {
                kind,
                arguments,
                result_type,
                ..
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| self.compile(argument))
                    .collect::<Result<Vec<_>, _>>()?;
                Self::native_function(*kind, arguments, result_type.type_oid)
            }
            ExecutionExpr::Postgres(postgres) => {
                let arguments = postgres
                    .inputs()
                    .iter()
                    .map(|input| self.compile(input.expression()))
                    .collect::<Result<Vec<_>, _>>()?;
                match &self.bindings {
                    ExpressionBindings::Execution {
                        postgres: runtime, ..
                    } => PgExprUdf::try_new(postgres.clone(), *runtime)
                        .ok_or(DataFusionPlanError::UnsupportedPostgresExpression)
                        .map(|udf| udf.call(arguments)),
                    ExpressionBindings::Planning { .. } => {
                        PgExprSignature::try_new(postgres)
                            .ok_or(DataFusionPlanError::UnsupportedPostgresExpression)
                            .map(|signature| signature.into_udf().call(arguments))
                    }
                }
            }
        }
    }

    fn native_function(
        kind: ScalarFunctionKind,
        arguments: Vec<Expr>,
        result_type: pg_sys::Oid,
    ) -> Result<Expr, DataFusionPlanError> {
        use datafusion::functions::core::expr_fn::{
            coalesce, greatest, least, nullif,
        };
        use datafusion::functions::math::expr_fn::{abs, ceil, floor};
        use datafusion::functions::string::expr_fn::{
            ascii, repeat, replace, starts_with,
        };
        use datafusion::functions::unicode::expr_fn::{
            character_length, reverse, substr, substring,
        };

        let mut arguments = arguments.into_iter();
        let missing = || {
            DataFusionError::Plan(
                "native scalar function has an invalid argument layout".to_owned(),
            )
        };

        Ok(match kind {
            ScalarFunctionKind::Ascii => ascii(arguments.next().ok_or_else(missing)?),
            ScalarFunctionKind::Repeat => repeat(
                arguments.next().ok_or_else(missing)?,
                arguments.next().ok_or_else(missing)?,
            ),
            ScalarFunctionKind::StartsWith => starts_with(
                arguments.next().ok_or_else(missing)?,
                arguments.next().ok_or_else(missing)?,
            ),
            ScalarFunctionKind::Replace => replace(
                arguments.next().ok_or_else(missing)?,
                arguments.next().ok_or_else(missing)?,
                arguments.next().ok_or_else(missing)?,
            ),
            ScalarFunctionKind::CharacterLength => {
                character_length(arguments.next().ok_or_else(missing)?)
            }
            ScalarFunctionKind::Substring => {
                let value = arguments.next().ok_or_else(missing)?;
                let start = arguments.next().ok_or_else(missing)?;
                match arguments.next() {
                    Some(length) => substring(value, start, length),
                    None => substr(value, start),
                }
            }
            ScalarFunctionKind::Reverse => {
                reverse(arguments.next().ok_or_else(missing)?)
            }
            ScalarFunctionKind::Abs => {
                let argument = arguments.next().ok_or_else(missing)?;
                match result_type {
                    pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
                        PgIntegerAbsUdf::for_type(result_type)
                            .ok_or_else(|| {
                                DataFusionError::Plan(
                                    "native integer ABS has an invalid PostgreSQL type"
                                        .to_owned(),
                                )
                            })?
                            .call(vec![argument])
                    }
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID => abs(argument),
                    _ => {
                        return Err(DataFusionError::Plan(
                            "native ABS has an unsupported PostgreSQL result type"
                                .to_owned(),
                        )
                        .into());
                    }
                }
            }
            ScalarFunctionKind::Ceil => ceil(arguments.next().ok_or_else(missing)?),
            ScalarFunctionKind::Floor => floor(arguments.next().ok_or_else(missing)?),
            ScalarFunctionKind::Greatest => greatest(arguments.collect()),
            ScalarFunctionKind::Least => least(arguments.collect()),
            ScalarFunctionKind::Coalesce => coalesce(arguments.collect()),
            ScalarFunctionKind::NullIf => nullif(
                arguments.next().ok_or_else(missing)?,
                arguments.next().ok_or_else(missing)?,
            ),
        })
    }

    fn column(&self, column: &ColumnRef) -> Result<Expr, DataFusionPlanError> {
        let binding =
            self.scans
                .get(column.scan)
                .ok_or(DataFusionPlanError::MissingScan {
                    index: column.scan.index(),
                })?;
        binding.column(column.attno).map(Expr::Column).ok_or(
            DataFusionPlanError::MissingColumn {
                scan: column.scan.index(),
                attno: column.attno,
            },
        )
    }

    fn typed_value(data_type: DataType) -> Expr {
        Expr::Placeholder(Placeholder::new_with_field(
            "$type".to_owned(),
            Some(Arc::new(Field::new("", data_type, true))),
        ))
    }

    pub(super) fn output_name(output: OutputId) -> String {
        format!("__lagodb_output_{}", output.index())
    }
}
