//! PG semantics not established by DataFusion's type coercion alone.
//! These explicit restrictions supplement the IR capabilities; they do not
//! change the provider's supported types or define every operator's semantics.
//!
//! Arrow strings are UTF-8, while PostgreSQL text follows the server
//! encoding. Outside a UTF-8 server encoding, Arrow cannot carry PostgreSQL
//! text, and string functions such as `ascii` differ by encoding. Query
//! offload therefore admits no string representation there, including an
//! encoding-insensitive use such as a NULL test on a text column. This
//! product restriction is the query-admission gate for Arrow string
//! representations; the IR keeps only collation, operator and typmod
//! semantics. Other PostgreSQL/Arrow representations, including structural
//! JSONB output, retain their codec-specific capability checks at their
//! binding boundaries.

use arrow_schema::{DataType, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};
use lagodb_core::datum::Utf8ServerEncoding;

use super::AdmissionDeclineReason;

pub(super) struct RepresentationSemantics {
    /// Whether Arrow strings represent PostgreSQL text in this database.
    strings: bool,
}

impl RepresentationSemantics {
    pub(super) fn for_server_encoding() -> Self {
        Self {
            strings: Utf8ServerEncoding::resolve().is_ok(),
        }
    }

    pub(super) fn check_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(), AdmissionDeclineReason> {
        let inputs = plan.inputs();
        for input in &inputs {
            self.check_plan(input)?;
        }
        // Every scan, intermediate and output schema of the fragment.
        if !self.strings
            && plan
                .schema()
                .fields()
                .iter()
                .any(|field| Self::carries_string(field.data_type()))
        {
            return Err(AdmissionDeclineReason::NonUtf8String);
        }
        let schema = merge_schema(&inputs);
        // Query DISTINCT lowers to Aggregate grouping keys. Check these at the
        // relational boundary; a bare column expression performs no comparison.
        if let LogicalPlan::Aggregate(aggregate) = plan {
            for key in &aggregate.group_expr {
                Self::check_equality_key(key, &schema)?;
            }
        }
        let mut result = Ok(());
        plan.apply_expressions(|expression| {
            result = self.check(expression, &schema);
            Ok(if result.is_ok() {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            })
        })
        .map_err(AdmissionDeclineReason::ExpressionType)?;
        result
    }

    fn check_equality_key(
        expression: &Expr,
        schema: &DFSchema,
    ) -> Result<(), AdmissionDeclineReason> {
        let data_type = expression
            .get_type(schema)
            .map_err(AdmissionDeclineReason::ExpressionType)?;
        if Self::nanoseconds(&data_type) {
            Err(AdmissionDeclineReason::NanosecondComparison)
        } else {
            Ok(())
        }
    }

    fn check(
        &self,
        expression: &Expr,
        schema: &DFSchema,
    ) -> Result<(), AdmissionDeclineReason> {
        let mut decline = None;
        let visited = expression.apply(|expression| {
            // A string produced and consumed inside one expression, such as
            // a PostgreSQL-evaluated cast compared with a constant, appears
            // in no plan schema.
            if !self.strings && Self::carries_string(&expression.get_type(schema)?) {
                decline = Some(AdmissionDeclineReason::NonUtf8String);
                return Ok(TreeNodeRecursion::Stop);
            }
            decline = match expression {
                Expr::BinaryExpr(binary) if matches!(binary.op,
                    Operator::Eq | Operator::NotEq | Operator::Lt
                    | Operator::LtEq | Operator::Gt | Operator::GtEq
                ) => {
                    // PG decoding floors nanoseconds to microseconds, so
                    // comparing the source values does not implement PG
                    // equality or ordering, even when DataFusion accepts it.
                    (Self::nanoseconds(&binary.left.get_type(schema)?)
                        || Self::nanoseconds(&binary.right.get_type(schema)?))
                        .then_some(AdmissionDeclineReason::NanosecondComparison)
                }
                Expr::InList(list) => Self::nanoseconds(&list.expr.get_type(schema)?)
                    .then_some(AdmissionDeclineReason::NanosecondComparison),
                Expr::AggregateFunction(aggregate) => {
                    let ordered = aggregate
                        .params
                        .order_by
                        .iter()
                        .map(|order| &order.expr);
                    let distinct = aggregate
                        .params
                        .args
                        .iter()
                        .filter(|_| aggregate.params.distinct);
                    for expression in ordered.chain(distinct) {
                        if Self::nanoseconds(&expression.get_type(schema)?) {
                            decline = Some(AdmissionDeclineReason::NanosecondComparison);
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    None
                }
                Expr::Cast(cast) => {
                    // CASE/COALESCE can silently promote a PG microsecond
                    // literal to the source's nanoseconds. A finite PG value
                    // need not fit that narrower range. Keep such combinations
                    // in PG instead of adding value checks or per-row casts.
                    (Self::nanoseconds(cast.field.data_type())
                        && matches!(cast.expr.get_type(schema)?,
                            DataType::Timestamp(unit, _) if unit != TimeUnit::Nanosecond))
                        .then_some(AdmissionDeclineReason::NanosecondPromotion)
                }
                _ => None,
            };
            Ok(if decline.is_none() { TreeNodeRecursion::Continue } else { TreeNodeRecursion::Stop })
        });
        visited.map_err(AdmissionDeclineReason::ExpressionType)?;
        decline.map_or(Ok(()), Err)
    }

    fn nanoseconds(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _))
    }

    /// Whether this Arrow type tree contains a string leaf, including inside
    /// lists, structs, maps and encoded arrays. It does not decide the
    /// encoding requirements of PostgreSQL Datum codecs.
    fn carries_string(data_type: &DataType) -> bool {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
            DataType::Dictionary(_, value) => Self::carries_string(value),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => {
                Self::carries_string(field.data_type())
            }
            DataType::Struct(fields) => fields
                .iter()
                .any(|field| Self::carries_string(field.data_type())),
            DataType::Union(fields, _) => fields
                .iter()
                .any(|(_, field)| Self::carries_string(field.data_type())),
            _ => false,
        }
    }
}
