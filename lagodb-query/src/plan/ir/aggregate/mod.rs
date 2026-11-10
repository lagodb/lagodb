//! Aggregate semantic IR nodes.

use std::ffi::CString;

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

mod row_count;
mod semantics;

use super::ordering::{OrderingSemantics, SortDirection};
use super::semantics::ScalarSemantics;
use super::{
    Decimal128Semantics, ExecutionExpr, QueryNode, QueryPlanError, RowEstimate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    Count,
    Min,
    Max,
    Sum,
    Average,
    NumericSum,
    NumericAverage,
    VarianceSample,
    VariancePopulation,
    StddevSample,
    StddevPopulation,
    BoolAnd,
    BoolOr,
    ArrayAgg,
    StringAgg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArguments {
    None,
    Unary(ExecutionExpr),
    StringAgg {
        value: ExecutionExpr,
        delimiter: CString,
    },
}

impl AggregateArguments {
    #[inline]
    pub const fn primary(&self) -> Option<&ExecutionExpr> {
        match self {
            Self::None => None,
            Self::Unary(argument)
            | Self::StringAgg {
                value: argument, ..
            } => Some(argument),
        }
    }

    #[inline]
    pub const fn delimiter(&self) -> Option<&CString> {
        match self {
            Self::StringAgg { delimiter, .. } => Some(delimiter),
            Self::None | Self::Unary(_) => None,
        }
    }

    #[inline]
    pub const fn semantic_len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Unary(_) => 1,
            Self::StringAgg { .. } => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateOrderExpr {
    expression: ExecutionExpr,
    direction: SortDirection,
    nulls_first: bool,
}

impl AggregateOrderExpr {
    pub fn try_new(
        expression: ExecutionExpr,
        direction: SortDirection,
        nulls_first: bool,
    ) -> Result<Self, QueryPlanError> {
        let ExecutionExpr::Column(column) = &expression else {
            return Err(QueryPlanError::UnsupportedAggregate);
        };
        if !OrderingSemantics::supports_aggregate_order(column.value_type) {
            return Err(QueryPlanError::UnsupportedAggregate);
        }
        Ok(Self {
            expression,
            direction,
            nulls_first,
        })
    }

    #[inline]
    pub const fn expression(&self) -> &ExecutionExpr {
        &self.expression
    }

    #[inline]
    pub const fn direction(&self) -> SortDirection {
        self.direction
    }

    #[inline]
    pub const fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

/// Complete semantic gate for one PostgreSQL aggregate call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggCall {
    function_oid: pg_sys::Oid,
    kind: AggregateKind,
    arguments: AggregateArguments,
    distinct: bool,
    order_by: Box<[AggregateOrderExpr]>,
    filter: Option<ExecutionExpr>,
    result_type: ExprType,
    output: OutputId,
}

impl AggCall {
    pub fn try_new(
        function_oid: pg_sys::Oid,
        arguments: AggregateArguments,
        distinct: bool,
        filter: Option<ExecutionExpr>,
        result_type: ExprType,
        output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        let aggregate = Self {
            function_oid,
            kind: AggregateKind::classify(function_oid, &arguments)?,
            arguments,
            distinct,
            order_by: Box::new([]),
            filter,
            result_type,
            output,
        };
        // PostgreSQL evaluates an aggregate FILTER before its arguments. A
        // grouped DataFusion aggregate may evaluate argument expressions
        // first, so accepting an arbitrary expression here could expose an
        // error or volatile side effect on a row PostgreSQL would filter out.
        // Direct columns have no such evaluation behavior to reorder.
        if aggregate.filter.is_some()
            && aggregate
                .arguments
                .primary()
                .is_some_and(|argument| !matches!(argument, ExecutionExpr::Column(_)))
        {
            return Err(QueryPlanError::UnsupportedAggregate);
        }
        aggregate.validate_argument_type(
            aggregate.arguments.primary().and_then(Self::scalar_type),
        )?;
        Ok(aggregate)
    }

    /// Attach validated aggregate ordering to an implementation that honors it.
    pub fn try_with_order_by(
        mut self,
        order_by: Box<[AggregateOrderExpr]>,
    ) -> Result<Self, QueryPlanError> {
        if !order_by.is_empty() && !self.kind.honors_order_by() {
            return Err(QueryPlanError::UnsupportedAggregate);
        }
        self.order_by = order_by;
        Ok(self)
    }

    fn scalar_type(value: &ExecutionExpr) -> Option<ExprType> {
        value.result_type_hint()
    }

    #[inline]
    pub const fn function_oid(&self) -> pg_sys::Oid {
        self.function_oid
    }

    #[inline]
    pub const fn kind(&self) -> AggregateKind {
        self.kind
    }

    #[inline]
    pub const fn arguments(&self) -> &AggregateArguments {
        &self.arguments
    }

    #[inline]
    pub const fn argument(&self) -> Option<&ExecutionExpr> {
        self.arguments.primary()
    }

    #[inline]
    pub const fn is_distinct(&self) -> bool {
        self.distinct
    }

    #[inline]
    pub fn order_by(&self) -> &[AggregateOrderExpr] {
        &self.order_by
    }

    /// Whether physical execution needs a growing DISTINCT state.
    ///
    /// Duplicate elimination cannot change MIN/MAX or boolean idempotent
    /// aggregates, including their NULL and empty-set behavior. Those calls
    /// retain DISTINCT in the semantic IR while lowering to their ordinary
    /// constant-space aggregate.
    #[inline]
    pub const fn uses_distinct_state(&self) -> bool {
        self.distinct
            && !matches!(
                self.kind,
                AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::BoolAnd
                    | AggregateKind::BoolOr
            )
    }

    #[inline]
    pub const fn filter(&self) -> Option<&ExecutionExpr> {
        self.filter.as_ref()
    }

    #[inline]
    pub const fn result_type(&self) -> ExprType {
        self.result_type
    }

    /// Physical type emitted by DataFusion before the PostgreSQL output codec.
    /// Decimal MIN/MAX preserve their input Decimal128 metadata, and SUM(int8)
    /// emits its accumulation decimal, even though PostgreSQL's Aggref result
    /// typmod is -1.
    pub fn execution_result_type(&self) -> ExprType {
        if let Some(decimal) = self.int8_sum_decimal() {
            return decimal.value_type();
        }
        if matches!(self.kind, AggregateKind::Min | AggregateKind::Max)
            && self.result_type.type_oid == pg_sys::NUMERICOID
        {
            return self
                .argument()
                .and_then(ExecutionExpr::result_type_hint)
                .unwrap_or(self.result_type);
        }
        self.result_type
    }

    /// Exact accumulation decimal for SUM(int8), whose PostgreSQL result is
    /// NUMERIC. DataFusion's native Int64 SUM wraps on overflow, so the input
    /// is widened to NUMERIC(38,0) and summed by the native Decimal128 SUM. Its
    /// magnitude stays below 2^63 per input row and therefore below 10^38 for
    /// fewer than 10^19 rows.
    pub fn int8_sum_decimal(&self) -> Option<Decimal128Semantics> {
        (u32::from(self.function_oid) == pg_sys::F_SUM_INT8).then(|| {
            Decimal128Semantics::new(38, 0)
                .expect("NUMERIC(38,0) is a valid Decimal128 shape")
        })
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }

    #[inline]
    pub const fn nullable(&self) -> bool {
        !matches!(self.kind, AggregateKind::Count)
    }

    /// Whether the aggregate's physical result can participate in HAVING.
    ///
    /// Integer AVG deliberately uses DataFusion's native Float64 result even
    /// though PostgreSQL declares NUMERIC, inheriting Float64 rounding instead
    /// of PostgreSQL's arbitrary-precision transition semantics. SUM(int8) and
    /// Decimal MIN/MAX emit bounded Decimal128 results and use the native exact
    /// comparison path. NUMERIC-input SUM/AVG keep LagoDB's exact Binary result
    /// and remain unavailable to HAVING.
    #[inline]
    pub const fn supports_having_result(&self) -> bool {
        !matches!(
            self.kind,
            AggregateKind::NumericSum | AggregateKind::NumericAverage
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupExpr {
    expression: ExecutionExpr,
    result_type: ExprType,
    output: OutputId,
}

impl GroupExpr {
    pub fn try_new(
        expression: ExecutionExpr,
        result_type: ExprType,
        output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        let expression_type = expression
            .result_type_hint()
            .ok_or(QueryPlanError::UnsupportedGroupKey)?;
        if expression_type != result_type
            || !ScalarSemantics::Exact.supports_hash_group_key(result_type)
        {
            return Err(QueryPlanError::UnsupportedGroupKey);
        }
        Ok(Self {
            expression,
            result_type,
            output,
        })
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateNode {
    input: Box<QueryNode>,
    groups: Box<[GroupExpr]>,
    aggregates: Box<[AggCall]>,
    estimated_rows: RowEstimate,
}

impl AggregateNode {
    pub fn new(
        input: QueryNode,
        groups: Box<[GroupExpr]>,
        aggregates: Box<[AggCall]>,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        if groups.is_empty() && aggregates.is_empty() {
            return Err(QueryPlanError::EmptyAggregate);
        }
        Ok(Self {
            input: Box::new(input),
            groups,
            aggregates,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn groups(&self) -> &[GroupExpr] {
        &self.groups
    }

    #[inline]
    pub fn aggregates(&self) -> &[AggCall] {
        &self.aggregates
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows.get()
    }
}
