//! `copyObject`-safe query plan data and validation boundary.

use lagodb_core::expr::{
    ExprType, ExpressionCodecError, ExpressionPlanDataDecode,
    ExpressionPlanDataEncode, RuntimeValueLayout, RuntimeValueSpec,
};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{OutputId, ScanId};
use pgrx::pg_sys;

use super::validation::PlanExpressionValidator;
use super::{
    AggCall, AggregateArguments, AggregateNode, DistinctExpr, GroupExpr, ProjectExpr,
    ProjectNode, QueryFragment, QueryNode, QueryPlanError, QueryTupleLayout,
    ScanNode,
};

#[derive(Debug, thiserror::Error)]
pub enum QueryPlanDataError {
    #[error("query plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("query expression codec failed: {0}")]
    Expression(#[from] ExpressionCodecError),
    #[error("invalid query plan: {0}")]
    InvalidPlan(#[from] QueryPlanError),
    #[error("query plan contains unknown node kind {found}")]
    UnknownNodeKind { found: i32 },
    #[error("query plan contains unknown join type {found}")]
    UnknownJoinType { found: i32 },
    #[error("query plan contains unknown expression kind {found}")]
    UnknownExpressionKind { found: i32 },
    #[error("query plan contains unknown PostgreSQL expression volatility {found}")]
    UnknownExpressionVolatility { found: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlanData {
    fragment: QueryFragment,
    tuple_layout: QueryTupleLayout,
    runtime_values: RuntimeValueLayout,
}

impl QueryPlanData {
    pub fn new(
        fragment: QueryFragment,
        tuple_layout: QueryTupleLayout,
        runtime_values: RuntimeValueLayout,
        scan_count: usize,
    ) -> Result<Self, QueryPlanError> {
        let plan = Self {
            fragment,
            tuple_layout,
            runtime_values,
        };
        plan.validate(scan_count)?;
        Ok(plan)
    }

    pub fn scalar_count(
        scan: ScanId,
        function_oid: pg_sys::Oid,
        result_type: pg_sys::Oid,
    ) -> Result<Self, QueryPlanError> {
        let result = ExprType {
            type_oid: result_type,
            typmod: -1,
            collation: pg_sys::InvalidOid,
        };
        let output = OutputId::from_index(0);
        let aggregate = AggCall::try_new(
            function_oid,
            AggregateArguments::None,
            false,
            Box::new([]),
            None,
            result,
            output,
        )?;
        let scan = QueryNode::Scan(ScanNode::new(scan, Box::new([]), None));
        let aggregate = QueryNode::Aggregate(AggregateNode::new(
            scan,
            Box::new([]),
            Box::new([aggregate]),
            1.0,
        )?);
        let fragment = QueryFragment::new(QueryNode::Project(ProjectNode::new(
            aggregate,
            Box::new([ProjectExpr::new(
                super::ExecutionExpr::Output(output),
                result,
                output,
                false,
            )]),
        )));
        Self::new(
            fragment,
            QueryTupleLayout::scalar_count(output, result_type),
            RuntimeValueLayout::new(Box::new([])),
            1,
        )
    }

    #[inline]
    pub const fn fragment(&self) -> &QueryFragment {
        &self.fragment
    }

    #[inline]
    pub const fn tuple_layout(&self) -> &QueryTupleLayout {
        &self.tuple_layout
    }

    #[inline]
    pub const fn runtime_values(&self) -> &RuntimeValueLayout {
        &self.runtime_values
    }

    /// Append late-planned runtime values after exact-expression planning has
    /// fixed every existing identity. Provider pruning and upper operators
    /// both use this single dense layout contract.
    pub fn try_append_runtime_values(
        &mut self,
        additional: &[RuntimeValueSpec],
    ) -> Option<usize> {
        let start = self.runtime_values.len();
        let total = start.checked_add(additional.len())?;
        let mut values = Vec::with_capacity(total);
        values.extend_from_slice(self.runtime_values.values());
        values.extend_from_slice(additional);
        self.runtime_values = RuntimeValueLayout::new(values.into_boxed_slice());
        Some(start)
    }

    pub fn into_parts(self) -> (QueryFragment, QueryTupleLayout, RuntimeValueLayout) {
        (self.fragment, self.tuple_layout, self.runtime_values)
    }

    pub(crate) fn validate(&self, scan_count: usize) -> Result<(), QueryPlanError> {
        let output_count = Self::semantic_output_count(self.fragment.root());
        self.fragment.validate(scan_count, output_count)?;
        PlanExpressionValidator::new(&self.runtime_values, output_count)
            .validate(self.fragment.root())?;
        let project = self
            .fragment
            .output_project()
            .expect("query topology validation requires an output Project");
        if project.expressions().len() != self.tuple_layout.len()
            || project
                .expressions()
                .iter()
                .zip(self.tuple_layout.slots())
                .any(|(expression, slot)| expression.output() != slot.output())
        {
            return Err(QueryPlanError::TupleLayoutOutputMismatch);
        }
        for slot in self.tuple_layout.slots() {
            let (result_type, nullable) =
                Self::output_semantics(self.fragment.root(), slot.output())
                    .ok_or(QueryPlanError::TupleLayoutOutputMismatch)?;
            if slot.type_oid() != result_type.type_oid
                || slot.typmod() != result_type.typmod
                || slot.collation() != result_type.collation
            {
                return Err(QueryPlanError::TupleLayoutTypeMismatch);
            }
            if slot.nullable() != nullable {
                return Err(QueryPlanError::TupleLayoutNullabilityMismatch);
            }
        }
        Ok(())
    }

    fn semantic_output_count(node: &QueryNode) -> usize {
        match node {
            QueryNode::Scan(_) | QueryNode::Join(_) => 0,
            QueryNode::Aggregate(aggregate) => aggregate
                .groups()
                .iter()
                .map(GroupExpr::output)
                .chain(aggregate.aggregates().iter().map(AggCall::output))
                .map(|output| output.index() + 1)
                .max()
                .unwrap_or(0),
            QueryNode::Distinct(distinct) => distinct
                .keys()
                .iter()
                .map(DistinctExpr::output)
                .map(|output| output.index() + 1)
                .max()
                .unwrap_or(0),
            QueryNode::Filter(filter) => Self::semantic_output_count(filter.input()),
            QueryNode::Sort(sort) => Self::semantic_output_count(sort.input()),
            QueryNode::Limit(limit) => Self::semantic_output_count(limit.input()),
            QueryNode::Project(project) => project
                .expressions()
                .iter()
                .map(|expression| expression.output().index() + 1)
                .chain(std::iter::once(Self::semantic_output_count(
                    project.input(),
                )))
                .max()
                .unwrap_or(0),
        }
    }

    fn output_semantics(
        node: &QueryNode,
        output: OutputId,
    ) -> Option<(ExprType, bool)> {
        match node {
            QueryNode::Scan(_) | QueryNode::Join(_) => None,
            QueryNode::Aggregate(aggregate) => aggregate
                .groups()
                .iter()
                .find(|group| group.output() == output)
                .map(|group| (group.result_type(), true))
                .or_else(|| {
                    aggregate
                        .aggregates()
                        .iter()
                        .find(|aggregate| aggregate.output() == output)
                        .map(|aggregate| {
                            (aggregate.result_type(), aggregate.nullable())
                        })
                }),
            QueryNode::Distinct(distinct) => distinct
                .keys()
                .iter()
                .find(|key| key.output() == output)
                .map(|key| (key.result_type(), true)),
            QueryNode::Filter(filter) => {
                Self::output_semantics(filter.input(), output)
            }
            QueryNode::Sort(sort) => Self::output_semantics(sort.input(), output),
            QueryNode::Limit(limit) => Self::output_semantics(limit.input(), output),
            QueryNode::Project(project) => project
                .expressions()
                .iter()
                .find(|expression| expression.output() == output)
                .map(|expression| (expression.result_type(), expression.nullable())),
        }
    }

    pub fn encode(
        &self,
        scan_count: usize,
    ) -> Result<*mut pg_sys::List, QueryPlanDataError> {
        self.validate(scan_count)?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_nested(|layout| self.runtime_values.encode_plan_data(layout))
                .append_nested(|fragment| {
                    Self::encode_node(self.fragment.root(), fragment)
                })
                .append_nested(|layout| {
                    Self::encode_layout(&self.tuple_layout, layout)
                });
            Ok(())
        })
    }

    /// # Safety
    ///
    /// `list` must be a live PostgreSQL plan-data list for the duration of this
    /// call. Its nested lists must belong to the same live planner allocation.
    pub unsafe fn decode(
        list: *mut pg_sys::List,
        scan_count: usize,
    ) -> Result<Self, QueryPlanDataError> {
        unsafe {
            PlanDataReader::decode_checked_list(list, 0, |reader| {
                let runtime_values = reader.read_nested(|record| {
                    RuntimeValueLayout::decode_plan_data(record, ())
                })?;
                let root = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_values.len())
                })?;
                let tuple_layout = reader.read_nested(Self::decode_layout)?;
                Ok(Self::new(
                    QueryFragment::new(root),
                    tuple_layout,
                    runtime_values,
                    scan_count,
                )?)
            })
        }
    }
}
