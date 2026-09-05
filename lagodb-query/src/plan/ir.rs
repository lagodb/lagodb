//! Provider-neutral relational IR for query offload.

use lagodb_core::expr::ColumnRef;
use lagodb_core::query_contract::{OutputId, ScanId};

use super::ExecutionExpr;
use super::aggregate::{AggCall, AggregateKind, AggregateNode, GroupExpr};
use super::distinct::{DistinctExpr, DistinctNode};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueryPlanError {
    #[error("scan identity {index} is outside the planned scan table")]
    ScanOutOfBounds { index: usize },
    #[error("planned scan identity {index} is not referenced by the query fragment")]
    UnusedScan { index: usize },
    #[error("scan identity {index} is referenced more than once")]
    DuplicateScanReference { index: usize },
    #[error("output identity {index} is outside the query output layout")]
    OutputOutOfBounds { index: usize },
    #[error("output identity {index} is defined more than once")]
    DuplicateOutput { index: usize },
    #[error("projection output identity {index} is not produced by its input")]
    ProjectOutputMismatch { index: usize },
    #[error("aggregate node must contain at least one group key or aggregate")]
    EmptyAggregate,
    #[error("distinct node must contain at least one key")]
    EmptyDistinct,
    #[error("projection node must contain at least one output")]
    EmptyProjection,
    #[error("scan projection must contain every referenced source column")]
    MissingProjectedColumn,
    #[error("scan projection contains a column belonging to another scan")]
    MismatchedScanColumn,
    #[error("scan projection contains the same attribute more than once")]
    DuplicateProjectedColumn,
    #[error("predicate is outside the proven integer scalar semantics")]
    UnsupportedPredicate,
    #[error("runtime value identity is outside the query runtime layout")]
    RuntimeValueOutOfBounds,
    #[error("runtime value type has no DataFusion scalar representation")]
    UnsupportedRuntimeValueType,
    #[error("query runtime layout contains PARAM_EXEC or outer values")]
    UnsupportedRuntimeValueSource,
    #[error("aggregate OID/type/option combination is not supported")]
    UnsupportedAggregate,
    #[error("group key must be an uncollated int4 or int8 source column")]
    UnsupportedGroupKey,
    #[error("DISTINCT key is outside the supported direct-column semantics")]
    UnsupportedDistinctKey,
    #[error("query output layout differs from the fragment outputs")]
    TupleLayoutOutputMismatch,
    #[error("query fragment root must be a projection")]
    RootNotProjection,
    #[error("query fragment topology is outside the implemented offload pipeline")]
    UnsupportedTopology,
    #[error("query output layout type differs from an expression result type")]
    TupleLayoutTypeMismatch,
    #[error("query output layout nullability differs from expression semantics")]
    TupleLayoutNullabilityMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanNode {
    scan: ScanId,
    columns: Box<[ColumnRef]>,
    filter: Option<ExecutionExpr>,
}

impl ScanNode {
    pub fn new(
        scan: ScanId,
        columns: Box<[ColumnRef]>,
        filter: Option<ExecutionExpr>,
    ) -> Self {
        Self {
            scan,
            columns,
            filter,
        }
    }

    #[inline]
    pub const fn scan(&self) -> ScanId {
        self.scan
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnRef] {
        &self.columns
    }

    #[inline]
    pub const fn filter(&self) -> Option<&ExecutionExpr> {
        self.filter.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterNode {
    input: Box<QueryNode>,
    predicate: ExecutionExpr,
}

impl FilterNode {
    pub fn new(input: QueryNode, predicate: ExecutionExpr) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }

    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    pub const fn predicate(&self) -> &ExecutionExpr {
        &self.predicate
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectNode {
    input: Box<QueryNode>,
    outputs: Box<[OutputId]>,
}

impl ProjectNode {
    pub fn new(
        input: QueryNode,
        outputs: Box<[OutputId]>,
    ) -> Result<Self, QueryPlanError> {
        if outputs.is_empty() {
            return Err(QueryPlanError::EmptyProjection);
        }
        Ok(Self {
            input: Box::new(input),
            outputs,
        })
    }

    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    pub fn outputs(&self) -> &[OutputId] {
        &self.outputs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNode {
    Scan(ScanNode),
    Aggregate(AggregateNode),
    Distinct(DistinctNode),
    Filter(FilterNode),
    Project(ProjectNode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFragment {
    root: QueryNode,
}

/// Planner-semantic facts exposed to PostgreSQL EXPLAIN without preparing a
/// source or lowering a DataFusion plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryPlanSummary {
    group_keys: usize,
    distinct_keys: usize,
    count_star: usize,
    count_expr: usize,
    min: usize,
    max: usize,
    sum: usize,
    avg: usize,
    variance: usize,
    stddev: usize,
    boolean: usize,
    array_agg: usize,
    string_agg: usize,
    distinct_aggregates: usize,
    ordered_aggregates: usize,
    aggregate_filters: usize,
    having_filters: usize,
    postgres_expression_fallbacks: usize,
}

impl QueryPlanSummary {
    fn record(&mut self, node: &QueryNode) {
        match node {
            QueryNode::Scan(scan) => {
                self.postgres_expression_fallbacks += scan
                    .filter()
                    .map_or(0, ExecutionExpr::postgres_fallback_count);
            }
            QueryNode::Aggregate(aggregate) => {
                self.group_keys += aggregate.groups().len();
                self.postgres_expression_fallbacks += aggregate
                    .groups()
                    .iter()
                    .map(|group| group.expression().postgres_fallback_count())
                    .sum::<usize>();
                for aggregate in aggregate.aggregates() {
                    match aggregate.kind() {
                        AggregateKind::Count if aggregate.argument().is_none() => {
                            self.count_star += 1;
                        }
                        AggregateKind::Count => self.count_expr += 1,
                        AggregateKind::Min => self.min += 1,
                        AggregateKind::Max => self.max += 1,
                        AggregateKind::Sum | AggregateKind::NumericSum => {
                            self.sum += 1;
                        }
                        AggregateKind::Average | AggregateKind::NumericAverage => {
                            self.avg += 1;
                        }
                        AggregateKind::VarianceSample
                        | AggregateKind::VariancePopulation => {
                            self.variance += 1;
                        }
                        AggregateKind::StddevSample
                        | AggregateKind::StddevPopulation => {
                            self.stddev += 1;
                        }
                        AggregateKind::BoolAnd | AggregateKind::BoolOr => {
                            self.boolean += 1;
                        }
                        AggregateKind::ArrayAgg => self.array_agg += 1,
                        AggregateKind::StringAgg => self.string_agg += 1,
                    }
                    self.distinct_aggregates += usize::from(aggregate.is_distinct());
                    self.ordered_aggregates +=
                        usize::from(!aggregate.order_by().is_empty());
                    self.aggregate_filters +=
                        usize::from(aggregate.filter().is_some());
                    self.postgres_expression_fallbacks += aggregate
                        .argument()
                        .map_or(0, ExecutionExpr::postgres_fallback_count)
                        + aggregate
                            .filter()
                            .map_or(0, ExecutionExpr::postgres_fallback_count);
                }
                self.record(aggregate.input());
            }
            QueryNode::Distinct(distinct) => {
                self.distinct_keys += distinct.keys().len();
                self.postgres_expression_fallbacks += distinct
                    .keys()
                    .iter()
                    .map(|key| key.expression().postgres_fallback_count())
                    .sum::<usize>();
                self.record(distinct.input());
            }
            QueryNode::Filter(filter) => {
                self.having_filters += 1;
                self.postgres_expression_fallbacks +=
                    filter.predicate().postgres_fallback_count();
                self.record(filter.input());
            }
            QueryNode::Project(project) => self.record(project.input()),
        }
    }

    #[inline]
    pub const fn group_keys(self) -> usize {
        self.group_keys
    }

    #[inline]
    pub const fn distinct_keys(self) -> usize {
        self.distinct_keys
    }

    #[inline]
    pub const fn count_star(self) -> usize {
        self.count_star
    }

    #[inline]
    pub const fn count_expr(self) -> usize {
        self.count_expr
    }

    #[inline]
    pub const fn min(self) -> usize {
        self.min
    }

    #[inline]
    pub const fn max(self) -> usize {
        self.max
    }

    #[inline]
    pub const fn sum(self) -> usize {
        self.sum
    }

    #[inline]
    pub const fn avg(self) -> usize {
        self.avg
    }

    #[inline]
    pub const fn variance(self) -> usize {
        self.variance
    }

    #[inline]
    pub const fn stddev(self) -> usize {
        self.stddev
    }

    #[inline]
    pub const fn boolean(self) -> usize {
        self.boolean
    }

    #[inline]
    pub const fn array_agg(self) -> usize {
        self.array_agg
    }

    #[inline]
    pub const fn string_agg(self) -> usize {
        self.string_agg
    }

    #[inline]
    pub const fn distinct_aggregates(self) -> usize {
        self.distinct_aggregates
    }

    #[inline]
    pub const fn ordered_aggregates(self) -> usize {
        self.ordered_aggregates
    }

    #[inline]
    pub const fn aggregate_filters(self) -> usize {
        self.aggregate_filters
    }

    #[inline]
    pub const fn having_filters(self) -> usize {
        self.having_filters
    }

    #[inline]
    pub const fn postgres_expression_fallbacks(self) -> usize {
        self.postgres_expression_fallbacks
    }
}

impl QueryFragment {
    pub fn new(root: QueryNode) -> Self {
        Self { root }
    }

    #[inline]
    pub const fn root(&self) -> &QueryNode {
        &self.root
    }

    /// Whether the validated operator tree contains a post-aggregate filter.
    ///
    /// Scan predicates are stored on [`ScanNode`], so every standalone
    /// [`QueryNode::Filter`] in this IR is a HAVING filter.
    pub(crate) fn has_having_filter(&self) -> bool {
        matches!(
            &self.root,
            QueryNode::Project(project)
                if matches!(project.input(), QueryNode::Filter(_))
        )
    }

    pub fn summary(&self) -> QueryPlanSummary {
        let mut summary = QueryPlanSummary::default();
        summary.record(&self.root);
        summary
    }

    pub(crate) fn validate(
        &self,
        scan_count: usize,
        output_count: usize,
    ) -> Result<(), QueryPlanError> {
        self.validate_topology()?;
        let mut used_scans = vec![false; scan_count];
        let _ = Self::validate_node(&self.root, &mut used_scans, output_count)?;
        if let Some(index) = used_scans.iter().position(|used| !used) {
            return Err(QueryPlanError::UnusedScan { index });
        }
        Ok(())
    }

    fn validate_topology(&self) -> Result<(), QueryPlanError> {
        let QueryNode::Project(project) = &self.root else {
            return Err(QueryPlanError::RootNotProjection);
        };
        let input = match project.input() {
            QueryNode::Aggregate(aggregate) => aggregate.input(),
            QueryNode::Distinct(distinct) => distinct.input(),
            QueryNode::Filter(filter) => match filter.input() {
                QueryNode::Aggregate(aggregate) => aggregate.input(),
                _ => return Err(QueryPlanError::UnsupportedTopology),
            },
            _ => return Err(QueryPlanError::UnsupportedTopology),
        };
        if matches!(input, QueryNode::Scan(_)) {
            Ok(())
        } else {
            Err(QueryPlanError::UnsupportedTopology)
        }
    }

    fn validate_node(
        node: &QueryNode,
        used_scans: &mut [bool],
        output_count: usize,
    ) -> Result<Vec<bool>, QueryPlanError> {
        match node {
            QueryNode::Scan(scan) => {
                let index = scan.scan().index();
                let used = used_scans
                    .get_mut(index)
                    .ok_or(QueryPlanError::ScanOutOfBounds { index })?;
                if *used {
                    return Err(QueryPlanError::DuplicateScanReference { index });
                }
                *used = true;
                Ok(vec![false; output_count])
            }
            QueryNode::Aggregate(aggregate) => {
                let _ =
                    Self::validate_node(aggregate.input(), used_scans, output_count)?;
                let mut outputs = vec![false; output_count];
                for output in aggregate
                    .groups()
                    .iter()
                    .map(GroupExpr::output)
                    .chain(aggregate.aggregates().iter().map(AggCall::output))
                {
                    let index = output.index();
                    let defined = outputs
                        .get_mut(index)
                        .ok_or(QueryPlanError::OutputOutOfBounds { index })?;
                    if *defined {
                        return Err(QueryPlanError::DuplicateOutput { index });
                    }
                    *defined = true;
                }
                Ok(outputs)
            }
            QueryNode::Distinct(distinct) => {
                let _ =
                    Self::validate_node(distinct.input(), used_scans, output_count)?;
                let mut outputs = vec![false; output_count];
                for output in distinct.keys().iter().map(DistinctExpr::output) {
                    let index = output.index();
                    let defined = outputs
                        .get_mut(index)
                        .ok_or(QueryPlanError::OutputOutOfBounds { index })?;
                    if *defined {
                        return Err(QueryPlanError::DuplicateOutput { index });
                    }
                    *defined = true;
                }
                Ok(outputs)
            }
            QueryNode::Filter(filter) => {
                Self::validate_node(filter.input(), used_scans, output_count)
            }
            QueryNode::Project(project) => {
                let input =
                    Self::validate_node(project.input(), used_scans, output_count)?;
                let mut outputs = vec![false; output_count];
                for output in project.outputs() {
                    let index = output.index();
                    if !input.get(index).copied().unwrap_or(false) {
                        return Err(QueryPlanError::ProjectOutputMismatch { index });
                    }
                    outputs[index] = true;
                }
                Ok(outputs)
            }
        }
    }
}
