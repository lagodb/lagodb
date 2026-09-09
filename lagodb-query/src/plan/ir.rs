//! Provider-neutral relational IR for query offload.

use lagodb_core::expr::ColumnRef;
use lagodb_core::query_contract::ScanId;

use super::ExecutionExpr;
use super::aggregate::{AggCall, AggregateNode, GroupExpr};
use super::distinct::{DistinctExpr, DistinctNode};
use super::join::JoinNode;
use super::limit::LimitNode;
use super::project::ProjectNode;
use super::sort::SortNode;

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
    #[error("join node must contain an equi key, except for a semi/anti join")]
    EmptyJoinKeys,
    #[error("join key is outside the exact DataFusion hash-equality contract")]
    UnsupportedJoinKey,
    #[error("join key does not reference one column from each input")]
    MismatchedJoinKey,
    #[error("operator row estimate must be finite and non-negative")]
    InvalidOperatorRows,
    #[error("aggregate node must contain at least one group key or aggregate")]
    EmptyAggregate,
    #[error("distinct node must contain at least one key")]
    EmptyDistinct,
    #[error("sort node must contain at least one key")]
    EmptySort,
    #[error("sort key is outside the exact DataFusion ordering contract")]
    UnsupportedSortKey,
    #[error("limit node must contain LIMIT or OFFSET")]
    EmptyLimit,
    #[error("limit cost estimate does not match its LIMIT/OFFSET expressions")]
    InvalidLimitEstimate,
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
    #[error("aggregate OID/type/option combination is not supported")]
    UnsupportedAggregate,
    #[error("group key is outside the exact hash-grouping contract")]
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
    estimated_rows: RowEstimate,
}

impl FilterNode {
    pub fn new(
        input: QueryNode,
        predicate: ExecutionExpr,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        Ok(Self {
            input: Box::new(input),
            predicate,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    pub const fn predicate(&self) -> &ExecutionExpr {
        &self.predicate
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowEstimate(u64);

impl RowEstimate {
    pub(super) fn try_new(rows: f64) -> Result<Self, QueryPlanError> {
        if !rows.is_finite() || rows < 0.0 {
            return Err(QueryPlanError::InvalidOperatorRows);
        }
        Ok(Self(rows.to_bits()))
    }

    #[inline]
    pub(super) const fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNode {
    Scan(ScanNode),
    Join(JoinNode),
    Aggregate(AggregateNode),
    Distinct(DistinctNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Sort(SortNode),
    Limit(LimitNode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFragment {
    root: QueryNode,
}

impl QueryFragment {
    pub fn new(root: QueryNode) -> Self {
        Self { root }
    }

    #[inline]
    pub const fn root(&self) -> &QueryNode {
        &self.root
    }

    pub fn into_root(self) -> QueryNode {
        self.root
    }

    pub(crate) fn output_project(&self) -> Option<&ProjectNode> {
        let mut node = &self.root;
        if let QueryNode::Limit(limit) = node {
            node = limit.input();
        }
        if let QueryNode::Sort(sort) = node {
            node = sort.input();
        }
        match node {
            QueryNode::Project(project) => Some(project),
            _ => None,
        }
    }

    /// Whether the validated operator tree contains a post-aggregate filter.
    ///
    /// Join post-filters are also standalone Filter nodes, so only a Filter
    /// directly above Aggregate represents HAVING.
    pub(crate) fn has_having_filter(&self) -> bool {
        matches!(
            self.output_project(),
            Some(project)
                if matches!(project.input(), QueryNode::Filter(filter)
                    if matches!(filter.input(), QueryNode::Aggregate(_)))
        )
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
        let Some(project) = self.output_project() else {
            return Err(QueryPlanError::RootNotProjection);
        };
        match project.input() {
            QueryNode::Aggregate(aggregate) => {
                Self::validate_aggregate_input(aggregate.input())
            }
            QueryNode::Distinct(distinct) => {
                Self::validate_relation_tree(distinct.input())
            }
            QueryNode::Filter(filter) => match filter.input() {
                QueryNode::Aggregate(aggregate) => {
                    Self::validate_aggregate_input(aggregate.input())
                }
                QueryNode::Join(join) => Self::validate_binary_join(join),
                _ => Err(QueryPlanError::UnsupportedTopology),
            },
            QueryNode::Join(join) => Self::validate_binary_join(join),
            _ => Err(QueryPlanError::UnsupportedTopology),
        }
    }

    fn validate_aggregate_input(input: &QueryNode) -> Result<(), QueryPlanError> {
        // Join-type capability belongs to the shared relation tree. The node
        // validator below separately proves that aggregate expressions only
        // reference columns emitted by that tree.
        Self::validate_relation_tree(input)
    }

    fn validate_binary_join(join: &JoinNode) -> Result<(), QueryPlanError> {
        Self::validate_relation_tree(join.left())?;
        Self::validate_relation_tree(join.right())
    }

    fn validate_relation_tree(node: &QueryNode) -> Result<(), QueryPlanError> {
        match node {
            QueryNode::Scan(_) => Ok(()),
            QueryNode::Filter(filter) => Self::validate_relation_tree(filter.input()),
            QueryNode::Join(join) => {
                Self::validate_relation_tree(join.left())?;
                Self::validate_relation_tree(join.right())
            }
            _ => Err(QueryPlanError::UnsupportedTopology),
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
            QueryNode::Join(join) => {
                let _ = Self::validate_node(join.left(), used_scans, output_count)?;
                let _ = Self::validate_node(join.right(), used_scans, output_count)?;
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
                let _ =
                    Self::validate_node(project.input(), used_scans, output_count)?;
                let mut outputs = vec![false; output_count];
                for expression in project.expressions() {
                    let index = expression.output().index();
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
            QueryNode::Sort(sort) => {
                Self::validate_node(sort.input(), used_scans, output_count)
            }
            QueryNode::Limit(limit) => {
                Self::validate_node(limit.input(), used_scans, output_count)
            }
        }
    }

    pub fn scan(&self, scan: ScanId) -> Option<&ScanNode> {
        Self::find_scan(&self.root, scan)
    }

    fn find_scan(node: &QueryNode, scan: ScanId) -> Option<&ScanNode> {
        match node {
            QueryNode::Scan(node) => (node.scan() == scan).then_some(node),
            QueryNode::Join(node) => Self::find_scan(node.left(), scan)
                .or_else(|| Self::find_scan(node.right(), scan)),
            QueryNode::Aggregate(node) => Self::find_scan(node.input(), scan),
            QueryNode::Distinct(node) => Self::find_scan(node.input(), scan),
            QueryNode::Filter(node) => Self::find_scan(node.input(), scan),
            QueryNode::Project(node) => Self::find_scan(node.input(), scan),
            QueryNode::Sort(node) => Self::find_scan(node.input(), scan),
            QueryNode::Limit(node) => Self::find_scan(node.input(), scan),
        }
    }
}
