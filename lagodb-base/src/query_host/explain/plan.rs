//! PostgreSQL-facing query-offload plan presentation.

use std::ffi::{CStr, CString};

use lagodb_core::expr::ColumnRef;
use lagodb_core::expr::explain::{FILTER, PUSHED_FILTER, PUSHED_FILTER_CONSERVATIVE};
use lagodb_core::query_contract::{ScanCost, ScanId, TableScanRouteKind};
use lagodb_query::datafusion::{
    ExecutionMetricsSnapshot, ScanExecutionMetricsSnapshot,
};
use lagodb_query::plan::{
    JoinType, PlanExplainNode, PlanExplainProperty, PlanExplainRelation,
    PlannedTableScan, QueryFragment, QueryNode, SortDirection,
};
use pgrx::pg_sys;

use super::ExplainOptions;
use super::expression::ExpressionFormatter;

struct ScanFilterExplain {
    exact: CString,
    pushed: Option<CString>,
}

pub(super) struct ScanExplainMetadata {
    route_kind: TableScanRouteKind,
    route_name: CString,
    schema_name: CString,
    relation_name: CString,
    alias: CString,
    cost: ScanCost,
    filter: Option<ScanFilterExplain>,
    column_names: Box<[Option<CString>]>,
}

impl ScanExplainMetadata {
    /// Capture catalog names once when EXPLAIN presentation is requested,
    /// outside every row and batch processing path.
    pub(super) unsafe fn capture(
        scans: &[PlannedTableScan<'_>],
        fragment: &QueryFragment,
    ) -> Box<[Self]> {
        scans
            .iter()
            .enumerate()
            .map(|(index, scan)| {
                let scan_id = ScanId::from_index(index);
                let columns = fragment
                    .scan(scan_id)
                    .expect("validated selected plan contains every dense scan");
                let max_attno = columns
                    .columns()
                    .iter()
                    .map(|column| column.attno as usize)
                    .max()
                    .unwrap_or(0);
                let mut column_names = vec![None; max_attno + 1];
                for column in columns.columns() {
                    let name = unsafe {
                        CStr::from_ptr(pg_sys::get_attname(
                            scan.relation_oid(),
                            column.attno,
                            false,
                        ))
                    };
                    column_names[column.attno as usize] = Some(name.to_owned());
                }
                let relation_name = unsafe {
                    CStr::from_ptr(pg_sys::get_rel_name(scan.relation_oid()))
                };
                let schema_name = unsafe {
                    CStr::from_ptr(pg_sys::get_namespace_name_or_temp(
                        pg_sys::get_rel_namespace(scan.relation_oid()),
                    ))
                };
                Self {
                    route_kind: scan.route().kind(),
                    route_name: scan.route().name().to_owned(),
                    schema_name: schema_name.to_owned(),
                    relation_name: relation_name.to_owned(),
                    alias: scan.alias().to_owned(),
                    cost: scan.cost(),
                    filter: scan.filter_explain().map(|filter| ScanFilterExplain {
                        exact: filter.exact_expression().to_owned(),
                        pushed: filter.pushed_expression().map(CStr::to_owned),
                    }),
                    column_names: column_names.into_boxed_slice(),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }

    pub(super) fn column_name(&self, attno: pg_sys::AttrNumber) -> &CStr {
        self.column_names[attno as usize]
            .as_deref()
            .expect("validated scan projection has captured column metadata")
    }

    pub(super) fn alias(&self) -> &CStr {
        &self.alias
    }

    fn relation(&self, verbose: bool) -> PlanExplainRelation {
        PlanExplainRelation::new(
            self.relation_name.to_string_lossy(),
            self.alias.to_string_lossy(),
            verbose.then(|| self.schema_name.to_string_lossy()),
        )
    }
}

pub(super) struct QueryExplainPlan<'a> {
    fragment: &'a QueryFragment,
    scans: &'a [ScanExplainMetadata],
    options: ExplainOptions,
    metrics: Option<&'a ExecutionMetricsSnapshot>,
    expressions: ExpressionFormatter<'a>,
}

impl<'a> QueryExplainPlan<'a> {
    pub(super) const fn new(
        fragment: &'a QueryFragment,
        scans: &'a [ScanExplainMetadata],
        options: ExplainOptions,
        metrics: Option<&'a ExecutionMetricsSnapshot>,
    ) -> Self {
        Self {
            fragment,
            scans,
            options,
            metrics,
            expressions: ExpressionFormatter::new(scans, options.verbose),
        }
    }

    pub(super) fn build(&self) -> PlanExplainNode {
        self.node(self.fragment.root())
    }

    fn node(&self, node: &QueryNode) -> PlanExplainNode {
        match node {
            QueryNode::Scan(scan) => self.scan(scan.scan(), scan.columns()),
            QueryNode::Join(join) => {
                let join_type = match join.join_type() {
                    JoinType::Inner => "Inner",
                    JoinType::Left => "Left",
                    JoinType::Right => "Right",
                    JoinType::Full => "Full",
                    JoinType::LeftSemi => "Left Semi",
                    JoinType::LeftAnti => "Left Anti",
                    JoinType::LeftMark => "Left Mark",
                };
                let mut properties =
                    vec![PlanExplainNode::property("Join Type", join_type)];
                if !join.keys().is_empty() {
                    let condition = join
                        .keys()
                        .iter()
                        .map(|key| {
                            format!(
                                "{} = {}",
                                self.expressions.column(key.left()),
                                self.expressions.column(key.right())
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    properties
                        .push(PlanExplainNode::property("Hash Cond", condition));
                }
                if let Some(filter) = join.on_filter() {
                    properties.push(PlanExplainNode::property(
                        "Join Filter",
                        self.expressions.expression(filter).to_string(),
                    ));
                }
                if let Some(filter) = join.mark_filter() {
                    properties.push(PlanExplainNode::property(
                        "Mark Filter",
                        format!(
                            "mark = {} OR {} IS NULL",
                            !filter.anti(),
                            self.expressions.column(filter.null_test())
                        ),
                    ));
                }
                if join.null_aware() {
                    properties
                        .push(PlanExplainNode::boolean_property("Null Aware", true));
                }
                PlanExplainNode::new(
                    "Hash Join",
                    properties,
                    vec![self.node(join.left()), self.node(join.right())],
                )
            }
            QueryNode::Aggregate(aggregate) => {
                let mut properties = Vec::new();
                if !aggregate.groups().is_empty() {
                    properties.push(PlanExplainNode::property(
                        "Group Key",
                        aggregate
                            .groups()
                            .iter()
                            .map(|group| {
                                self.expressions
                                    .expression(group.expression())
                                    .to_string()
                            })
                            .collect::<Vec<_>>()
                            .join(", "),
                    ));
                }
                if !aggregate.aggregates().is_empty() {
                    properties.push(PlanExplainNode::property(
                        "Aggregates",
                        aggregate
                            .aggregates()
                            .iter()
                            .map(|aggregate| self.expressions.aggregate(aggregate))
                            .collect::<Vec<_>>()
                            .join(", "),
                    ));
                }
                PlanExplainNode::new(
                    "Aggregate",
                    properties,
                    vec![self.node(aggregate.input())],
                )
            }
            QueryNode::Distinct(distinct) => PlanExplainNode::new(
                "Distinct",
                vec![PlanExplainNode::property(
                    "Distinct Key",
                    distinct
                        .keys()
                        .iter()
                        .map(|key| {
                            self.expressions.expression(key.expression()).to_string()
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                )],
                vec![self.node(distinct.input())],
            ),
            QueryNode::Filter(filter) => PlanExplainNode::new(
                "Filter",
                vec![PlanExplainNode::property(
                    FILTER.to_str().expect("static EXPLAIN label is UTF-8"),
                    self.expressions
                        .expression_from(filter.predicate(), filter.input())
                        .to_string(),
                )],
                vec![self.node(filter.input())],
            ),
            QueryNode::Project(project)
                if !self.options.verbose && project.is_identity() =>
            {
                self.node(project.input())
            }
            QueryNode::Project(project) => {
                let properties = self.options.verbose.then(|| {
                    PlanExplainNode::property(
                        "Output",
                        project
                            .expressions()
                            .iter()
                            .map(|expression| {
                                self.expressions
                                    .expression_from(
                                        expression.expression(),
                                        project.input(),
                                    )
                                    .to_string()
                            })
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                });
                PlanExplainNode::new(
                    "Projection",
                    properties.into_iter().collect(),
                    vec![self.node(project.input())],
                )
            }
            QueryNode::Sort(sort) => PlanExplainNode::new(
                "Sort",
                vec![PlanExplainNode::property(
                    "Sort Key",
                    sort.keys()
                        .iter()
                        .map(|key| {
                            let direction = match key.direction() {
                                SortDirection::Ascending => "ASC",
                                SortDirection::Descending => "DESC",
                            };
                            let nulls = if key.nulls_first() {
                                "NULLS FIRST"
                            } else {
                                "NULLS LAST"
                            };
                            format!(
                                "{} {direction} {nulls}",
                                self.expressions.output(sort.input(), key.output())
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                )],
                vec![self.node(sort.input())],
            ),
            QueryNode::Limit(limit) => {
                let mut properties = Vec::new();
                if self.options.verbose
                    && let Some(offset) = limit.offset()
                {
                    properties.push(PlanExplainNode::property(
                        "Offset",
                        format!("Runtime Binding {}", offset.index() + 1),
                    ));
                }
                if self.options.verbose
                    && let Some(count) = limit.count()
                {
                    properties.push(PlanExplainNode::property(
                        "Count",
                        format!("Runtime Binding {}", count.index() + 1),
                    ));
                }
                PlanExplainNode::new(
                    "Limit",
                    properties,
                    vec![self.node(limit.input())],
                )
            }
        }
    }

    fn scan(&self, scan: ScanId, columns: &[ColumnRef]) -> PlanExplainNode {
        let metadata = &self.scans[scan.index()];
        let route_label = match metadata.route_kind {
            TableScanRouteKind::AccessMethod => "Access Method",
            TableScanRouteKind::ForeignDataWrapper => "Foreign Data Wrapper",
        };
        let mut properties = vec![PlanExplainNode::property(
            route_label,
            metadata.route_name.to_string_lossy(),
        )];
        if self.options.verbose {
            properties.push(PlanExplainNode::uinteger_property(
                "Scan ID",
                u64::try_from(scan.index())
                    .expect("validated dense scan index fits in u64"),
            ));
            if !columns.is_empty() {
                properties.push(PlanExplainNode::property(
                    "Output",
                    columns
                        .iter()
                        .map(|column| self.expressions.column(*column).to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
            }
        }
        if self.options.costs {
            properties.push(PlanExplainNode::float_property(
                "Estimated Rows Read",
                metadata.cost.rows_read(),
                0,
                None,
            ));
            properties.push(PlanExplainNode::float_property(
                "Estimated Bytes Read",
                metadata.cost.bytes_read(),
                0,
                Some("bytes"),
            ));
            if self.options.verbose {
                properties.push(PlanExplainNode::float_property(
                    "Provider Startup Cost",
                    metadata.cost.startup_cost(),
                    2,
                    None,
                ));
            }
        }
        if let Some(filter) = &metadata.filter {
            properties.push(PlanExplainNode::property(
                FILTER.to_str().expect("static EXPLAIN label is UTF-8"),
                filter.exact.to_string_lossy(),
            ));
            if let Some(pushed) = &filter.pushed {
                let label = if self.options.verbose {
                    PUSHED_FILTER_CONSERVATIVE
                } else {
                    PUSHED_FILTER
                };
                properties.push(PlanExplainNode::property(
                    label.to_str().expect("static EXPLAIN label is UTF-8"),
                    pushed.to_string_lossy(),
                ));
            }
        }
        if self.options.analyze
            && let Some(metrics) = self.metrics
        {
            Self::append_actual_scan_properties(
                metrics.scan(scan),
                self.options.verbose,
                &mut properties,
            );
        }
        PlanExplainNode::new("Table Scan", properties, Vec::new())
            .with_relation(metadata.relation(self.options.verbose))
    }

    fn append_actual_scan_properties(
        metrics: ScanExecutionMetricsSnapshot,
        verbose: bool,
        properties: &mut Vec<PlanExplainProperty>,
    ) {
        properties.push(PlanExplainNode::uinteger_property(
            "Actual Input Batches",
            metrics.input_batches,
        ));
        properties.push(PlanExplainNode::uinteger_property(
            "Actual Input Rows",
            metrics.input_rows,
        ));
        properties.push(PlanExplainNode::uinteger_property(
            "Scan Tasks",
            metrics.planned_tasks,
        ));
        properties.push(PlanExplainNode::uinteger_property(
            "Data Files Selected",
            metrics.planned_files,
        ));
        properties.push(PlanExplainNode::uinteger_property(
            "Selected File Bytes",
            metrics.planned_bytes,
        ));
        if verbose {
            properties.push(PlanExplainNode::uinteger_property(
                "Arrow Batch Bytes",
                metrics.arrow_batch_bytes,
            ));
        }
    }
}
