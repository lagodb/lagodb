use pg_lakebase_core::expr::predicate::{
    PlanPredicate, PlanPredicateContext, PlanScalar,
};
use pg_lakebase_core::expr::{
    PushdownContract, PushdownCosting, QualPushdownDecision,
};
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignPathBuilder, ForeignPathContext,
    ForeignPathKeys, ForeignPathSpec, ForeignPlanContext, ForeignPlanPrivate,
    ForeignPlanSpec, ForeignRelContext, ForeignRelSize, ForeignRelSizeContext,
    ForeignRowIdentityRequirement, ForeignScanError, PathVariantKind,
    ReScanForeignScanContext, RuntimeExpressionValues, ScanDatumWriter,
    ScanOutputColumn, ScanProjection, ScanProjectionPolicy, ScanSlotWriter,
};
use pgrx::pg_sys;
use pgrx::{FromDatum, IntoDatum};
use std::cmp::Reverse;

use pg_lakebase_core::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

use super::fixture::{TestRow, TestStore, TestTrace, TraceEvent};
use super::provider::FrameworkTestFdw;

const INT4EQ_OPNO: u32 = 96;

#[derive(Clone, Debug)]
pub struct ScanPrivate {
    ordered: bool,
    descending: bool,
    filters: Vec<FilterSpec>,
}

#[derive(Clone, Copy, Debug)]
struct FilterSpec {
    attno: pg_sys::AttrNumber,
    fdw_expr_index: usize,
}

impl ForeignPlanPrivate for ScanPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError> {
        writer
            .append_bool(self.ordered)
            .append_bool(self.descending)
            .append_nested(|writer| {
                for filter in &self.filters {
                    writer.append_nested(|entry| {
                        entry
                            .append_i32(filter.attno as i32)
                            .append_count(filter.fdw_expr_index);
                    });
                }
            });
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        Ok(Self {
            ordered: reader.read_bool()?,
            descending: reader.read_bool()?,
            filters: read_filter_specs(reader)?,
        })
    }
}

pub struct ScanPlannerState {
    relation_oid: pg_sys::Oid,
}

pub struct ScanState {
    rows: Vec<TestRow>,
    cursor: usize,
    ordered: bool,
    descending: bool,
    filter_specs: Vec<FilterSpec>,
    filters: Vec<RuntimeFilter>,
    output_columns: Vec<ScanOutputColumn>,
    item_pointer_identity: bool,
}

#[derive(Clone, Copy, Debug)]
struct RuntimeFilter {
    attno: pg_sys::AttrNumber,
    value: Option<i32>,
}

impl ScanState {
    fn new(
        rows: Vec<TestRow>,
        ordered: bool,
        descending: bool,
        filter_specs: Vec<FilterSpec>,
        filters: Vec<RuntimeFilter>,
        output_columns: Vec<ScanOutputColumn>,
        item_pointer_identity: bool,
    ) -> Self {
        let mut state = Self {
            rows,
            cursor: 0,
            ordered,
            descending,
            filter_specs,
            filters,
            output_columns,
            item_pointer_identity,
        };
        state.sort_rows();
        state
    }

    fn sort_rows(&mut self) {
        if self.ordered {
            if self.descending {
                self.rows.sort_by_key(|row| Reverse(row.sort_key));
            } else {
                self.rows.sort_by_key(|row| row.sort_key);
            }
        }
    }

    fn set_filters(&mut self, filters: Vec<RuntimeFilter>) {
        self.filters = filters;
        self.cursor = 0;
    }
}

impl FdwScan for FrameworkTestFdw {
    type PlannerState = ScanPlannerState;
    type PrivateData = ScanPrivate;
    type State = ScanState;

    fn init_planner(
        ctx: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError> {
        TestStore::ensure(ctx.relation_oid());
        Ok(ScanPlannerState {
            relation_oid: ctx.relation_oid(),
        })
    }

    fn classify_predicate(
        _ctx: &PlanPredicateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision {
        if is_int4_eq_comparison(predicate) {
            QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            }
        } else {
            QualPushdownDecision::Unsupported
        }
    }

    fn estimate(
        state: &mut Self::PlannerState,
        _ctx: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ForeignScanError> {
        let rows = TestStore::snapshot(state.relation_oid).len() as f64;
        Ok(ForeignRelSize::new(rows, 32))
    }

    fn build_paths(
        state: &Self::PlannerState,
        ctx: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<Self::PrivateData>,
    ) -> Result<(), ForeignScanError> {
        let rows = TestStore::snapshot(state.relation_oid).len() as f64;
        let private = ScanPrivate {
            ordered: false,
            descending: false,
            filters: Vec::new(),
        };

        let mut baseline = ForeignPathSpec::new(rows, 1.0, 100.0, private);
        baseline.retrieved_rows = rows;
        paths.push(baseline);

        if ctx.kind() == PathVariantKind::Plain {
            let query_pathkeys = ctx.relation().query_pathkeys();
            if !query_pathkeys.is_null()
                && unsafe { pg_sys::list_length(query_pathkeys) == 1 }
            {
                let mut ordered = ForeignPathSpec::new(
                    rows,
                    0.0,
                    1.0,
                    ScanPrivate {
                        ordered: true,
                        descending: first_pathkey_is_descending(query_pathkeys),
                        filters: Vec::new(),
                    },
                );
                // SAFETY: PostgreSQL owns query_pathkeys for the current
                // planner invocation and consumes this path before it ends.
                unsafe { ordered.set_pathkeys(query_pathkeys) };
                paths.push(ordered);
            }
        }
        Ok(())
    }

    fn supports_pathkeys(
        _state: &Self::PlannerState,
        _ctx: &ForeignPathContext<'_>,
        pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ForeignScanError> {
        for pathkey_index in 0..pathkeys.len() {
            let mut selected = None;
            for candidate_index in 0..pathkeys.candidate_count(pathkey_index) {
                let Some(candidate) =
                    pathkeys.candidate(pathkey_index, candidate_index)
                else {
                    continue;
                };
                if candidate.data_type() == pg_sys::INT4OID
                    && candidate.collation() == pg_sys::InvalidOid
                    && candidate.opfamily() != pg_sys::InvalidOid
                    && matches!(
                        candidate.strategy(),
                        value if value == pg_sys::BTLessStrategyNumber as i32
                            || value == pg_sys::BTGreaterStrategyNumber as i32
                    )
                    && sort_var_attno(candidate.expression()).is_some()
                {
                    selected = Some(candidate_index);
                    break;
                }
            }
            let Some(selected_candidate) = selected else {
                return Ok(false);
            };
            let selected_attno = pathkeys
                .candidate(pathkey_index, selected_candidate)
                .and_then(|candidate| sort_var_attno(candidate.expression()))
                .ok_or_else(|| {
                    ForeignScanError::unsupported(
                        "test FDW lost its selected sort-key expression",
                    )
                })?;
            TestTrace::record(TraceEvent::Pathkeys {
                candidate_count: pathkeys.candidate_count(pathkey_index),
                selected_candidate,
                selected_attno,
            });
            pathkeys.select_candidate(pathkey_index, selected_candidate)?;
        }
        Ok(true)
    }

    fn build_plan(
        _state: &mut Self::PlannerState,
        ctx: &ForeignPlanContext<'_, Self::PrivateData>,
    ) -> Result<ForeignPlanSpec<Self::PrivateData>, ForeignScanError> {
        let mut private = ctx.path_private().clone();
        private.filters.clear();
        let mut spec = ForeignPlanSpec::new(private);
        spec.required_columns.require_all_columns();
        spec.projection_policy = if ctx.is_modify_target() {
            ScanProjectionPolicy::RequireRelationShape
        } else {
            ScanProjectionPolicy::AllowColumnPruning
        };

        for (pushed_index, pushed) in ctx.pushdown().pushed.iter().enumerate() {
            if pushed.contract != PushdownContract::ExactRowFilter {
                return Err(ForeignScanError::unsupported(
                    "test FDW only supports exact pushed filters",
                ));
            }
            let column_ref = ctx
                .pushdown()
                .column_refs
                .iter()
                .find(|column_ref| column_ref.expr_index == pushed_index)
                .ok_or_else(|| {
                    ForeignScanError::unsupported(
                        "test FDW could not find the pushed filter column",
                    )
                })?;
            if !matches!(column_ref.attno, 1 | 2) {
                return Err(ForeignScanError::unsupported(
                    "test FDW exact filter uses an unsupported column",
                ));
            }
            let runtime_expression = runtime_filter_expression(
                pushed.expr,
                ctx.relation().scan_relid(),
            )
            .ok_or_else(|| {
                ForeignScanError::unsupported(
                    "test FDW could not find the non-scan side of the pushed equality",
                )
            })?;
            // SAFETY: `runtime_expression` is a planner-owned expression from
            // the final scan clause and is retained by PostgreSQL's plan tree.
            unsafe { spec.fdw_exprs.push(runtime_expression)? };
            spec.private_data.filters.push(FilterSpec {
                attno: column_ref.attno,
                fdw_expr_index: spec.private_data.filters.len(),
            });
        }
        Ok(spec)
    }

    fn begin(
        ctx: BeginForeignScanContext<'_, Self::PrivateData>,
    ) -> Result<Self::State, ForeignScanError> {
        let filters = runtime_filters(&ctx.private_data.filters, ctx.expressions)?;
        let output_columns = ctx.output_layout.columns().to_vec();
        TestTrace::record(TraceEvent::ScanBegin {
            ordered: ctx.private_data.ordered,
            pushed_count: ctx.pushdown.pushed_contracts().len(),
            filters: trace_filters(&filters),
            projection: projection_name(ctx.projection),
        });
        Ok(ScanState::new(
            TestStore::snapshot(ctx.relation.oid()),
            ctx.private_data.ordered,
            ctx.private_data.descending,
            ctx.private_data.filters.clone(),
            filters,
            output_columns,
            matches!(
                ctx.row_identity_requirement,
                ForeignRowIdentityRequirement::ItemPointer
            ),
        ))
    }

    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        while let Some(row) = state.rows.get(state.cursor).cloned() {
            state.cursor += 1;
            if !matches_filters(&state.filters, &row) {
                continue;
            }
            {
                // SAFETY: `output_columns` is the complete Begin-bound output
                // layout, and `write_row` writes every column exactly once.
                let mut datums = unsafe { output.datum_writer() };
                write_row(&mut datums, &row, &state.output_columns)?;
            }
            if state.item_pointer_identity {
                output.write_item_pointer(&row.item_pointer());
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn rescan(
        state: &mut Self::State,
        ctx: ReScanForeignScanContext<'_>,
    ) -> Result<(), ForeignScanError> {
        let filters = runtime_filters(&state.filter_specs, ctx.expressions)?;
        state.set_filters(filters);
        state.rows = TestStore::snapshot(ctx.relation.oid());
        state.sort_rows();
        TestTrace::record(TraceEvent::ScanRescan {
            params_changed: ctx.params_changed,
            filters: trace_filters(&state.filters),
        });
        Ok(())
    }

    fn end(_state: &mut Self::State) -> Result<(), ForeignScanError> {
        Ok(())
    }
}

fn is_int4_eq_comparison(predicate: &PlanPredicate) -> bool {
    match predicate {
        PlanPredicate::Comparison { op, left, right } => {
            let accepted_shape = matches!(
                (left, right),
                (
                    PlanScalar::Column(_),
                    PlanScalar::Literal(_) | PlanScalar::Dynamic(_),
                ) | (
                    PlanScalar::Literal(_) | PlanScalar::Dynamic(_),
                    PlanScalar::Column(_),
                )
            );
            accepted_shape
                && predicate.scan_column_type() == Some(pg_sys::INT4OID)
                && op.opno == pg_sys::Oid::from(INT4EQ_OPNO)
        }
        _ => false,
    }
}

fn sort_var_attno(expression: *mut pg_sys::Expr) -> Option<pg_sys::AttrNumber> {
    if expression.is_null() {
        return None;
    }
    unsafe {
        ((*expression).type_ == pg_sys::NodeTag::T_Var
            && (*expression.cast::<pg_sys::Var>()).varattno == 2
            && (*expression.cast::<pg_sys::Var>()).vartype == pg_sys::INT4OID)
            .then_some((*expression.cast::<pg_sys::Var>()).varattno)
    }
}

fn first_pathkey_is_descending(pathkeys: *mut pg_sys::List) -> bool {
    let pathkey = unsafe { pg_sys::list_nth(pathkeys, 0) } as *mut pg_sys::PathKey;
    !pathkey.is_null()
        && unsafe { (*pathkey).pk_strategy == pg_sys::BTGreaterStrategyNumber as i32 }
}

fn runtime_filter_expression(
    expression: *mut pg_sys::Expr,
    scan_relid: pg_sys::Index,
) -> Option<*mut pg_sys::Expr> {
    if expression.is_null()
        || unsafe { (*expression).type_ } != pg_sys::NodeTag::T_OpExpr
    {
        return None;
    }
    let args = unsafe { (*expression.cast::<pg_sys::OpExpr>()).args };
    if args.is_null() || unsafe { pg_sys::list_length(args) } != 2 {
        return None;
    }
    let left = unsafe { pg_sys::list_nth(args, 0) } as *mut pg_sys::Expr;
    let right = unsafe { pg_sys::list_nth(args, 1) } as *mut pg_sys::Expr;
    match (
        is_scan_var(left, scan_relid),
        is_scan_var(right, scan_relid),
    ) {
        (true, false) => Some(right),
        (false, true) => Some(left),
        _ => None,
    }
}

fn is_scan_var(expression: *mut pg_sys::Expr, scan_relid: pg_sys::Index) -> bool {
    !expression.is_null()
        && unsafe { (*expression).type_ } == pg_sys::NodeTag::T_Var
        && unsafe { (*expression.cast::<pg_sys::Var>()).varno } == scan_relid as i32
}

fn runtime_filters(
    filter_specs: &[FilterSpec],
    expressions: RuntimeExpressionValues<'_>,
) -> Result<Vec<RuntimeFilter>, ForeignScanError> {
    filter_specs
        .iter()
        .map(|filter| {
            let value = expressions.get(filter.fdw_expr_index).ok_or_else(|| {
                ForeignScanError::unsupported(
                    "test FDW filter descriptor points outside fdw_exprs",
                )
            })?;
            let value = if value.is_null {
                None
            } else {
                Some(unsafe { i32::from_datum(value.datum, false) }.ok_or_else(
                    || {
                        ForeignScanError::unsupported(
                            "test FDW runtime filter was not int4",
                        )
                    },
                )?)
            };
            Ok(RuntimeFilter {
                attno: filter.attno,
                value,
            })
        })
        .collect()
}

fn matches_filters(filters: &[RuntimeFilter], row: &TestRow) -> bool {
    filters.iter().all(|filter| {
        filter
            .value
            .is_some_and(|value| row.int4_value(filter.attno) == Some(value))
    })
}

fn trace_filters(
    filters: &[RuntimeFilter],
) -> Vec<(pg_sys::AttrNumber, Option<i32>)> {
    filters
        .iter()
        .map(|filter| (filter.attno, filter.value))
        .collect()
}

fn projection_name(projection: &ScanProjection) -> &'static str {
    match projection {
        ScanProjection::Relation => "relation",
        ScanProjection::Projected { .. } => "projected",
        ScanProjection::SyntheticNull => "synthetic-null",
    }
}

fn read_filter_specs(
    reader: &mut ForeignPrivateReader<'_>,
) -> Result<Vec<FilterSpec>, ForeignScanError> {
    let mut filters_reader = reader.read_nested()?;
    let mut filters = Vec::with_capacity(filters_reader.remaining());
    while filters_reader.remaining() > 0 {
        let mut entry = filters_reader.read_nested()?;
        let attno =
            pg_sys::AttrNumber::try_from(entry.read_i32()?).map_err(|_| {
                ForeignScanError::unsupported(
                    "test FDW filter has an invalid attribute",
                )
            })?;
        let fdw_expr_index = entry.read_count()?;
        entry.finish()?;
        if !matches!(attno, 1 | 2) {
            return Err(ForeignScanError::unsupported(
                "test FDW filter has an unsupported attribute",
            ));
        }
        if filters
            .iter()
            .any(|filter: &FilterSpec| filter.fdw_expr_index == fdw_expr_index)
        {
            return Err(ForeignScanError::unsupported(
                "test FDW filter indexes are not unique",
            ));
        }
        filters.push(FilterSpec {
            attno,
            fdw_expr_index,
        });
    }
    filters_reader.finish()?;
    Ok(filters)
}

fn write_row(
    output: &mut ScanDatumWriter<'_, '_>,
    row: &TestRow,
    columns: &[ScanOutputColumn],
) -> Result<(), ForeignScanError> {
    for column in columns {
        let (datum, is_null) = match column.attno() {
            1 => (row.id.into_datum().expect("int4 datum"), false),
            2 => (row.sort_key.into_datum().expect("int4 datum"), false),
            3 => (
                row.payload.as_str().into_datum().expect("text datum"),
                false,
            ),
            _ => {
                return Err(ForeignScanError::unsupported(
                    "test FDW received an unexpected scan attribute",
                ));
            }
        };
        // SAFETY: every destination was bound from this scan's output layout at
        // provider start, and each Datum matches the corresponding test column.
        unsafe { output.write(*column, datum, is_null) };
    }
    Ok(())
}
