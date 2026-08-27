use lagodb_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignPathBuilder, ForeignPathContext,
    ForeignPathKeys, ForeignPathSpec, ForeignPlanContext, ForeignPlanPrivate,
    ForeignPlanSpec, ForeignRelContext, ForeignRelSize, ForeignRelSizeContext,
    ForeignRowIdentityRequirement, ForeignScanError, PathVariantKind,
    ReScanForeignScanContext, ScanDatumWriter, ScanOutputColumn, ScanProjection,
    ScanProjectionPolicy, ScanSlotWriter, StartForeignScanContext,
};
use pgrx::IntoDatum;
use pgrx::pg_sys;
use std::cmp::Reverse;

use lagodb_core::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

use super::fixture::{TestRow, TestStore, TestTrace, TraceEvent};
use super::provider::FrameworkTestFdw;
use super::provider_filter::RuntimeFilter;

#[derive(Clone, Debug)]
pub struct ScanPrivate {
    ordered: bool,
    descending: bool,
}

impl ForeignPlanPrivate for ScanPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError> {
        writer
            .append_bool(self.ordered)
            .append_bool(self.descending);
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        Ok(Self {
            ordered: reader.read_bool()?,
            descending: reader.read_bool()?,
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
    filters: Vec<RuntimeFilter>,
    output_columns: Vec<ScanOutputColumn>,
    item_pointer_identity: bool,
}

impl ScanState {
    fn new(
        rows: Vec<TestRow>,
        ordered: bool,
        descending: bool,
        filters: Vec<RuntimeFilter>,
        output_columns: Vec<ScanOutputColumn>,
        item_pointer_identity: bool,
    ) -> Self {
        let mut state = Self {
            rows,
            cursor: 0,
            ordered,
            descending,
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
        ctx: &ForeignPlanContext<'_, Self>,
    ) -> Result<ForeignPlanSpec<Self::PrivateData>, ForeignScanError> {
        TestTrace::record(TraceEvent::PlanBuild {
            filters: ctx
                .filters()
                .iter()
                .map(|filter| {
                    (
                        filter.predicate().attno(),
                        filter.binding_range(),
                        filter.contract(),
                        filter.costing(),
                        filter.qual_location(),
                    )
                })
                .collect(),
            binding_count: ctx.filters().binding_count(),
            residual_count: ctx.filters().residual_count(),
            recheck_count: ctx.filters().recheck_count(),
        });
        let mut spec = ForeignPlanSpec::new(ctx.path_private().clone());
        spec.required_columns.require_all_columns();
        spec.projection_policy = if ctx.is_modify_target() {
            ScanProjectionPolicy::RequireRelationShape
        } else {
            ScanProjectionPolicy::AllowColumnPruning
        };
        Ok(spec)
    }

    fn begin(
        ctx: BeginForeignScanContext<'_, Self>,
    ) -> Result<Self::State, ForeignScanError> {
        let filters: Vec<RuntimeFilter> = ctx.filters.iter().copied().collect();
        let output_columns = ctx.output_layout.columns().to_vec();
        Ok(ScanState::new(
            TestStore::snapshot(ctx.relation.oid()),
            ctx.private_data.ordered,
            ctx.private_data.descending,
            filters,
            output_columns,
            matches!(
                ctx.row_identity_requirement,
                ForeignRowIdentityRequirement::ItemPointer
            ),
        ))
    }

    fn start(
        state: &mut Self::State,
        ctx: StartForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        let filters = ctx.filters.iter().copied().collect::<Vec<_>>();
        state.set_filters(filters);
        TestTrace::record(TraceEvent::ScanBegin {
            ordered: state.ordered,
            planned_count: state.filters.len(),
            filters: trace_filters(&state.filters),
            projection: projection_name(ctx.projection),
        });
        Ok(())
    }

    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        while let Some(row) = state.rows.get(state.cursor).cloned() {
            state.cursor += 1;
            if !state.filters.iter().all(|filter| filter.matches(&row)) {
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
        ctx: ReScanForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        let filters: Vec<RuntimeFilter> = ctx.filters.iter().copied().collect();
        state.set_filters(filters);
        state.rows = TestStore::snapshot(ctx.relation.oid());
        state.sort_rows();
        TestTrace::record(TraceEvent::ScanRescan {
            filters_changed: ctx.filters_changed,
            filters: trace_filters(&state.filters),
        });
        Ok(())
    }

    fn end(_state: &mut Self::State) -> Result<(), ForeignScanError> {
        Ok(())
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

fn trace_filters(
    filters: &[RuntimeFilter],
) -> Vec<(pg_sys::AttrNumber, Option<i32>)> {
    filters.iter().map(|filter| filter.trace()).collect()
}

fn projection_name(projection: &ScanProjection) -> &'static str {
    match projection {
        ScanProjection::Relation => "relation",
        ScanProjection::Projected { .. } => "projected",
        ScanProjection::SyntheticNull => "synthetic-null",
    }
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
