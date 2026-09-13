//! Continuation of a selected query-offload path through PostgreSQL upper rels.

use std::ffi::c_void;

use lagodb_core::expr::{
    ExprType, PgComparisonKind, PgComparisonSignature, RuntimeValueId,
    RuntimeValueSource, RuntimeValueSpec,
};
use lagodb_query::plan::{
    LimitEstimate, LimitNode, PlanCost, QueryFragment, QueryNode, QueryPlanData,
    QueryPlanError, SelectedQueryPlan, SortDirection, SortExpr, SortNode,
};
use pgrx::pg_sys;

use super::materialize;
use crate::query_host::{error::QueryHostError, methods};

/// Absorb only a PostgreSQL wrapper whose direct child is the already selected
/// LagoDB query path. PostgreSQL remains the authority for pathkeys, rows and
/// wrapper cost; this adapter only translates the proven operator semantics.
pub(super) struct UpperPlanCandidate;

/// A PostgreSQL path viewed through a projection that PostgreSQL has already
/// proven does not require a Result node.  Semantic wrappers such as Sort and
/// Limit are never skipped by this view.
#[derive(Clone, Copy)]
struct TransparentPath {
    path: *mut pg_sys::Path,
}

impl TransparentPath {
    unsafe fn inspect(path: *mut pg_sys::Path) -> Option<Self> {
        if unsafe { (*path).type_ } != pg_sys::NodeTag::T_ProjectionPath {
            return Some(Self { path });
        }
        let projection = path.cast::<pg_sys::ProjectionPath>();
        if !unsafe { (*projection).dummypp } {
            return None;
        }
        Some(Self {
            path: unsafe { (*projection).subpath },
        })
    }

    unsafe fn sort(self) -> Option<*mut pg_sys::SortPath> {
        (unsafe { (*self.path).type_ } == pg_sys::NodeTag::T_SortPath)
            .then_some(self.path.cast())
    }

    unsafe fn limit(self) -> Option<*mut pg_sys::LimitPath> {
        (unsafe { (*self.path).type_ } == pg_sys::NodeTag::T_LimitPath)
            .then_some(self.path.cast())
    }

    unsafe fn query_custom(self) -> Option<*mut pg_sys::CustomPath> {
        if unsafe { (*self.path).type_ } != pg_sys::NodeTag::T_CustomPath {
            return None;
        }
        let custom = self.path.cast::<pg_sys::CustomPath>();
        (unsafe { (*custom).methods } == methods::tables().path()).then_some(custom)
    }
}

struct LimitBindings {
    specs: Vec<RuntimeValueSpec>,
    expressions: Vec<*mut pg_sys::Expr>,
    offset: Option<usize>,
    count: Option<usize>,
}

impl UpperPlanCandidate {
    pub(super) unsafe fn install(
        root: *mut pg_sys::PlannerInfo,
        stage: pg_sys::UpperRelationKind::Type,
        output_rel: *mut pg_sys::RelOptInfo,
        extra: *mut c_void,
    ) -> Result<bool, QueryHostError> {
        match stage {
            pg_sys::UpperRelationKind::UPPERREL_ORDERED => unsafe {
                Self::install_sort(root, output_rel)
            },
            pg_sys::UpperRelationKind::UPPERREL_FINAL => unsafe {
                Self::install_limit(
                    output_rel,
                    extra.cast::<pg_sys::FinalPathExtraData>(),
                )
            },
            _ => Ok(false),
        }
    }

    unsafe fn install_sort(
        root: *mut pg_sys::PlannerInfo,
        output_rel: *mut pg_sys::RelOptInfo,
    ) -> Result<bool, QueryHostError> {
        let paths = unsafe { (*output_rel).pathlist };
        let count = unsafe { pg_sys::list_length(paths) };
        for index in 0..count {
            let exposed =
                unsafe { pg_sys::list_nth(paths, index) }.cast::<pg_sys::Path>();
            let Some(wrapper) = (unsafe {
                TransparentPath::inspect(exposed).and_then(|path| path.sort())
            }) else {
                continue;
            };
            let child = unsafe { (*wrapper).subpath };
            let Some(custom) = (unsafe {
                TransparentPath::inspect(child).and_then(|path| path.query_custom())
            }) else {
                continue;
            };
            let selected =
                unsafe { SelectedQueryPlan::decode_path(&*(*custom).custom_private) }
                    .map_err(QueryHostError::invalid_plan)?;
            let Some(keys) = (unsafe { Self::sort_keys(root, &selected) }) else {
                continue;
            };
            let (fragment, layout, runtime_values) =
                selected.query().clone().into_parts();
            let sort = SortNode::new(fragment.into_root(), keys.into_boxed_slice())
                .map_err(QueryHostError::invalid_plan)?;
            let query = QueryPlanData::new(
                QueryFragment::new(QueryNode::Sort(sort)),
                layout,
                runtime_values,
                selected.scans().len(),
            )
            .map_err(QueryHostError::invalid_plan)?;
            let encoded = unsafe { selected.encode_replacement_path(&query, &[]) }
                .map_err(QueryHostError::invalid_plan)?;
            let cost =
                PlanCost::try_new(unsafe { (*exposed).startup_cost }, unsafe {
                    (*exposed).total_cost
                })
                .map_err(QueryHostError::invalid_plan)?;
            unsafe { materialize::replace_path(output_rel, index, encoded, cost) };
            return Ok(true);
        }
        Ok(false)
    }

    unsafe fn install_limit(
        output_rel: *mut pg_sys::RelOptInfo,
        extra: *mut pg_sys::FinalPathExtraData,
    ) -> Result<bool, QueryHostError> {
        let paths = unsafe { (*output_rel).pathlist };
        let count = unsafe { pg_sys::list_length(paths) };
        for index in 0..count {
            let exposed =
                unsafe { pg_sys::list_nth(paths, index) }.cast::<pg_sys::Path>();
            let Some(limit_path) = (unsafe {
                TransparentPath::inspect(exposed).and_then(|path| path.limit())
            }) else {
                continue;
            };
            if unsafe { (*limit_path).limitOption }
                != pg_sys::LimitOption::LIMIT_OPTION_COUNT
            {
                // FETCH WITH TIES needs the peer ordering contract owned by
                // PostgreSQL's Limit node; an ordinary DataFusion limit is not
                // semantically interchangeable.
                continue;
            }
            let child = unsafe { (*limit_path).subpath };
            let Some(custom) = (unsafe {
                TransparentPath::inspect(child).and_then(|path| path.query_custom())
            }) else {
                continue;
            };
            let selected =
                unsafe { SelectedQueryPlan::decode_path(&*(*custom).custom_private) }
                    .map_err(QueryHostError::invalid_plan)?;
            let expressions = [
                unsafe { (*limit_path).limitOffset }.cast::<pg_sys::Expr>(),
                unsafe { (*limit_path).limitCount }.cast::<pg_sys::Expr>(),
            ];
            let Some(bindings) = (unsafe { Self::limit_bindings(expressions) })
            else {
                continue;
            };
            let mut query = selected.query().clone();
            let start = query
                .try_append_runtime_values(&bindings.specs)
                .ok_or_else(|| {
                    QueryHostError::invalid_plan(
                        "LIMIT/OFFSET runtime layout exceeds addressable memory",
                    )
                })?;
            let offset = bindings
                .offset
                .map(|position| RuntimeValueId::from_index(start + position));
            let count_value = bindings
                .count
                .map(|position| RuntimeValueId::from_index(start + position));
            let (fragment, layout, runtime_values) = query.into_parts();
            let estimate = unsafe {
                Self::limit_estimate(extra, (*child).rows, (*exposed).rows)
            }
            .map_err(QueryHostError::invalid_plan)?;
            let limit =
                LimitNode::new(fragment.into_root(), offset, count_value, estimate)
                    .map_err(QueryHostError::invalid_plan)?;
            let query = QueryPlanData::new(
                QueryFragment::new(QueryNode::Limit(limit)),
                layout,
                runtime_values,
                selected.scans().len(),
            )
            .map_err(QueryHostError::invalid_plan)?;
            let encoded = unsafe {
                selected.encode_replacement_path(&query, &bindings.expressions)
            }
            .map_err(QueryHostError::invalid_plan)?;
            let cost =
                PlanCost::try_new(unsafe { (*exposed).startup_cost }, unsafe {
                    (*exposed).total_cost
                })
                .map_err(QueryHostError::invalid_plan)?;
            unsafe { materialize::replace_path(output_rel, index, encoded, cost) };
            return Ok(true);
        }
        Ok(false)
    }

    unsafe fn limit_estimate(
        extra: *mut pg_sys::FinalPathExtraData,
        input_rows: f64,
        output_rows: f64,
    ) -> Result<LimitEstimate, QueryPlanError> {
        let offset_rows = match unsafe { (*extra).offset_est } {
            0 => 0.0,
            estimate if estimate > 0 => estimate as f64,
            _ => unsafe { pg_sys::clamp_row_est(input_rows * 0.10) },
        };
        let count_rows = match unsafe { (*extra).count_est } {
            0 => None,
            estimate if estimate > 0 => Some(estimate as f64),
            _ => Some(unsafe { pg_sys::clamp_row_est(input_rows * 0.10) }),
        };
        LimitEstimate::try_new(output_rows, offset_rows, count_rows)
    }

    unsafe fn sort_keys(
        root: *mut pg_sys::PlannerInfo,
        selected: &SelectedQueryPlan<'_>,
    ) -> Option<Vec<SortExpr>> {
        let parse = unsafe { &*(*root).parse };
        let clause_count = unsafe { pg_sys::list_length(parse.sortClause) };
        if clause_count == 0 {
            return None;
        }
        let mut keys = Vec::with_capacity(clause_count as usize);
        for clause_index in 0..clause_count {
            let clause = unsafe { pg_sys::list_nth(parse.sortClause, clause_index) }
                .cast::<pg_sys::SortGroupClause>();
            let target = unsafe {
                Self::target_for_sort_ref(parse.targetList, (*clause).tleSortGroupRef)
            }?;
            let position =
                (0..selected.query().tuple_layout().len()).find(|&position| {
                    let candidate = selected
                        .scan_target_expr(position)
                        .expect("selected path target layout is complete");
                    unsafe {
                        pg_sys::equal(
                            candidate.cast::<c_void>(),
                            (*target).expr.cast::<c_void>(),
                        )
                    }
                })?;
            let slot = selected.query().tuple_layout().slots()[position];
            let signature =
                PgComparisonSignature::for_operator(unsafe { (*clause).sortop })?;
            let direction = match signature.kind() {
                PgComparisonKind::Less => SortDirection::Ascending,
                PgComparisonKind::Greater => SortDirection::Descending,
                _ => return None,
            };
            if signature.left_type() != signature.right_type()
                || (signature.left_type() != slot.type_oid()
                    && !unsafe {
                        pg_sys::IsBinaryCoercible(
                            slot.type_oid(),
                            signature.left_type(),
                        )
                    })
            {
                return None;
            }
            keys.push(
                SortExpr::try_new(
                    slot.output(),
                    ExprType {
                        type_oid: slot.type_oid(),
                        typmod: slot.typmod(),
                        collation: slot.collation(),
                    },
                    direction,
                    unsafe { (*clause).nulls_first },
                )
                .ok()?,
            );
        }
        Some(keys)
    }

    unsafe fn target_for_sort_ref(
        target_list: *mut pg_sys::List,
        sort_ref: pg_sys::Index,
    ) -> Option<*mut pg_sys::TargetEntry> {
        let count = unsafe { pg_sys::list_length(target_list) };
        for index in 0..count {
            let target = unsafe { pg_sys::list_nth(target_list, index) }
                .cast::<pg_sys::TargetEntry>();
            if unsafe { (*target).ressortgroupref } == sort_ref {
                return Some(target);
            }
        }
        None
    }

    unsafe fn limit_bindings(
        expressions: [*mut pg_sys::Expr; 2],
    ) -> Option<LimitBindings> {
        let mut specs = Vec::with_capacity(2);
        let mut additional = Vec::with_capacity(2);
        let mut positions = [None, None];
        for (slot, expression) in expressions.into_iter().enumerate() {
            if expression.is_null() {
                continue;
            }
            let source_kind = match unsafe { (*expression).type_ } {
                pg_sys::NodeTag::T_Const => RuntimeValueSource::Constant,
                pg_sys::NodeTag::T_Param => {
                    match unsafe { (*expression.cast::<pg_sys::Param>()).paramkind } {
                        pg_sys::ParamKind::PARAM_EXTERN => {
                            RuntimeValueSource::ExternalParam
                        }
                        pg_sys::ParamKind::PARAM_EXEC => {
                            RuntimeValueSource::ExecParam
                        }
                        _ => return None,
                    }
                }
                _ => return None,
            };
            if unsafe { pg_sys::exprType(expression.cast()) } != pg_sys::INT8OID
                || unsafe { pg_sys::exprTypmod(expression.cast()) } != -1
                || unsafe { pg_sys::exprCollation(expression.cast()) }
                    != pg_sys::InvalidOid
            {
                return None;
            }
            positions[slot] = Some(specs.len());
            specs.push(RuntimeValueSpec {
                value_type: ExprType {
                    type_oid: pg_sys::INT8OID,
                    typmod: -1,
                    collation: pg_sys::InvalidOid,
                },
                source_kind,
            });
            additional.push(expression);
        }
        (!specs.is_empty()).then_some(LimitBindings {
            specs,
            expressions: additional,
            offset: positions[0],
            count: positions[1],
        })
    }
}
