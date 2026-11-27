//! Runtime value and `IN`-set tree binder for planned Iceberg predicates.

use iceberg_lite::expr::Predicate;
use lagodb_core::datum::ArrayDatumLayout;
use lagodb_core::expr::pushdown::{FilterBindResult, PredicatePlan};
use lagodb_core::expr::{PgComparisonKind, RuntimeValue, RuntimeValueBindings};

use crate::predicate::error::IcebergFilterError;
use crate::predicate::ir::{PlannedColumn, PlannedNode, PlannedValueType};
use crate::predicate::semantics::IcebergColumnRef;

use super::datum::DecodedValue;
use super::{IcebergBoundPredicate, RowFilter};

struct BoundNode {
    pruning: Predicate,
    row_filter: RowFilter,
}

impl BoundNode {
    #[inline]
    const fn whole(pruning: Predicate) -> Self {
        Self {
            pruning,
            row_filter: RowFilter::Whole,
        }
    }
}

#[derive(Clone, Copy)]
enum LogicalKind {
    And,
    Or,
}

pub(super) struct PredicateBinder<'a> {
    values: RuntimeValueBindings<'a>,
}

impl<'a> PredicateBinder<'a> {
    pub(super) const fn new(values: RuntimeValueBindings<'a>) -> Self {
        Self { values }
    }

    pub(super) fn bind(
        &self,
        schema_id: i32,
        root: &PlannedNode,
    ) -> Result<FilterBindResult<IcebergBoundPredicate>, IcebergFilterError> {
        let Some(bound) = self.bind_node(root, false)? else {
            return Ok(FilterBindResult::ValueNotRepresentable);
        };
        Ok(FilterBindResult::Bound(IcebergBoundPredicate::new(
            schema_id,
            bound.pruning,
            bound.row_filter,
        )))
    }

    fn bind_node(
        &self,
        node: &PlannedNode,
        negated: bool,
    ) -> Result<Option<BoundNode>, IcebergFilterError> {
        match node {
            PlannedNode::AlwaysTrue => Ok(Some(BoundNode::whole(if negated {
                Predicate::AlwaysFalse
            } else {
                Predicate::AlwaysTrue
            }))),
            PlannedNode::AlwaysFalse => Ok(Some(BoundNode::whole(if negated {
                Predicate::AlwaysTrue
            } else {
                Predicate::AlwaysFalse
            }))),
            PlannedNode::Comparison {
                operator,
                column,
                value,
                value_type,
            } => self
                .bind_comparison(
                    *operator,
                    column,
                    *value_type,
                    self.values.value(*value),
                    negated,
                )
                .map(|predicate| predicate.map(BoundNode::whole)),
            PlannedNode::BooleanTest { kind, column } => {
                let kind = if negated { kind.negated() } else { *kind };
                Ok(Some(BoundNode::whole(
                    Self::column_ref(column).boolean_test(kind),
                )))
            }
            PlannedNode::IsNull(column) => {
                Ok(Some(BoundNode::whole(Self::null_test(column, negated))))
            }
            PlannedNode::IsNotNull(column) => {
                Ok(Some(BoundNode::whole(Self::null_test(column, !negated))))
            }
            PlannedNode::IsNan(column) => {
                Ok(Some(BoundNode::whole(Self::nan_test(column, negated))))
            }
            PlannedNode::IsNotNan(column) => {
                Ok(Some(BoundNode::whole(Self::nan_test(column, !negated))))
            }
            // Negation refuses ExactNoComplement plans, so `negated` is false.
            PlannedNode::StartsWith { column, prefix } => {
                debug_assert!(!negated);
                self.bind_starts_with(column, self.values.value(*prefix))
                    .map(|predicate| predicate.map(BoundNode::whole))
            }
            // Negation refuses Conservative plans, so `negated` is false.
            PlannedNode::InSet {
                column,
                set,
                value_type,
            } => {
                debug_assert!(!negated);
                let pruning =
                    Self::bind_in_set(column, *value_type, self.values.value(*set))?;
                Ok(Some(BoundNode {
                    pruning,
                    row_filter: RowFilter::Skipped,
                }))
            }
            PlannedNode::And(children) => self.bind_logical(
                children,
                negated,
                if negated {
                    LogicalKind::Or
                } else {
                    LogicalKind::And
                },
            ),
            PlannedNode::Or(children) => self.bind_logical(
                children,
                negated,
                if negated {
                    LogicalKind::And
                } else {
                    LogicalKind::Or
                },
            ),
            PlannedNode::Not(child) => self.bind_node(child, !negated),
        }
    }

    fn bind_starts_with(
        &self,
        column: &PlannedColumn,
        value: RuntimeValue,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        if value.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
        }
        let Some(DecodedValue::Datum(prefix)) =
            (unsafe { PlannedValueType::String.decode_datum(value) })?
        else {
            return Ok(None);
        };
        Ok(Some(Self::column_ref(column).starts_with(prefix)))
    }

    fn bind_comparison(
        &self,
        operator: PgComparisonKind,
        column: &PlannedColumn,
        value_type: PlannedValueType,
        value: RuntimeValue,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        // A strict SQL comparison with NULL is UNKNOWN. Its truth set remains
        // empty under NOT, so folding must happen after carrying negation to
        // the leaf rather than by negating `AlwaysFalse` at a parent node.
        if value.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
        }
        let Some(value) = (unsafe { value_type.decode_datum(value) })? else {
            return Ok(None);
        };
        let operator = if negated {
            operator.negated()
        } else {
            operator
        };
        let predicate = match value {
            DecodedValue::Timestamp(value) => {
                return Ok(value.bind(Self::column_ref(column), operator));
            }
            DecodedValue::Float(value) => {
                value.bind(Self::column_ref(column), operator)
            }
            DecodedValue::Datum(datum) => {
                Self::column_ref(column).pg_comparison(operator, datum)
            }
            DecodedValue::OutsideFinite(value) => {
                if value
                    .matches_finite_column(operator)
                    .expect("outside-finite Decimal128 value")
                {
                    Self::null_test(column, true)
                } else {
                    Predicate::AlwaysFalse
                }
            }
            DecodedValue::OutsideInteger(value) => {
                if value.comparison_matches_non_null(operator) {
                    Self::null_test(column, true)
                } else {
                    Predicate::AlwaysFalse
                }
            }
            // Admission allows only `=` / `<>` for `character(n)`: no stored
            // value equals one wider than the column.
            DecodedValue::OutsideWidth => {
                if matches!(operator, PgComparisonKind::NotEqual) {
                    Self::null_test(column, true)
                } else {
                    Predicate::AlwaysFalse
                }
            }
        };
        Ok(Some(predicate))
    }

    fn bind_in_set(
        column: &PlannedColumn,
        value_type: PlannedValueType,
        set: RuntimeValue,
    ) -> Result<Predicate, IcebergFilterError> {
        // `x = ANY (NULL)` is UNKNOWN for every row.
        if set.is_null() {
            return Ok(Predicate::AlwaysFalse);
        }
        let element = set
            .metadata()
            .array_element()
            .expect("planner admitted an array-valued set");
        // SAFETY: the planner admitted an array of this element type, and the
        // bound value outlives this call.
        let elements = unsafe {
            ArrayDatumLayout::bind(element.value_type.type_oid).values(set.datum())
        };
        let mut literals = Vec::with_capacity(elements.len());
        // A NULL element only turns a non-match into UNKNOWN, and Conservative
        // plans are never negated.
        for datum in elements.flatten() {
            // SAFETY: the iterator retains the array for this loop, and decoding
            // copies the element.
            let value = unsafe { RuntimeValue::from_raw(datum, false, element) };
            match unsafe { value_type.decode_datum(value) }? {
                Some(DecodedValue::Datum(literal)) => literals.push(literal),
                // A value outside the column's domain matches no row.
                Some(
                    DecodedValue::OutsideInteger(_) | DecodedValue::OutsideWidth,
                ) => {}
                // One unrepresentable element leaves the set to the residual.
                // The widest answer keeps the filter's other predicates bound;
                // it is safe because Conservative plans are never negated.
                _ => return Ok(Predicate::AlwaysTrue),
            }
        }
        Ok(Self::column_ref(column).in_set(literals))
    }

    fn bind_logical(
        &self,
        children: &[PlannedNode],
        negated: bool,
        kind: LogicalKind,
    ) -> Result<Option<BoundNode>, IcebergFilterError> {
        let combine: fn(Predicate, Predicate) -> Predicate = match kind {
            LogicalKind::And => Predicate::and,
            LogicalKind::Or => Predicate::or,
        };
        let mut pruning: Option<Predicate> = None;
        let mut row_plans: Option<Vec<PredicatePlan<Predicate>>> = None;

        for child in children {
            let Some(bound) = self.bind_node(child, negated)? else {
                return Ok(None);
            };
            match (&mut row_plans, bound.row_filter) {
                (None, RowFilter::Whole) => {}
                (Some(plans), row_filter) => {
                    plans.push(row_filter.into_plan(&bound.pruning));
                }
                (None, row_filter) => {
                    let mut plans = Vec::with_capacity(children.len());
                    if let Some(prior) = &pruning {
                        plans.push(PredicatePlan::Exact(prior.clone()));
                    }
                    plans.push(row_filter.into_plan(&bound.pruning));
                    row_plans = Some(plans);
                }
            }
            pruning = Some(match pruning {
                Some(left) => combine(left, bound.pruning),
                None => bound.pruning,
            });
        }

        let pruning = pruning.expect("logical filter nodes are non-empty");
        let row_filter = match row_plans {
            None => RowFilter::Whole,
            Some(plans) => {
                let plan = match kind {
                    LogicalKind::And => PredicatePlan::conjunction(plans, |items| {
                        Self::reduce(items, Predicate::and)
                    }),
                    LogicalKind::Or => PredicatePlan::disjunction(plans, |items| {
                        Self::reduce(items, Predicate::or)
                    }),
                };
                plan.into_predicate()
                    .map_or(RowFilter::Skipped, RowFilter::Projected)
            }
        };
        Ok(Some(BoundNode {
            pruning,
            row_filter,
        }))
    }

    fn reduce(
        predicates: Vec<Predicate>,
        combine: fn(Predicate, Predicate) -> Predicate,
    ) -> Predicate {
        predicates
            .into_iter()
            .reduce(combine)
            .expect("the shared tree rules combine at least one predicate")
    }

    fn null_test(column: &PlannedColumn, is_not_null: bool) -> Predicate {
        Self::column_ref(column).null_test(is_not_null)
    }

    fn nan_test(column: &PlannedColumn, is_not_nan: bool) -> Predicate {
        Self::column_ref(column).nan_test(is_not_nan)
    }

    #[inline]
    fn column_ref(column: &PlannedColumn) -> IcebergColumnRef<'_> {
        IcebergColumnRef::new(&column.display_name, column.field_id)
    }
}
