//! PostgreSQL float comparisons as exact Arrow row filters.
//!
//! Arrow compares floats by IEEE totalOrder (`-NaN < -Infinity < … < -0 < 0 <
//! … < NaN`, equality by bits). Each comparison is rewritten to accept, row by
//! row, exactly the rows PostgreSQL accepts, as `lagodb-iceberg`'s
//! `predicate::semantics::float` does for Iceberg predicates:
//!
//! - PostgreSQL treats -0 and 0 as equal. A zero constant is given the sign
//!   that puts both zeros on the correct side, `= 0` tests both zeros and
//!   `<> 0` excludes both.
//! - PostgreSQL orders every NaN above every value. `>` and `>=` with a number
//!   also accept NaN, and `<` and `<=` reject it, since totalOrder puts a
//!   negative-sign NaN below every number.
//! - A double value against a real column is rounded to the neighbouring real
//!   on the side each operator keeps.
//!
//! Row-group statistics never prune these leaves: Parquet float statistics
//! do not order signed zeros or NaN as PostgreSQL does.

use super::bound::BoundNode;
use super::operator::ComparisonOperator;
use super::value::BoundValue;

/// A float comparison value after PostgreSQL's operator-family widening.
#[derive(Clone, Copy)]
pub(super) enum FloatComparison {
    F32(f32),
    F64(f64),
    /// A double compared with a real column. PostgreSQL's float48 operators
    /// widen the column value and compare in double precision.
    DoubleOnFloat(f64),
}

impl FloatComparison {
    /// NULL is handled by the caller; mirroring and negation are already
    /// applied, which is sound because PostgreSQL's float order is total.
    pub(super) fn bind(
        self,
        column: &str,
        operator: ComparisonOperator,
    ) -> BoundNode {
        match self {
            Self::F32(value) => {
                Self::bind_native(column, operator, FloatValue::F32(value))
            }
            Self::F64(value) => {
                Self::bind_native(column, operator, FloatValue::F64(value))
            }
            Self::DoubleOnFloat(value) if value.is_nan() => {
                Self::nan_comparison(column, operator)
            }
            Self::DoubleOnFloat(value) => {
                let nearest = value as f32;
                let widened = f64::from(nearest);
                // Reals are discrete, so with no real between the two
                // neighbours of `value`, `x < value` is `x < up` and
                // `x > value` is `x > down`. Rounding saturates to an
                // infinity, which keeps the same rules beyond real's range.
                let bound = if widened == value {
                    nearest
                } else {
                    match operator {
                        ComparisonOperator::Eq => return BoundNode::NeverTrue,
                        ComparisonOperator::NotEq => {
                            return BoundNode::IsNotNull(column.into());
                        }
                        ComparisonOperator::Lt | ComparisonOperator::Ge => {
                            if widened < value {
                                nearest.next_up()
                            } else {
                                nearest
                            }
                        }
                        ComparisonOperator::Le | ComparisonOperator::Gt => {
                            if widened > value {
                                nearest.next_down()
                            } else {
                                nearest
                            }
                        }
                    }
                };
                Self::bind_native(column, operator, FloatValue::F32(bound))
            }
        }
    }

    /// PostgreSQL orders NaN above every number and equal to itself.
    fn nan_comparison(column: &str, operator: ComparisonOperator) -> BoundNode {
        match operator {
            ComparisonOperator::Eq | ComparisonOperator::Ge => {
                BoundNode::IsNan(column.into())
            }
            ComparisonOperator::NotEq | ComparisonOperator::Lt => {
                BoundNode::IsNotNan(column.into())
            }
            ComparisonOperator::Le => BoundNode::IsNotNull(column.into()),
            ComparisonOperator::Gt => BoundNode::NeverTrue,
        }
    }

    fn bind_native(
        column: &str,
        operator: ComparisonOperator,
        value: FloatValue,
    ) -> BoundNode {
        if value.is_nan() {
            return Self::nan_comparison(column, operator);
        }
        let compare = |operator, negative_zero| BoundNode::Comparison {
            operator,
            column: column.into(),
            value: value.literal(negative_zero),
        };
        let or_nan = |node| {
            BoundNode::Or(
                vec![node, BoundNode::IsNan(column.into())].into_boxed_slice(),
            )
        };
        let and_not_nan = |node| {
            BoundNode::And(
                vec![node, BoundNode::IsNotNan(column.into())].into_boxed_slice(),
            )
        };
        match operator {
            ComparisonOperator::Eq if value.is_zero() => BoundNode::Or(
                vec![
                    compare(ComparisonOperator::Eq, true),
                    compare(ComparisonOperator::Eq, false),
                ]
                .into_boxed_slice(),
            ),
            ComparisonOperator::NotEq if value.is_zero() => BoundNode::And(
                vec![
                    compare(ComparisonOperator::NotEq, true),
                    compare(ComparisonOperator::NotEq, false),
                ]
                .into_boxed_slice(),
            ),
            ComparisonOperator::Eq | ComparisonOperator::NotEq => {
                compare(operator, false)
            }
            ComparisonOperator::Lt => and_not_nan(compare(operator, true)),
            ComparisonOperator::Le => and_not_nan(compare(operator, false)),
            ComparisonOperator::Gt => or_nan(compare(operator, false)),
            ComparisonOperator::Ge => or_nan(compare(operator, true)),
        }
    }
}

#[derive(Clone, Copy)]
enum FloatValue {
    F32(f32),
    F64(f64),
}

impl FloatValue {
    fn is_nan(self) -> bool {
        match self {
            Self::F32(value) => value.is_nan(),
            Self::F64(value) => value.is_nan(),
        }
    }

    fn is_zero(self) -> bool {
        match self {
            Self::F32(value) => value == 0.0,
            Self::F64(value) => value == 0.0,
        }
    }

    /// The literal, with a zero given the requested sign.
    fn literal(self, negative_zero: bool) -> BoundValue {
        let zero = if negative_zero { -0.0 } else { 0.0 };
        match self {
            Self::F32(value) => {
                BoundValue::F32(if value == 0.0 { zero as f32 } else { value })
            }
            Self::F64(value) => {
                BoundValue::F64(if value == 0.0 { zero } else { value })
            }
        }
    }
}
