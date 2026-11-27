//! PostgreSQL float comparisons as exact Iceberg predicates.
//!
//! A bound predicate prunes files and row groups and is the reader's Arrow
//! row filter; PostgreSQL does not recheck it. The Arrow kernels compare
//! floats by IEEE totalOrder (`-NaN < -Infinity < … < -0 < 0 < … < NaN`,
//! equality by bits), and Iceberg bounds leave NaN out. Each comparison is
//! rewritten to accept, row by row, exactly the rows PostgreSQL accepts:
//!
//! - PostgreSQL treats -0 and 0 as equal. A zero constant is given the sign
//!   that puts both zeros on the correct side, `= 0` tests both zeros and
//!   `<> 0` excludes both.
//! - PostgreSQL orders every NaN above every value. `>` and `>=` with a number
//!   also accept `isnan`, and `<` and `<=` reject it, since totalOrder puts a
//!   negative-sign NaN below every number. `<>` needs neither: every NaN
//!   differs from a number, and no iceberg-lite evaluator prunes `<>` by
//!   bounds.
//! - A double value against a real column is rounded to the neighbouring real
//!   on the side each operator keeps ([`FloatComparison::DoubleOnFloat`]).
//!
//! Exactness rests on iceberg-lite's reader. An upstream iceberg-rust defect
//! breaks it for every promoted type, not only floats, and is not worked
//! around here: a column promoted from real to double is filtered on older
//! files with the double literal rounded to real (see
//! `IcebergBoundPredicate::conjoin_row_filter`). The reader's missing-column
//! `<` / `<=` defect does not reach these predicates: the not-NaN test they
//! carry includes a NotNull guard, false for a missing column (see
//! `PredicateCapabilities::comparison_capability`).

use iceberg_lite::expr::{Predicate, PredicateOperator};
use iceberg_lite::spec::Datum;
use lagodb_core::expr::PgComparisonKind;

use super::column::IcebergColumnRef;

/// A float value compared with a float column.
pub(in crate::predicate) enum FloatComparison {
    /// A value at the Iceberg column's own width.
    Native(FloatValue),
    /// A double compared with a real column. PostgreSQL's float48 operators
    /// widen the column value and compare in double precision, so a double
    /// no real represents is rounded toward the side each operator keeps.
    DoubleOnFloat(f64),
}

/// A float literal at the Iceberg column's width.
#[derive(Clone, Copy)]
pub(in crate::predicate) enum FloatValue {
    Float(f32),
    Double(f64),
}

impl FloatComparison {
    /// Whether binding a non-NaN value with `operator`, after mirroring and
    /// negation, adds an `OR isnan` branch to the comparison.
    pub(in crate::predicate) const fn may_add_nan_disjunction(
        operator: PgComparisonKind,
    ) -> bool {
        matches!(
            operator,
            PgComparisonKind::Greater | PgComparisonKind::GreaterEqual
        )
    }

    /// NULL is handled by the caller, and mirroring and negation are already
    /// applied to `operator`: PostgreSQL's float order is total, so its
    /// negated operator keeps the same truth set.
    pub(in crate::predicate) fn bind(
        self,
        column: IcebergColumnRef<'_>,
        operator: PgComparisonKind,
    ) -> Predicate {
        match self {
            Self::Native(value) => value.bind(column, operator),
            Self::DoubleOnFloat(value) if value.is_nan() => {
                column.nan_comparison(operator.with_nan_on(false))
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
                        // No real equals `value`, and neither does NaN.
                        PgComparisonKind::Equal => {
                            return Predicate::AlwaysFalse;
                        }
                        // Every non-NULL real, NaN included, differs from it.
                        PgComparisonKind::NotEqual => {
                            return column.null_test(true);
                        }
                        PgComparisonKind::Less | PgComparisonKind::GreaterEqual => {
                            if widened < value {
                                nearest.next_up()
                            } else {
                                nearest
                            }
                        }
                        PgComparisonKind::LessEqual | PgComparisonKind::Greater => {
                            if widened > value {
                                nearest.next_down()
                            } else {
                                nearest
                            }
                        }
                    }
                };
                FloatValue::Float(bound).bind(column, operator)
            }
        }
    }
}

impl FloatValue {
    fn bind(
        self,
        column: IcebergColumnRef<'_>,
        operator: PgComparisonKind,
    ) -> Predicate {
        if self.is_nan() {
            return column.nan_comparison(operator.with_nan_on(false));
        }
        let compare = |operator: PredicateOperator, negative_zero: bool| {
            column.binary_predicate(operator, self.datum(negative_zero))
        };
        let or_nan = |predicate: Predicate| predicate.or(column.nan_test(false));
        let and_not_nan = |predicate: Predicate| predicate.and(column.nan_test(true));
        match operator {
            PgComparisonKind::Equal if self.is_zero() => {
                compare(PredicateOperator::Eq, true)
                    .or(compare(PredicateOperator::Eq, false))
            }
            PgComparisonKind::Equal => compare(PredicateOperator::Eq, false),
            PgComparisonKind::NotEqual if self.is_zero() => {
                compare(PredicateOperator::NotEq, true)
                    .and(compare(PredicateOperator::NotEq, false))
            }
            PgComparisonKind::NotEqual => compare(PredicateOperator::NotEq, false),
            PgComparisonKind::Less => {
                and_not_nan(compare(PredicateOperator::LessThan, true))
            }
            PgComparisonKind::LessEqual => {
                and_not_nan(compare(PredicateOperator::LessThanOrEq, false))
            }
            PgComparisonKind::Greater => {
                or_nan(compare(PredicateOperator::GreaterThan, false))
            }
            PgComparisonKind::GreaterEqual => {
                or_nan(compare(PredicateOperator::GreaterThanOrEq, true))
            }
        }
    }

    fn is_nan(self) -> bool {
        match self {
            Self::Float(value) => value.is_nan(),
            Self::Double(value) => value.is_nan(),
        }
    }

    fn is_zero(self) -> bool {
        match self {
            Self::Float(value) => value == 0.0,
            Self::Double(value) => value == 0.0,
        }
    }

    /// The literal, with a zero given the requested sign.
    fn datum(self, negative_zero: bool) -> Datum {
        let zero = if negative_zero { -0.0 } else { 0.0 };
        match self {
            Self::Float(value) => {
                Datum::float(if value == 0.0 { zero as f32 } else { value })
            }
            Self::Double(value) => {
                Datum::double(if value == 0.0 { zero } else { value })
            }
        }
    }
}
