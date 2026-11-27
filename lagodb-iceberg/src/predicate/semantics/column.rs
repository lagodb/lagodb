//! Field-id-referenced Iceberg leaf construction shared by predicate adapters.

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use iceberg_lite::spec::Datum;
use lagodb_core::expr::{BooleanTestKind, PgComparisonKind, PgNanComparison};

#[derive(Clone, Copy)]
pub(in crate::predicate) struct IcebergColumnRef<'a> {
    display_name: &'a str,
    field_id: i32,
}

impl<'a> IcebergColumnRef<'a> {
    #[inline]
    pub(in crate::predicate) const fn new(
        display_name: &'a str,
        field_id: i32,
    ) -> Self {
        Self {
            display_name,
            field_id,
        }
    }

    #[inline]
    const fn comparison_operator(operator: PgComparisonKind) -> PredicateOperator {
        match operator {
            PgComparisonKind::Equal => PredicateOperator::Eq,
            PgComparisonKind::NotEqual => PredicateOperator::NotEq,
            PgComparisonKind::Less => PredicateOperator::LessThan,
            PgComparisonKind::LessEqual => PredicateOperator::LessThanOrEq,
            PgComparisonKind::Greater => PredicateOperator::GreaterThan,
            PgComparisonKind::GreaterEqual => PredicateOperator::GreaterThanOrEq,
        }
    }

    #[inline]
    fn reference(self) -> Reference {
        Reference::new_bound_field(self.display_name, self.field_id)
    }

    pub(super) fn binary_predicate(
        self,
        operator: PredicateOperator,
        value: Datum,
    ) -> Predicate {
        Predicate::Binary(BinaryExpression::new(operator, self.reference(), value))
    }

    pub(in crate::predicate) fn pg_comparison(
        self,
        operator: PgComparisonKind,
        value: Datum,
    ) -> Predicate {
        self.binary_predicate(Self::comparison_operator(operator), value)
    }

    pub(in crate::predicate) fn null_test(self, is_not_null: bool) -> Predicate {
        Predicate::Unary(UnaryExpression::new(
            if is_not_null {
                PredicateOperator::NotNull
            } else {
                PredicateOperator::IsNull
            },
            self.reference(),
        ))
    }

    pub(in crate::predicate) fn nan_test(self, is_not_nan: bool) -> Predicate {
        let predicate = Predicate::Unary(UnaryExpression::new(
            if is_not_nan {
                PredicateOperator::NotNan
            } else {
                PredicateOperator::IsNan
            },
            self.reference(),
        ));
        if is_not_nan {
            Predicate::and(self.null_test(true), predicate)
        } else {
            predicate
        }
    }

    /// BooleanTest is two-valued in SQL; unlike a strict comparison, its
    /// NOT TRUE / NOT FALSE cases include a NULL input.
    pub(in crate::predicate) fn boolean_test(
        self,
        kind: BooleanTestKind,
    ) -> Predicate {
        match kind {
            BooleanTestKind::IsTrue | BooleanTestKind::IsFalse => self
                .binary_predicate(
                    PredicateOperator::Eq,
                    Datum::bool(kind == BooleanTestKind::IsTrue),
                ),
            BooleanTestKind::IsNotTrue | BooleanTestKind::IsNotFalse => {
                self.null_test(false).or(self.binary_predicate(
                    PredicateOperator::Eq,
                    Datum::bool(kind == BooleanTestKind::IsNotFalse),
                ))
            }
            BooleanTestKind::IsUnknown => self.null_test(false),
            BooleanTestKind::IsNotUnknown => self.null_test(true),
        }
    }

    /// PostgreSQL's comparison with a NaN value, which it orders above every
    /// number.
    pub(super) fn nan_comparison(self, comparison: PgNanComparison) -> Predicate {
        match comparison {
            PgNanComparison::IsNan => self.nan_test(false),
            PgNanComparison::IsNotNan => self.nan_test(true),
            PgNanComparison::StrictTrue => self.null_test(true),
            PgNanComparison::StrictFalse => Predicate::AlwaysFalse,
        }
    }

    pub(in crate::predicate) fn starts_with(self, prefix: Datum) -> Predicate {
        self.binary_predicate(PredicateOperator::StartsWith, prefix)
    }

    pub(in crate::predicate) fn in_set(self, literals: Vec<Datum>) -> Predicate {
        self.reference().is_in(literals)
    }
}
