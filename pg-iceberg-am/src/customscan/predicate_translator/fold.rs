//! Predicate combination: left-associative AND/OR folding and binary-operator
//! mirroring for swapped (`literal op column`) operands.

use iceberg_lite::expr::{Predicate, PredicateOperator};

use super::error::IcebergTranslationError;

/// Left-associative fold of `items` with `combine`; `None` for empty input.
///
/// The kernel shared by [`fold_predicates`] (errors on empty) and the scan
/// provider's `combine_with_and` (treats empty as "no filter").
pub(crate) fn fold_left(
    items: Vec<Predicate>,
    combine: impl Fn(Predicate, Predicate) -> Predicate,
) -> Option<Predicate> {
    let mut iter = items.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, combine))
}

/// Left-associative fold of predicates with `Predicate::and` or `Predicate::or`.
pub(super) fn fold_predicates(
    items: Vec<Predicate>,
    and: bool,
) -> Result<Predicate, IcebergTranslationError> {
    let combine: fn(Predicate, Predicate) -> Predicate =
        if and { Predicate::and } else { Predicate::or };
    fold_left(items, combine).ok_or(IcebergTranslationError::EmptyBoolExpr)
}

/// Mirror a binary operator for `literal op column` → `column op literal`.
pub(super) fn mirror_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        _ => op,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_lite::expr::Reference;
    use iceberg_lite::spec::Datum;
    use proptest::prelude::*;

    #[test]
    fn mirror_operator_is_self_inverse_for_directional_ops() {
        for op in [
            PredicateOperator::LessThan,
            PredicateOperator::LessThanOrEq,
            PredicateOperator::GreaterThan,
            PredicateOperator::GreaterThanOrEq,
        ] {
            assert_eq!(mirror_operator(mirror_operator(op)), op);
        }
    }

    #[test]
    fn mirror_operator_is_identity_for_symmetric_ops() {
        for op in [
            PredicateOperator::Eq,
            PredicateOperator::NotEq,
            PredicateOperator::IsNull,
            PredicateOperator::NotNull,
        ] {
            assert_eq!(mirror_operator(op), op);
        }
    }

    #[test]
    fn mirror_operator_swaps_lt_and_gt() {
        assert_eq!(
            mirror_operator(PredicateOperator::LessThan),
            PredicateOperator::GreaterThan,
        );
        assert_eq!(
            mirror_operator(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThanOrEq,
        );
    }

    #[test]
    fn fold_predicates_handles_single_child() {
        let only = Reference::new("a").equal_to(Datum::int(1));
        let folded = fold_predicates(vec![only.clone()], true).unwrap();
        assert_eq!(folded, only);
    }

    #[test]
    fn fold_predicates_chains_and_left_assoc() {
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));
        let folded =
            fold_predicates(vec![a.clone(), b.clone(), c.clone()], true).unwrap();
        let expected = a.and(b).and(c);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_chains_or() {
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let folded = fold_predicates(vec![a.clone(), b.clone()], false).unwrap();
        let expected = a.or(b);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_rejects_empty_input() {
        assert!(matches!(
            fold_predicates(vec![], true),
            Err(IcebergTranslationError::EmptyBoolExpr),
        ));
    }

    fn arb_leaf_predicate() -> impl Strategy<Value = Predicate> {
        prop_oneof![
            Just(Predicate::AlwaysTrue),
            Just(Predicate::AlwaysFalse),
            any::<i32>().prop_map(|v| Reference::new("a").equal_to(Datum::int(v))),
            any::<i64>().prop_map(|v| Reference::new("b").less_than(Datum::long(v))),
            any::<i32>()
                .prop_map(|v| Reference::new("c").greater_than(Datum::int(v))),
        ]
    }

    fn arb_predicate_tree() -> impl Strategy<Value = Predicate> {
        arb_leaf_predicate().prop_recursive(4, 16, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(l, r)| l.and(r)),
                (inner.clone(), inner.clone()).prop_map(|(l, r)| l.or(r)),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn prop3_always_false_composes_through_and_or(x in arb_predicate_tree()) {
            prop_assert_eq!(
                Predicate::AlwaysFalse.and(x.clone()),
                Predicate::AlwaysFalse
            );
            prop_assert_eq!(
                x.clone().and(Predicate::AlwaysFalse),
                Predicate::AlwaysFalse
            );

            prop_assert_eq!(Predicate::AlwaysFalse.or(x.clone()), x.clone());
            prop_assert_eq!(x.clone().or(Predicate::AlwaysFalse), x.clone());
        }
    }
}
