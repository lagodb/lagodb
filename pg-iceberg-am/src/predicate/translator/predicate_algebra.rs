//! Predicate-tree assembly and operator algebra for the Iceberg translator.

use iceberg_lite::expr::{Predicate, PredicateOperator};

use super::{IcebergPredicateTranslator, IcebergTranslationError};

impl IcebergPredicateTranslator {
    /// Combine translated predicates as a left-associative conjunction.
    /// Empty input means that no provider filter is required.
    pub(crate) fn conjoin(items: Vec<Predicate>) -> Option<Predicate> {
        Self::fold(items, Predicate::and)
    }

    fn fold(
        items: Vec<Predicate>,
        combine: impl Fn(Predicate, Predicate) -> Predicate,
    ) -> Option<Predicate> {
        let mut iter = items.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, combine))
    }

    /// Mirror a binary operator for the `literal op column` operand order so it
    /// reads as `column op literal` (e.g. `7 < col` → `col > 7`).
    ///
    /// Translator-only assembly step used by [`Self::comparison`] when
    /// `swap_sides` holds: self-inverse on directional ops, identity on
    /// symmetric ones.
    pub(super) fn mirror_operator(&self, op: PredicateOperator) -> PredicateOperator {
        match op {
            PredicateOperator::LessThan => PredicateOperator::GreaterThan,
            PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
            PredicateOperator::GreaterThan => PredicateOperator::LessThan,
            PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
            _ => op,
        }
    }

    /// Left-associative fold of predicates with `Predicate::and` or
    /// `Predicate::or`, erroring on an empty children list.
    ///
    /// The translator owns the `and` / `or` empty-list semantics through
    /// [`IcebergTranslationError::EmptyBoolExpr`].
    pub(super) fn fold_predicates(
        &self,
        items: Vec<Predicate>,
        and: bool,
    ) -> Result<Predicate, IcebergTranslationError> {
        let combine: fn(Predicate, Predicate) -> Predicate =
            if and { Predicate::and } else { Predicate::or };
        Self::fold(items, combine).ok_or(IcebergTranslationError::EmptyBoolExpr)
    }
}

#[cfg(test)]
mod tests {
    use iceberg_lite::expr::{Predicate, PredicateOperator, Reference};
    use iceberg_lite::spec::Datum;
    use proptest::prelude::*;

    use super::{IcebergPredicateTranslator, IcebergTranslationError};

    #[test]
    fn mirror_operator_is_self_inverse_for_directional_ops() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        for op in [
            PredicateOperator::LessThan,
            PredicateOperator::LessThanOrEq,
            PredicateOperator::GreaterThan,
            PredicateOperator::GreaterThanOrEq,
        ] {
            assert_eq!(t.mirror_operator(t.mirror_operator(op)), op);
        }
    }

    #[test]
    fn mirror_operator_is_identity_for_symmetric_ops() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        for op in [
            PredicateOperator::Eq,
            PredicateOperator::NotEq,
            PredicateOperator::IsNull,
            PredicateOperator::NotNull,
        ] {
            assert_eq!(t.mirror_operator(op), op);
        }
    }

    #[test]
    fn mirror_operator_swaps_lt_and_gt() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        assert_eq!(
            t.mirror_operator(PredicateOperator::LessThan),
            PredicateOperator::GreaterThan,
        );
        assert_eq!(
            t.mirror_operator(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThanOrEq,
        );
    }

    #[test]
    fn fold_predicates_handles_single_child() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        let only = Reference::new("a").equal_to(Datum::int(1));
        let folded = t.fold_predicates(vec![only.clone()], true).unwrap();
        assert_eq!(folded, only);
    }

    #[test]
    fn fold_predicates_chains_and_left_assoc() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));
        let folded = t
            .fold_predicates(vec![a.clone(), b.clone(), c.clone()], true)
            .unwrap();
        let expected = a.and(b).and(c);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_chains_or() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let folded = t
            .fold_predicates(vec![a.clone(), b.clone()], false)
            .unwrap();
        let expected = a.or(b);
        assert_eq!(folded, expected);
    }

    #[test]
    fn fold_predicates_rejects_empty_input() {
        let t = IcebergPredicateTranslator::new_unbound_for_tests();
        assert!(matches!(
            t.fold_predicates(vec![], true),
            Err(IcebergTranslationError::EmptyBoolExpr),
        ));
    }

    #[test]
    fn conjoin_empty_means_no_provider_filter() {
        assert_eq!(IcebergPredicateTranslator::conjoin(Vec::new()), None);
    }

    #[test]
    fn conjoin_multiple_predicates_is_left_associative() {
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));

        assert_eq!(
            IcebergPredicateTranslator::conjoin(vec![
                a.clone(),
                b.clone(),
                c.clone()
            ]),
            Some(a.and(b).and(c)),
        );
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
        fn prop_always_false_composes_through_and_or(x in arb_predicate_tree()) {
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
