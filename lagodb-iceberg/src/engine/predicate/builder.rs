//! Schema-bound Iceberg leaf construction shared by predicate adapters.

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use iceberg_lite::spec::Datum;

pub(crate) struct IcebergPredicateBuilder {
    reference: Reference,
}

impl IcebergPredicateBuilder {
    pub(crate) fn new(name: String, field_id: i32) -> Self {
        Self {
            reference: Reference::new_bound_field(name, field_id),
        }
    }

    pub(crate) fn comparison(
        self,
        operator: PredicateOperator,
        value: Datum,
    ) -> Predicate {
        Predicate::Binary(BinaryExpression::new(operator, self.reference, value))
    }

    pub(crate) fn null_test(self, is_not_null: bool) -> Predicate {
        Predicate::Unary(UnaryExpression::new(
            if is_not_null {
                PredicateOperator::NotNull
            } else {
                PredicateOperator::IsNull
            },
            self.reference,
        ))
    }

    pub(crate) fn nan_test(self, is_not_nan: bool) -> Predicate {
        let predicate = Predicate::Unary(UnaryExpression::new(
            if is_not_nan {
                PredicateOperator::NotNan
            } else {
                PredicateOperator::IsNan
            },
            self.reference.clone(),
        ));
        if is_not_nan {
            Predicate::and(
                Predicate::Unary(UnaryExpression::new(
                    PredicateOperator::NotNull,
                    self.reference,
                )),
                predicate,
            )
        } else {
            predicate
        }
    }

    pub(crate) fn starts_with(self, prefix: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            self.reference,
            prefix,
        ))
    }
}
