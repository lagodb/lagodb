//! Runtime predicate trait adapter.

use iceberg_lite::expr::Predicate;
use lagodb_core::expr::pushdown::{PredicatePlan, PredicatePlanner};
use lagodb_core::runtime_api::{RuntimeComparisonOperator, RuntimePredicateScalar};

use crate::engine::predicate::policy::PredicatePushdownPolicy;
use crate::error::{IcebergError, IcebergResult};

use super::IcebergPredicatePlanner;

impl<'predicate>
    PredicatePlanner<RuntimePredicateScalar<'predicate>, RuntimeComparisonOperator>
    for IcebergPredicatePlanner<'_>
{
    type Predicate = Predicate;
    type Error = IcebergError;

    fn always_true(&self) -> IcebergResult<PredicatePlan<Predicate>> {
        Ok(PredicatePlan::Exact(Predicate::AlwaysTrue))
    }

    fn always_false(&self) -> IcebergResult<PredicatePlan<Predicate>> {
        Ok(PredicatePlan::Exact(Predicate::AlwaysFalse))
    }

    fn strict_true(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(column_kind) = self.column_kind(*column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicatePushdownPolicy::supports_null_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(
            self.reference(*column)?.null_test(true),
        ))
    }

    fn strict_false(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        self.field_ids
            .get(*column)
            .ok_or(IcebergError::InvariantViolated(
                "table-scan predicate column exceeds the bound projection",
            ))?;
        self.arrow_schema.fields().get(*column).ok_or(
            IcebergError::InvariantViolated(
                "table-scan predicate column exceeds the bound Arrow schema",
            ),
        )?;
        Ok(PredicatePlan::ExactNoComplement(Predicate::AlwaysFalse))
    }

    fn comparison(
        &self,
        operator: &RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'predicate>,
        right: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        IcebergPredicatePlanner::comparison(self, *operator, left, right)
    }

    fn is_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.null_test(value, false)
    }

    fn is_not_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.null_test(value, true)
    }

    fn is_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.nan_test(value, false)
    }

    fn is_not_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.nan_test(value, true)
    }

    fn starts_with(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
        prefix: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        IcebergPredicatePlanner::starts_with(self, value, prefix)
    }

    fn conjunction(&self, predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::and)
            .expect("predicate negotiation accepted a conjunction child")
    }

    fn disjunction(&self, predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::or)
            .expect("predicate negotiation accepted a disjunction child")
    }

    fn negate(&self, predicate: Predicate) -> Predicate {
        !predicate
    }
}
