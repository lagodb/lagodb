//! Bind an array-valued set into one Iceberg `IN` predicate, and project sets
//! out of the predicate the Arrow reader evaluates.

use iceberg_lite::expr::Predicate;
use lagodb_core::datum::ArrayDatumLayout;
use lagodb_core::expr::RuntimeValue;
use lagodb_core::expr::pushdown::PredicatePlan;

use super::super::error::IcebergFilterError;
use super::super::plan::{
    PlannedIcebergColumn, PlannedIcebergNode, PlannedValueType,
};
use super::{BoundComparisonValue, IcebergFilterBinder};

impl IcebergFilterBinder<'_> {
    pub(super) fn bind_in_set(
        column: &PlannedIcebergColumn,
        value_type: PlannedValueType,
        set: RuntimeValue,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        // `x = ANY (NULL)` is UNKNOWN for every row.
        if set.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
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
        let mut literals = Vec::new();
        // A NULL element only turns a non-match into UNKNOWN, and Conservative
        // plans are never negated.
        for datum in elements.flatten() {
            // SAFETY: the iterator retains the array for this loop, and decoding
            // copies the element.
            let value = unsafe { RuntimeValue::from_raw(datum, false, element) };
            match unsafe { Self::decode_value(value_type, value) }? {
                Some(BoundComparisonValue::Datum(literal)) => literals.push(literal),
                // An integer outside the column's range matches no row.
                Some(BoundComparisonValue::OutsideInteger(_)) => {}
                // One unrepresentable element leaves the set to the residual.
                // The widest answer keeps the filter's other predicates bound;
                // it is safe because Conservative plans are never negated.
                _ => return Ok(Some(Predicate::AlwaysTrue)),
            }
        }
        Ok(Some(Self::builder(column).in_set(literals)))
    }

    /// Bind a set-bearing tree without its sets
    /// ([`PlannedIcebergNode::row_filter`]).
    pub(super) fn bind_row_filter(
        &self,
        node: &PlannedIcebergNode,
    ) -> Result<PredicatePlan<Predicate>, IcebergFilterError> {
        node.row_filter(
            &|leaf| {
                Ok(self
                    .bind_node(leaf, false)?
                    .map_or(PredicatePlan::Unsupported, PredicatePlan::Exact))
            },
            |bound| Self::combine(bound, Predicate::and),
            |bound| Self::combine(bound, Predicate::or),
        )
    }

    fn combine(
        predicates: Vec<Predicate>,
        combine: fn(Predicate, Predicate) -> Predicate,
    ) -> Predicate {
        predicates
            .into_iter()
            .reduce(combine)
            .expect("the shared tree rules combine at least one predicate")
    }
}
