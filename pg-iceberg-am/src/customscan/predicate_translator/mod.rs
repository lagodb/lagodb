//! Runtime [`IcebergPredicateTranslator`] (PG expr → iceberg [`Predicate`]).

mod datum_decoder;
mod error;
mod fold;
mod scalar;

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use pg_lakebase_core::expr::ColumnNameResolver;
use pg_lakebase_core::expr::nodes::{
    PgColumnRef, PgComparisonOp, PgLiteral, PgParamValue,
};
use pg_lakebase_core::expr::translator::PgPredicateTranslator;
use pgrx::pg_sys;

use super::predicate_pushdown_policy::{ComparisonOpClass, PredicatePushdownPolicy};
use fold::{fold_predicates, mirror_operator};

pub(crate) use datum_decoder::IcebergDatumDecoder;
pub use error::IcebergTranslationError;
pub(crate) use fold::fold_left;
pub use scalar::{IcebergScalar, ScalarKind};

/// Runtime [`PgPredicateTranslator`] for Iceberg: column refs, datum decode, predicate assembly.
#[derive(Debug, Default)]
pub struct IcebergPredicateTranslator {
    pushdown: PredicatePushdownPolicy,
}

impl IcebergPredicateTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub const fn with_policy(pushdown: PredicatePushdownPolicy) -> Self {
        Self { pushdown }
    }
}

impl PgPredicateTranslator for IcebergPredicateTranslator {
    type Scalar = IcebergScalar;
    type Predicate = Predicate;
    type Error = IcebergTranslationError;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error> {
        let name = Self::resolve_column_name(col.name, col.rel_oid, col.attno)?;
        Ok(IcebergScalar::Column {
            reference: Reference::new(name),
            atttypid: col.atttypid,
        })
    }

    /// NULL literals decode to [`IcebergScalar::Null`]; [`Self::comparison`] folds them to `AlwaysFalse`.
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error> {
        if lit.is_null {
            return Ok(IcebergScalar::Null {
                type_oid: lit.type_oid,
            });
        }
        let datum = unsafe { IcebergDatumDecoder::decode(lit.type_oid, lit.datum) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    /// Mirrors [`Self::literal`]: NULL params decode to [`IcebergScalar::Null`], not an error.
    fn param_value(
        &mut self,
        param: PgParamValue,
    ) -> Result<Self::Scalar, Self::Error> {
        if param.is_null {
            return Ok(IcebergScalar::Null {
                type_oid: param.type_oid,
            });
        }
        let datum =
            unsafe { IcebergDatumDecoder::decode(param.type_oid, param.datum) }?;
        Ok(IcebergScalar::Datum(datum))
    }

    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        // SQL three-valued logic: strict comparison with NULL is UNKNOWN → fold to AlwaysFalse.
        if matches!(left, IcebergScalar::Null { .. })
            || matches!(right, IcebergScalar::Null { .. })
        {
            return Ok(Predicate::AlwaysFalse);
        }

        let (reference, atttypid, datum, swap_sides) = match (left, right) {
            (
                IcebergScalar::Column {
                    reference,
                    atttypid,
                },
                IcebergScalar::Datum(datum),
            ) => (reference, atttypid, datum, false),
            (
                IcebergScalar::Datum(datum),
                IcebergScalar::Column {
                    reference,
                    atttypid,
                },
            ) => (reference, atttypid, datum, true),
            (l, r) => {
                return Err(IcebergTranslationError::ComparisonShape {
                    left: l.kind(),
                    right: r.kind(),
                });
            }
        };

        if !self.pushdown.can_build(atttypid, op) {
            return Err(IcebergTranslationError::UnsupportedType {
                type_oid: atttypid,
            });
        }

        let mut predicate_op = self.map_comparison_operator(op)?;
        if swap_sides {
            predicate_op = mirror_operator(predicate_op);
        }

        Ok(Predicate::Binary(BinaryExpression::new(
            predicate_op,
            reference,
            datum,
        )))
    }

    fn is_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        let reference = self.null_test_reference(value)?;
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            reference,
        )))
    }

    fn is_not_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error> {
        let reference = self.null_test_reference(value)?;
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            reference,
        )))
    }

    fn and(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error> {
        fold_predicates(items, /*and=*/ true)
    }

    fn or(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error> {
        fold_predicates(items, /*and=*/ false)
    }

    /// Wraps the child in `Predicate::Not` (schema binding applies `rewrite_not` later).
    fn not(&mut self, item: Self::Predicate) -> Result<Self::Predicate, Self::Error> {
        Ok(!item)
    }
}

impl IcebergPredicateTranslator {
    /// Extract the column [`Reference`] for an `IS NULL` / `IS NOT NULL` test.
    ///
    /// Owns the model→error mapping: a non-column operand is rejected as
    /// [`IcebergTranslationError::NullTestOnNonColumn`], and (defense in depth)
    /// a column whose type is outside the shared null-test allowlist is
    /// rejected as [`IcebergTranslationError::UnsupportedType`].
    fn null_test_reference(
        &self,
        value: IcebergScalar,
    ) -> Result<Reference, IcebergTranslationError> {
        let IcebergScalar::Column {
            reference,
            atttypid,
        } = value
        else {
            return Err(IcebergTranslationError::NullTestOnNonColumn);
        };

        if matches!(
            self.pushdown.null_test_capability(atttypid),
            super::predicate_pushdown_policy::PredicateCapability::Unsupported
        ) {
            return Err(IcebergTranslationError::UnsupportedType {
                type_oid: atttypid,
            });
        }
        Ok(reference)
    }

    /// Resolve column name from carried plan-time name or core's attname fallback.
    fn resolve_column_name(
        carried: Option<&str>,
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    ) -> Result<String, IcebergTranslationError> {
        if let Some(name) = carried {
            return Ok(name.to_string());
        }

        if attno <= 0 {
            return Err(IcebergTranslationError::SystemOrWholeRowColumn {
                rel_oid,
                attno,
            });
        }

        match ColumnNameResolver::new(rel_oid).try_resolve(attno) {
            Ok(Some(name)) => Ok(name),
            Ok(None) => {
                Err(IcebergTranslationError::ColumnLookupFailed { rel_oid, attno })
            }
            Err(cause) => Err(IcebergTranslationError::ColumnNameNotUtf8 {
                rel_oid,
                attno,
                cause: cause.to_string(),
            }),
        }
    }

    /// Map PG operator triple to iceberg [`PredicateOperator`] via shared
    /// comparison-op policy. Collation admissibility is checked separately in
    /// `comparison`.
    fn map_comparison_operator(
        &self,
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        match self.pushdown.op_class(op.opno) {
            Some(ComparisonOpClass::Eq) => Ok(PredicateOperator::Eq),
            Some(ComparisonOpClass::NotEq) => Ok(PredicateOperator::NotEq),
            Some(ComparisonOpClass::Lt) => Ok(PredicateOperator::LessThan),
            Some(ComparisonOpClass::Le) => Ok(PredicateOperator::LessThanOrEq),
            Some(ComparisonOpClass::Gt) => Ok(PredicateOperator::GreaterThan),
            Some(ComparisonOpClass::Ge) => Ok(PredicateOperator::GreaterThanOrEq),
            None => Err(IcebergTranslationError::UnsupportedOperator {
                opno: op.opno,
                opcollid: op.opcollid,
                inputcollid: op.inputcollid,
            }),
        }
    }
}

// =============================================================================
// Host tests: pure translator logic that needs no PG backend.
//
// `map_comparison_operator` delegates to the pure `op_class` mapping, and the
// `is_null` / `is_not_null` non-column rejection path only inspects the scalar
// shape — neither touches `pg_sys`. The NULL-folding `comparison` path (gated
// on `can_build` -> `get_collation_isdeterministic`) and the `param_value`
// decode path require a live backend and live in
// `customscan/pg_test/predicate/translator_semantics.rs` (see `docs/testing.md`).
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use pgrx::pg_sys::Oid;

    const INT4_TYPE_OID: u32 = 23;

    fn map_comparison_operator(
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        IcebergPredicateTranslator::new().map_comparison_operator(op)
    }

    fn op_triple(opno: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: Oid::INVALID,
        }
    }

    fn null_scalar(type_oid: u32) -> IcebergScalar {
        IcebergScalar::Null {
            type_oid: Oid::from(type_oid),
        }
    }

    #[test]
    fn maps_int4_operators() {
        assert_eq!(
            map_comparison_operator(op_triple(96)).unwrap(),
            PredicateOperator::Eq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(518)).unwrap(),
            PredicateOperator::NotEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(97)).unwrap(),
            PredicateOperator::LessThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(523)).unwrap(),
            PredicateOperator::LessThanOrEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(521)).unwrap(),
            PredicateOperator::GreaterThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(525)).unwrap(),
            PredicateOperator::GreaterThanOrEq,
        );
    }

    #[test]
    fn maps_int8_operators() {
        for opno in [410u32, 411, 412, 413, 414, 415] {
            assert!(
                map_comparison_operator(op_triple(opno)).is_ok(),
                "int8 opno {opno} must be in the consolidated op_class map",
            );
        }
    }

    #[test]
    fn maps_delegated_non_integer_operators() {
        assert_eq!(
            map_comparison_operator(op_triple(1754)).unwrap(),
            PredicateOperator::LessThan,
        );
        assert_eq!(
            map_comparison_operator(op_triple(1098)).unwrap(),
            PredicateOperator::GreaterThanOrEq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(98)).unwrap(),
            PredicateOperator::Eq,
        );
        assert_eq!(
            map_comparison_operator(op_triple(674)).unwrap(),
            PredicateOperator::GreaterThan,
        );
    }

    #[test]
    fn rejects_unknown_operator() {
        assert!(matches!(
            map_comparison_operator(op_triple(558)),
            Err(IcebergTranslationError::UnsupportedOperator { .. })
        ));
    }

    #[test]
    fn map_comparison_operator_is_collation_agnostic() {
        let mut t = op_triple(96);
        t.inputcollid = Oid::from(100u32);
        assert_eq!(map_comparison_operator(t).unwrap(), PredicateOperator::Eq);

        let mut t = op_triple(96);
        t.opcollid = Oid::from(100u32);
        assert_eq!(map_comparison_operator(t).unwrap(), PredicateOperator::Eq);
    }

    #[test]
    fn is_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new();
        assert!(matches!(
            t.is_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }

    #[test]
    fn is_not_null_with_null_scalar_fails_closed() {
        let mut t = IcebergPredicateTranslator::new();
        assert!(matches!(
            t.is_not_null(null_scalar(INT4_TYPE_OID)),
            Err(IcebergTranslationError::NullTestOnNonColumn)
        ));
    }
}
