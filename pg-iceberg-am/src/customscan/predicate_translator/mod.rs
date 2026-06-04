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

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_lite::spec::Datum;
    use pgrx::pg_sys::Oid;

    fn map_comparison_operator(
        op: PgComparisonOp,
    ) -> Result<PredicateOperator, IcebergTranslationError> {
        IcebergPredicateTranslator::new().map_comparison_operator(op)
    }

    fn capability_allows_build(type_oid: pg_sys::Oid, op: PgComparisonOp) -> bool {
        PredicatePushdownPolicy::new().can_build(type_oid, op)
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

    fn op_triple_with_collation(opno: u32, collid: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::from(collid),
            inputcollid: Oid::from(collid),
        }
    }

    fn text_triple(opno: u32, collid: Oid) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: collid,
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
    fn gate_rejects_integer_with_non_default_collation() {
        assert!(capability_allows_build(pg_sys::INT4OID, op_triple(96)));
        assert!(!capability_allows_build(
            pg_sys::INT4OID,
            op_triple_with_collation(96, 100),
        ));
        assert!(capability_allows_build(pg_sys::INT8OID, op_triple(412)));
        assert!(!capability_allows_build(
            pg_sys::INT8OID,
            op_triple_with_collation(412, 100),
        ));
    }

    #[test]
    fn gate_rejects_non_integer_not_equal() {
        // Numeric comparison buildability follows the pushdown toggle.
        assert_eq!(
            capability_allows_build(pg_sys::NUMERICOID, op_triple(1754)),
            super::super::NUMERIC_COMPARISON_PUSHDOWN_ENABLED,
        );
        assert!(!capability_allows_build(
            pg_sys::NUMERICOID,
            op_triple(1753)
        ));
        // Temporal types stay buildable for ordered ops, never for `<>`.
        assert!(capability_allows_build(pg_sys::DATEOID, op_triple(1098)));
        assert!(!capability_allows_build(pg_sys::DATEOID, op_triple(1094)));
        // Float buildability depends on the FLOAT_PUSHDOWN_ENABLED toggle.
        assert_eq!(
            capability_allows_build(pg_sys::FLOAT8OID, op_triple(670)),
            super::super::FLOAT_PUSHDOWN_ENABLED,
        );
        assert!(!capability_allows_build(pg_sys::FLOAT8OID, op_triple(671)));
    }

    #[test]
    fn gate_text_collation_host_safe_cases() {
        for collid in [pg_sys::C_COLLATION_OID, pg_sys::POSIX_COLLATION_OID] {
            assert!(capability_allows_build(
                pg_sys::TEXTOID,
                text_triple(664, collid),
            ));
        }
        assert!(!capability_allows_build(
            pg_sys::TEXTOID,
            text_triple(664, Oid::INVALID),
        ));
        assert!(!capability_allows_build(
            pg_sys::TEXTOID,
            text_triple(98, Oid::INVALID),
        ));
        assert!(!capability_allows_build(
            pg_sys::TEXTOID,
            text_triple(531, pg_sys::C_COLLATION_OID),
        ));
    }

    #[test]
    fn gate_rejects_unknown_type() {
        assert!(!capability_allows_build(pg_sys::BOOLOID, op_triple(96)));
        assert!(!capability_allows_build(pg_sys::BYTEAOID, op_triple(96)));
    }

    #[test]
    fn param_value_null_decodes_to_null() {
        let mut translator = IcebergPredicateTranslator::new();
        let null_param = PgParamValue {
            param_id: 1,
            paramkind: pg_sys::ParamKind::PARAM_EXTERN,
            type_oid: pg_sys::INT4OID,
            collid: pg_sys::Oid::INVALID,
            datum: pg_sys::Datum::from(0usize),
            is_null: true,
        };

        let result = translator.param_value(null_param);

        match result {
            Ok(IcebergScalar::Null { type_oid }) => {
                assert_eq!(
                    type_oid,
                    pg_sys::INT4OID,
                    "Null scalar must carry the param's PG type OID",
                );
            }
            other => panic!(
                "post-fix: a NULL-resolved param must decode to \
                 Ok(IcebergScalar::Null {{ .. }}); got {other:?}",
            ),
        }
    }

    use proptest::prelude::*;

    const INT4_TYPE_OID: u32 = 23;
    const INT8_TYPE_OID: u32 = 20;

    const ALLOWLISTED_OPNOS: [u32; 12] =
        [96, 518, 97, 523, 521, 525, 410, 411, 412, 413, 414, 415];

    fn oracle_predicate_op(opno: u32) -> PredicateOperator {
        match opno {
            96 | 410 => PredicateOperator::Eq,
            518 | 411 => PredicateOperator::NotEq,
            97 | 412 => PredicateOperator::LessThan,
            523 | 414 => PredicateOperator::LessThanOrEq,
            521 | 413 => PredicateOperator::GreaterThan,
            525 | 415 => PredicateOperator::GreaterThanOrEq,
            other => unreachable!("opno {other} is outside the Property-2 allowlist"),
        }
    }

    fn oracle_mirror(op: PredicateOperator) -> PredicateOperator {
        match op {
            PredicateOperator::LessThan => PredicateOperator::GreaterThan,
            PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
            PredicateOperator::GreaterThan => PredicateOperator::LessThan,
            PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
            other => other,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn prop2_preserves_non_null_binary_predicate(
            op_idx in 0usize..ALLOWLISTED_OPNOS.len(),
            column_left in any::<bool>(),
            is_int8 in any::<bool>(),
            v32 in any::<i32>(),
            v64 in any::<i64>(),
        ) {
            let opno = ALLOWLISTED_OPNOS[op_idx];
            let op = op_triple(opno);
            let col_name = "id";

            let (atttypid, datum) = if is_int8 {
                (Oid::from(INT8_TYPE_OID), Datum::long(v64))
            } else {
                (Oid::from(INT4_TYPE_OID), Datum::int(v32))
            };

            let column = IcebergScalar::Column {
                reference: Reference::new(col_name),
                atttypid,
            };
            let scalar = IcebergScalar::Datum(datum.clone());

            let (left, right) = if column_left {
                (column, scalar)
            } else {
                (scalar, column)
            };

            let mut translator = IcebergPredicateTranslator::new();
            let got = translator
                .comparison(op, left, right)
                .expect("non-NULL column op literal must translate on unfixed code");

            let base = oracle_predicate_op(opno);
            let expected_op = if column_left {
                base
            } else {
                oracle_mirror(base)
            };
            let expected = Predicate::Binary(BinaryExpression::new(
                expected_op,
                Reference::new(col_name),
                datum,
            ));

            prop_assert_eq!(got, expected);
        }
    }

    fn null_scalar(type_oid: u32) -> IcebergScalar {
        IcebergScalar::Null {
            type_oid: Oid::from(type_oid),
        }
    }

    fn column_scalar(name: &str, type_oid: u32) -> IcebergScalar {
        IcebergScalar::Column {
            reference: Reference::new(name),
            atttypid: Oid::from(type_oid),
        }
    }

    #[test]
    fn comparison_null_left_folds_to_always_false() {
        let mut t = IcebergPredicateTranslator::new();
        let got = t
            .comparison(
                op_triple(96),
                null_scalar(INT4_TYPE_OID),
                column_scalar("id", INT4_TYPE_OID),
            )
            .expect("a NULL operand must fold, never error");
        assert_eq!(got, Predicate::AlwaysFalse);
    }

    #[test]
    fn comparison_null_right_folds_to_always_false() {
        let mut t = IcebergPredicateTranslator::new();
        let got = t
            .comparison(
                op_triple(96),
                column_scalar("id", INT4_TYPE_OID),
                null_scalar(INT4_TYPE_OID),
            )
            .expect("a NULL operand must fold, never error");
        assert_eq!(got, Predicate::AlwaysFalse);
    }

    #[test]
    fn comparison_null_both_folds_to_always_false() {
        let mut t = IcebergPredicateTranslator::new();
        let got = t
            .comparison(
                op_triple(96),
                null_scalar(INT4_TYPE_OID),
                null_scalar(INT8_TYPE_OID),
            )
            .expect("a NULL operand must fold, never error");
        assert_eq!(got, Predicate::AlwaysFalse);
    }

    #[test]
    fn comparison_each_strict_operator_with_null_folds() {
        for opno in ALLOWLISTED_OPNOS {
            let mut t = IcebergPredicateTranslator::new();
            let got = t
                .comparison(
                    op_triple(opno),
                    column_scalar("id", INT4_TYPE_OID),
                    null_scalar(INT4_TYPE_OID),
                )
                .unwrap_or_else(|e| {
                    panic!("opno {opno} with NULL on RHS must fold, got Err: {e}")
                });
            assert_eq!(
                got,
                Predicate::AlwaysFalse,
                "opno {opno} with NULL on RHS must fold to AlwaysFalse",
            );

            let mut t = IcebergPredicateTranslator::new();
            let got = t
                .comparison(
                    op_triple(opno),
                    null_scalar(INT4_TYPE_OID),
                    column_scalar("id", INT4_TYPE_OID),
                )
                .unwrap_or_else(|e| {
                    panic!("opno {opno} with NULL on LHS must fold, got Err: {e}")
                });
            assert_eq!(
                got,
                Predicate::AlwaysFalse,
                "opno {opno} with NULL on LHS must fold to AlwaysFalse",
            );
        }
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

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn prop1_null_operand_folds_to_always_false(
            op_idx in 0usize..ALLOWLISTED_OPNOS.len(),
            null_on_left in any::<bool>(),
            is_int8 in any::<bool>(),
        ) {
            let opno = ALLOWLISTED_OPNOS[op_idx];
            let op = op_triple(opno);
            let type_oid = if is_int8 { INT8_TYPE_OID } else { INT4_TYPE_OID };

            let column = column_scalar("id", type_oid);
            let null = null_scalar(type_oid);

            let (left, right) = if null_on_left {
                (null, column)
            } else {
                (column, null)
            };

            let mut translator = IcebergPredicateTranslator::new();
            let result = translator.comparison(op, left, right);

            prop_assert!(
                result.is_ok(),
                "a NULL operand must never error; got {result:?}",
            );
            prop_assert_eq!(result.unwrap(), Predicate::AlwaysFalse);
        }
    }

    const INTEGER_OPNOS: [u32; 18] = [
        94, 519, 95, 522, 520, 524, 96, 518, 97, 523, 521, 525, 410, 411, 412, 413,
        414, 415,
    ];

    fn integer_oracle_op(opno: u32) -> PredicateOperator {
        match opno {
            94 | 96 | 410 => PredicateOperator::Eq,
            519 | 518 | 411 => PredicateOperator::NotEq,
            95 | 97 | 412 => PredicateOperator::LessThan,
            522 | 523 | 414 => PredicateOperator::LessThanOrEq,
            520 | 521 | 413 => PredicateOperator::GreaterThan,
            524 | 525 | 415 => PredicateOperator::GreaterThanOrEq,
            other => unreachable!("opno {other} is outside the integer op set"),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn prop2_preserves_integer_operator_mapping(
            idx in 0usize..INTEGER_OPNOS.len(),
        ) {
            let opno = INTEGER_OPNOS[idx];
            let got = map_comparison_operator(op_triple(opno)).expect(
                "every integer opno under (0,0) collation must map on unfixed code",
            );
            prop_assert_eq!(got, integer_oracle_op(opno));
        }
    }
}
