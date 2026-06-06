//! Backend tests for translator semantics over synthetic scalars.
//!
//! [`IcebergPredicateTranslator::comparison`] gates on
//! `PredicatePushdownPolicy::can_build` (which reaches
//! `get_collation_isdeterministic`), and `param_value` decodes through
//! `decode_datum` (numeric/text arms reference PG backend symbols), so
//! both paths require a live backend even when driven with already-built
//! scalars (see `docs/testing.md`).
//!
//! The pure `map_comparison_operator` and `is_null` / `is_not_null`
//! (non-column rejection) tests remain host `#[test]`s in
//! `customscan/predicate_translator.rs`. This module owns the SQL
//! three-valued-logic NULL folding and the NULL-param decode contract.

#[pgrx::pg_schema]
mod tests {
    use iceberg_lite::expr::{Predicate, Reference};
    use pg_lakebase_core::expr::nodes::{PgComparisonOp, PgParamValue};
    use pg_lakebase_core::expr::translator::PgPredicateTranslator;
    use pgrx::pg_sys;
    use pgrx::pg_sys::Oid;

    use crate::customscan::{IcebergPredicateTranslator, IcebergScalar};

    const INT4_TYPE_OID: u32 = 23;
    const INT8_TYPE_OID: u32 = 20;

    /// The strict binary comparison opnos that fold to `AlwaysFalse` on a NULL
    /// operand (int4 + int8 `=`, `<>`, `<`, `<=`, `>`, `>=`).
    const ALLOWLISTED_OPNOS: [u32; 12] =
        [96, 518, 97, 523, 521, 525, 410, 411, 412, 413, 414, 415];

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

    fn column_scalar(name: &str, type_oid: u32) -> IcebergScalar {
        IcebergScalar::Column {
            reference: Reference::new(name),
            atttypid: Oid::from(type_oid),
        }
    }

    /// A NULL on the left operand folds the comparison to `AlwaysFalse`
    /// (SQL three-valued logic: strict comparison with NULL is UNKNOWN).
    #[pgrx::pg_test(schema = "tests")]
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

    #[pgrx::pg_test(schema = "tests")]
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

    #[pgrx::pg_test(schema = "tests")]
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

    /// Every strict operator with NULL on either side folds to `AlwaysFalse`.
    #[pgrx::pg_test(schema = "tests")]
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

    /// A NULL-resolved param decodes to `Ok(IcebergScalar::Null { .. })`
    /// (mirrors the NULL `Const` literal path), carrying the param's type OID.
    #[pgrx::pg_test(schema = "tests")]
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
                "a NULL-resolved param must decode to \
                 Ok(IcebergScalar::Null {{ .. }}); got {other:?}",
            ),
        }
    }
}
