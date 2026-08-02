//! Representative backend wiring from raw PG nodes through classification and translation.

#[pgrx::pg_schema]
mod tests {
    use iceberg_lite::expr::{
        BinaryExpression, Predicate, PredicateOperator, Reference,
    };
    use iceberg_lite::spec::Datum;
    use pg_lakebase_core::expr::{
        PushdownContract, PushdownCosting, QualPushdownDecision,
    };
    use pgrx::IntoDatum;
    use pgrx::pg_sys;
    use pgrx::prelude::{AnyNumeric, Date, Timestamp};

    use crate::predicate::pg_test::harness::{
        ComparisonOpSpec, ComparisonSpec, ConstSpec, DATE_GE_OPNO, INT4EQ_OPNO,
        INT4LT_OPNO, NUMERIC_LT_OPNO, OperandSpec, PREDICATE_HARNESS, RelabelSpec,
        ScanColumnSpec, TEXT_LT_OPNO, TEXTEQ_OPNO, TIMESTAMP_GT_OPNO,
    };

    #[pgrx::pg_test(schema = "tests")]
    fn translator_text_wiring_respects_collation_policy() {
        let default_collation = u32::from(pg_sys::DEFAULT_COLLATION_OID);
        let c_collation = u32::from(pg_sys::C_COLLATION_OID);

        for (label, opno, collation, builds) in [
            ("default equality", TEXTEQ_OPNO, default_collation, true),
            ("default ordering", TEXT_LT_OPNO, default_collation, false),
            ("C ordering", TEXT_LT_OPNO, c_collation, true),
        ] {
            let obs = unsafe {
                PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                    ScanColumnSpec::synthetic(pg_sys::TEXTOID, collation),
                    ComparisonOpSpec {
                        opno,
                        opcollid: 0,
                        inputcollid: collation,
                        opfuncid: 0,
                        opresulttype: pg_sys::BOOLOID,
                    },
                    ConstSpec {
                        type_oid: pg_sys::TEXTOID,
                        collation,
                        len: -1,
                        datum: "x".into_datum().expect("text into_datum"),
                        byval: false,
                    },
                ))
            };
            assert_eq!(
                matches!(obs.decision, QualPushdownDecision::Pushable { .. }),
                builds,
                "{label}: classifier mismatch",
            );
            assert_eq!(
                obs.translator_builds(),
                builds,
                "{label}: translator mismatch"
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn representative_classifier_translator_cases_stay_in_lockstep() {
        type Case = (
            &'static str,
            pg_sys::Oid,
            u32,
            i32,
            pg_sys::Datum,
            bool,
            Option<(PushdownContract, PushdownCosting)>,
        );

        let cases: [Case; 4] = [
            (
                "int4 = 7",
                pg_sys::INT4OID,
                INT4EQ_OPNO,
                4,
                pg_sys::Datum::from(7usize),
                true,
                Some((
                    PushdownContract::ExactRowFilter,
                    PushdownCosting::CostedPruning,
                )),
            ),
            (
                "numeric < 100.5",
                pg_sys::NUMERICOID,
                NUMERIC_LT_OPNO,
                -1,
                AnyNumeric::try_from(100.5_f64)
                    .expect("valid numeric")
                    .into_datum()
                    .expect("numeric into_datum"),
                false,
                None,
            ),
            (
                "date >= 2024-01-01",
                pg_sys::DATEOID,
                DATE_GE_OPNO,
                4,
                Date::new(2024, 1, 1)
                    .expect("valid date")
                    .into_datum()
                    .expect("date into_datum"),
                true,
                Some((
                    PushdownContract::ConservativePruning,
                    PushdownCosting::UncostedBestEffort,
                )),
            ),
            (
                "timestamp > 2024-01-01",
                pg_sys::TIMESTAMPOID,
                TIMESTAMP_GT_OPNO,
                8,
                Timestamp::new(2024, 1, 1, 0, 0, 0.0)
                    .expect("valid timestamp")
                    .into_datum()
                    .expect("timestamp into_datum"),
                true,
                Some((
                    PushdownContract::ConservativePruning,
                    PushdownCosting::UncostedBestEffort,
                )),
            ),
        ];

        for (label, type_oid, opno, len, datum, byval, expected) in cases {
            let obs = unsafe {
                PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                    ScanColumnSpec::synthetic(type_oid, 0),
                    ComparisonOpSpec {
                        opno,
                        opcollid: 0,
                        inputcollid: 0,
                        opfuncid: 0,
                        opresulttype: pg_sys::BOOLOID,
                    },
                    ConstSpec {
                        type_oid,
                        collation: 0,
                        len,
                        datum,
                        byval,
                    },
                ))
            };

            match expected {
                Some((contract, costing)) => assert_eq!(
                    obs.decision,
                    QualPushdownDecision::Pushable { contract, costing },
                    "{label}",
                ),
                None => assert_eq!(
                    obs.decision,
                    QualPushdownDecision::Unsupported,
                    "{label}",
                ),
            }
            assert_eq!(
                obs.translator_builds(),
                expected.is_some(),
                "{label}: classifier and translator diverged: {}",
                obs.translate_debug(),
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn translator_builds_exact_integer_payload() {
        let obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::INT4OID, 0),
                ComparisonOpSpec {
                    opno: INT4EQ_OPNO,
                    opcollid: 0,
                    inputcollid: 0,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                ConstSpec {
                    type_oid: pg_sys::INT4OID,
                    collation: 0,
                    len: 4,
                    datum: pg_sys::Datum::from(7usize),
                    byval: true,
                },
            ))
        };

        assert_eq!(
            obs.translated.expect("int4 equality must translate"),
            Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                Reference::new("col"),
                Datum::int(7),
            )),
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn translator_mirrors_literal_left_directional_operator() {
        let obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::new(
                ScanColumnSpec::synthetic(pg_sys::INT4OID, 0),
                OperandSpec::Const(ConstSpec {
                    type_oid: pg_sys::INT4OID,
                    collation: 0,
                    len: 4,
                    datum: pg_sys::Datum::from(7usize),
                    byval: true,
                }),
                OperandSpec::ScanCol,
                ComparisonOpSpec {
                    opno: INT4LT_OPNO,
                    opcollid: 0,
                    inputcollid: 0,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                RelabelSpec::NONE,
            ))
        };

        assert_eq!(
            obs.translated
                .expect("literal-left comparison must translate"),
            Predicate::Binary(BinaryExpression::new(
                PredicateOperator::GreaterThan,
                Reference::new("col"),
                Datum::int(7),
            )),
        );
    }
}
