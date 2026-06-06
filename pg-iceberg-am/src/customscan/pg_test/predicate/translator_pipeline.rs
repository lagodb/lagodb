//! Backend classifier/translator pipeline tests from raw PG nodes.

#[pgrx::pg_schema]
mod tests {
    use crate::customscan::pg_test::support::fixtures::{
        DATE_EQ_OPNO, DATE_GE_OPNO, INT4EQ_OPNO, NUMERIC_LT_OPNO, TEXT_LT_OPNO,
        TEXTEQ_OPNO, TEXTEQ_OPNO_SCOPED, TEXTNE_OPNO, TIMESTAMP_EQ_OPNO,
        TIMESTAMP_GT_OPNO,
    };
    use crate::customscan::pg_test::support::predicate_harness::{
        ComparisonOpSpec, ComparisonSpec, ConstSpec, OperandSpec, PREDICATE_HARNESS,
        RelabelSpec, ScanColumnSpec,
    };
    use iceberg_lite::expr::{
        BinaryExpression, Predicate, PredicateOperator, Reference,
    };
    use iceberg_lite::spec::Datum;
    use pg_lakebase_core::expr::split::{
        PushdownContract, PushdownCosting, QualPushdownDecision,
    };
    use pgrx::pg_sys;

    /// Text comparison buildability depends on the same backend collation semantics
    /// that drive the classifier.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_text_buildability_matrix() {
        use pgrx::IntoDatum;

        let default_collation = u32::from(pg_sys::DEFAULT_COLLATION_OID);
        let c_collation = u32::from(pg_sys::C_COLLATION_OID);

        let text_eq = "x".into_datum().expect("text into_datum");
        let eq_obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::TEXTOID, default_collation),
                ComparisonOpSpec {
                    opno: TEXTEQ_OPNO,
                    opcollid: 0,
                    inputcollid: default_collation,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                ConstSpec {
                    type_oid: pg_sys::TEXTOID,
                    collation: default_collation,
                    len: -1,
                    datum: text_eq,
                    byval: false,
                },
            ))
        };
        assert!(
            eq_obs.translator_builds(),
            "text `=` under the deterministic default collation must build; got {}",
            eq_obs.translate_debug(),
        );

        let text_ne = "x".into_datum().expect("text into_datum");
        let ne_obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::TEXTOID, default_collation),
                ComparisonOpSpec {
                    opno: TEXTNE_OPNO,
                    opcollid: 0,
                    inputcollid: default_collation,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                ConstSpec {
                    type_oid: pg_sys::TEXTOID,
                    collation: default_collation,
                    len: -1,
                    datum: text_ne,
                    byval: false,
                },
            ))
        };
        assert!(
            !ne_obs.translator_builds(),
            "text `<>` must not build; got {}",
            ne_obs.translate_debug(),
        );

        let text_default_lt = "m".into_datum().expect("text into_datum");
        let default_lt_obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::TEXTOID, default_collation),
                ComparisonOpSpec {
                    opno: TEXT_LT_OPNO,
                    opcollid: 0,
                    inputcollid: default_collation,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                ConstSpec {
                    type_oid: pg_sys::TEXTOID,
                    collation: default_collation,
                    len: -1,
                    datum: text_default_lt,
                    byval: false,
                },
            ))
        };
        assert!(
            !default_lt_obs.translator_builds(),
            "ordered text under the default collation must not build; got {}",
            default_lt_obs.translate_debug(),
        );

        let text_c_lt = "m".into_datum().expect("text into_datum");
        let c_lt_obs = unsafe {
            PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::TEXTOID, c_collation),
                ComparisonOpSpec {
                    opno: TEXT_LT_OPNO,
                    opcollid: 0,
                    inputcollid: c_collation,
                    opfuncid: 0,
                    opresulttype: pg_sys::BOOLOID,
                },
                ConstSpec {
                    type_oid: pg_sys::TEXTOID,
                    collation: c_collation,
                    len: -1,
                    datum: text_c_lt,
                    byval: false,
                },
            ))
        };
        assert!(
            c_lt_obs.translator_builds(),
            "ordered text under the C collation must build; got {}",
            c_lt_obs.translate_debug(),
        );
    }

    /// Conservative runtime literals must still build when their concrete Datum
    /// is representable, even though classification costs some of them as
    /// best-effort at plan time.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_builds_representable_conservative_literals() {
        use pgrx::IntoDatum;
        use pgrx::prelude::{Date, Timestamp};

        type RuntimeCase = (
            &'static str,
            pg_sys::Oid,
            u32,
            u32,
            i32,
            pg_sys::Datum,
            bool,
            PushdownCosting,
        );

        let default_collation = u32::from(pg_sys::DEFAULT_COLLATION_OID);
        let text_datum = "x".into_datum().expect("text literal into_datum");
        let date_datum = Date::new(2024, 1, 1)
            .expect("2024-01-01 is a valid date")
            .into_datum()
            .expect("date literal into_datum");
        let ts_datum = Timestamp::new(2024, 1, 1, 0, 0, 0.0)
            .expect("2024-01-01 00:00:00 is a valid timestamp")
            .into_datum()
            .expect("timestamp literal into_datum");

        let cases: [RuntimeCase; 3] = [
            (
                "text = 'x'",
                pg_sys::TEXTOID,
                TEXTEQ_OPNO_SCOPED,
                default_collation,
                -1,
                text_datum,
                false,
                PushdownCosting::CostedPruning,
            ),
            (
                "date = DATE '2024-01-01'",
                pg_sys::DATEOID,
                DATE_EQ_OPNO,
                0,
                4,
                date_datum,
                true,
                PushdownCosting::UncostedBestEffort,
            ),
            (
                "timestamp = TIMESTAMP '2024-01-01'",
                pg_sys::TIMESTAMPOID,
                TIMESTAMP_EQ_OPNO,
                0,
                8,
                ts_datum,
                true,
                PushdownCosting::UncostedBestEffort,
            ),
        ];

        unsafe {
            for (
                label,
                type_oid,
                opno,
                collation,
                len,
                datum,
                byval,
                expected_costing,
            ) in cases
            {
                let obs =
                    PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                        ScanColumnSpec::synthetic(type_oid, collation),
                        ComparisonOpSpec {
                            opno,
                            opcollid: 0,
                            inputcollid: collation,
                            opfuncid: 0,
                            opresulttype: pg_sys::BOOLOID,
                        },
                        ConstSpec {
                            type_oid,
                            collation,
                            len,
                            datum,
                            byval,
                        },
                    ));

                assert!(
                    matches!(
                        obs.decision,
                        QualPushdownDecision::Pushable {
                            contract: PushdownContract::ConservativePruning,
                            costing,
                        }
                        if costing == expected_costing
                    ),
                    "{label}: representable runtime literal must remain ConservativePruning/{expected_costing:?}, got {:?}",
                    obs.decision,
                );
                assert!(
                    obs.translator_builds(),
                    "{label}: representable runtime literal must build, got {}",
                    obs.translate_debug(),
                );
            }
        }
    }

    /// Classifier pushability and translator buildability must stay in lockstep
    /// for representative scoped operators. Numeric is included as the
    /// disabled case (`NUMERIC_COMPARISON_PUSHDOWN_ENABLED == false`): both the
    /// classifier and the translator must reject it, so lockstep holds as
    /// `false == false`.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_scoped_classifier_lockstep_matrix() {
        use pgrx::IntoDatum;
        use pgrx::prelude::{AnyNumeric, Date, Timestamp};

        type LockstepCase = (
            &'static str,
            pg_sys::Oid,
            u32,
            u32,
            u32,
            i32,
            pg_sys::Datum,
            bool,
        );

        let default_collation = u32::from(pg_sys::DEFAULT_COLLATION_OID);
        let text_datum = "x".into_datum().expect("text literal into_datum");
        let numeric_datum = AnyNumeric::try_from(100.5_f64)
            .expect("100.5 is a valid numeric")
            .into_datum()
            .expect("numeric literal into_datum");
        let date_datum = Date::new(2024, 1, 1)
            .expect("2024-01-01 is a valid date")
            .into_datum()
            .expect("date literal into_datum");
        let ts_datum = Timestamp::new(2024, 1, 1, 0, 0, 0.0)
            .expect("2024-01-01 00:00:00 is a valid timestamp")
            .into_datum()
            .expect("timestamp literal into_datum");

        let cases: [LockstepCase; 4] = [
            (
                "text_col = 'x'",
                pg_sys::TEXTOID,
                default_collation,
                TEXTEQ_OPNO_SCOPED,
                default_collation,
                -1,
                text_datum,
                false,
            ),
            (
                "numeric_col < 100.5",
                pg_sys::NUMERICOID,
                0,
                NUMERIC_LT_OPNO,
                0,
                -1,
                numeric_datum,
                false,
            ),
            (
                "date_col >= DATE '2024-01-01'",
                pg_sys::DATEOID,
                0,
                DATE_GE_OPNO,
                0,
                4,
                date_datum,
                true,
            ),
            (
                "ts_col > TIMESTAMP '2024-01-01'",
                pg_sys::TIMESTAMPOID,
                0,
                TIMESTAMP_GT_OPNO,
                0,
                8,
                ts_datum,
                true,
            ),
        ];

        unsafe {
            for (label, type_oid, collation, opno, inputcollid, len, datum, byval) in
                cases
            {
                let obs =
                    PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                        ScanColumnSpec::synthetic(type_oid, collation),
                        ComparisonOpSpec {
                            opno,
                            opcollid: 0,
                            inputcollid,
                            opfuncid: 0,
                            opresulttype: pg_sys::BOOLOID,
                        },
                        ConstSpec {
                            type_oid,
                            collation,
                            len,
                            datum,
                            byval,
                        },
                    ));

                let classifier_pushable =
                    matches!(obs.decision, QualPushdownDecision::Pushable { .. });
                assert_eq!(
                    classifier_pushable,
                    obs.translator_builds(),
                    "{label}: classifier decision {:?} and translator result {} must stay in lockstep",
                    obs.decision,
                    obs.translate_debug(),
                );
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct IntExactCase {
        opno: u32,
        type_oid: pg_sys::Oid,
        expected_op: PredicateOperator,
    }

    fn int_exact_cases() -> [IntExactCase; 12] {
        [
            IntExactCase {
                opno: 96,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::Eq,
            },
            IntExactCase {
                opno: 518,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::NotEq,
            },
            IntExactCase {
                opno: 97,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::LessThan,
            },
            IntExactCase {
                opno: 523,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::LessThanOrEq,
            },
            IntExactCase {
                opno: 521,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::GreaterThan,
            },
            IntExactCase {
                opno: 525,
                type_oid: pg_sys::INT4OID,
                expected_op: PredicateOperator::GreaterThanOrEq,
            },
            IntExactCase {
                opno: 410,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::Eq,
            },
            IntExactCase {
                opno: 411,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::NotEq,
            },
            IntExactCase {
                opno: 412,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::LessThan,
            },
            IntExactCase {
                opno: 414,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::LessThanOrEq,
            },
            IntExactCase {
                opno: 413,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::GreaterThan,
            },
            IntExactCase {
                opno: 415,
                type_oid: pg_sys::INT8OID,
                expected_op: PredicateOperator::GreaterThanOrEq,
            },
        ]
    }

    /// Integer exact clauses must stay exact at classification time and preserve
    /// their final predicate operator through runtime translation.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_integer_exact_pipeline_matrix() {
        unsafe {
            for case in int_exact_cases() {
                let obs =
                    PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
                        ScanColumnSpec::synthetic(case.type_oid, 0),
                        ComparisonOpSpec {
                            opno: case.opno,
                            opcollid: 0,
                            inputcollid: 0,
                            opfuncid: 0,
                            opresulttype: pg_sys::BOOLOID,
                        },
                        ConstSpec {
                            type_oid: case.type_oid,
                            collation: 0,
                            len: if case.type_oid == pg_sys::INT8OID {
                                8
                            } else {
                                4
                            },
                            datum: pg_sys::Datum::from(7usize),
                            byval: true,
                        },
                    ));

                assert!(
                    matches!(
                        obs.decision,
                        QualPushdownDecision::Pushable {
                            contract: PushdownContract::ExactRowFilter,
                            costing: PushdownCosting::CostedPruning,
                        }
                    ),
                    "opno {} (type {:?}) must classify ExactRowFilter, got {:?}",
                    case.opno,
                    case.type_oid,
                    obs.decision,
                );

                match obs.translated {
                    Ok(Predicate::Binary(ref be)) => {
                        assert_eq!(
                            be.op(),
                            case.expected_op,
                            "opno {} (type {:?}) must translate to {:?}",
                            case.opno,
                            case.type_oid,
                            case.expected_op,
                        );
                    }
                    other => panic!(
                        "opno {} (type {:?}) must translate to a Binary predicate, got {other:?}",
                        case.opno, case.type_oid,
                    ),
                }
            }
        }
    }

    /// Mirror a binary operator for the `literal op column` operand order, so the
    /// expected operator matches what `comparison()` must produce after
    /// `swap_sides → mirror_operator`. Self-inverse on directional ops, identity
    /// on symmetric ones.
    fn mirror_operator_oracle(op: PredicateOperator) -> PredicateOperator {
        match op {
            PredicateOperator::LessThan => PredicateOperator::GreaterThan,
            PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
            PredicateOperator::GreaterThan => PredicateOperator::LessThan,
            PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
            other => other,
        }
    }

    /// `literal op column` clauses must classify identically to `column op
    /// literal` and translate to the *mirrored* predicate operator (e.g.
    /// `7 < col` → `col > 7`), still referencing the scan column with the
    /// original datum. This is the integration guard for the
    /// `swap_sides → mirror_operator` wiring in `comparison()`: the pure
    /// `mirror_operator` unit tests and the classifier order-invariance test do
    /// not exercise the final translated `PredicateOperator`.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_integer_literal_op_column_mirrors_operator() {
        unsafe {
            for case in int_exact_cases() {
                let is_int8 = case.type_oid == pg_sys::INT8OID;
                let const_spec = ConstSpec {
                    type_oid: case.type_oid,
                    collation: 0,
                    len: if is_int8 { 8 } else { 4 },
                    datum: pg_sys::Datum::from(7usize),
                    byval: true,
                };
                // Literal on the left, scan column on the right.
                let obs = PREDICATE_HARNESS.observe(&ComparisonSpec::new(
                    ScanColumnSpec::synthetic(case.type_oid, 0),
                    OperandSpec::Const(const_spec),
                    OperandSpec::ScanCol,
                    ComparisonOpSpec {
                        opno: case.opno,
                        opcollid: 0,
                        inputcollid: 0,
                        opfuncid: 0,
                        opresulttype: pg_sys::BOOLOID,
                    },
                    RelabelSpec::NONE,
                ));

                assert!(
                    matches!(
                        obs.decision,
                        QualPushdownDecision::Pushable {
                            contract: PushdownContract::ExactRowFilter,
                            costing: PushdownCosting::CostedPruning,
                        }
                    ),
                    "opno {} (type {:?}) with the literal on the left must still classify ExactRowFilter, got {:?}",
                    case.opno,
                    case.type_oid,
                    obs.decision,
                );

                let expected_op = mirror_operator_oracle(case.expected_op);
                let expected_datum = if is_int8 {
                    Datum::long(7)
                } else {
                    Datum::int(7)
                };
                let expected = Predicate::Binary(BinaryExpression::new(
                    expected_op,
                    Reference::new("col"),
                    expected_datum,
                ));
                assert_eq!(
                    obs.translated
                        .expect("literal op column must translate on unfixed code"),
                    expected,
                    "opno {} (type {:?}) with the literal on the left must mirror {:?} → {:?} while keeping the column reference and datum",
                    case.opno,
                    case.type_oid,
                    case.expected_op,
                    expected_op,
                );
            }
        }
    }

    /// `int4 = int4` must build the exact Iceberg reference/operator/datum payload.
    #[pgrx::pg_test(schema = "tests")]
    fn translator_int4_eq_builds_datum_int_predicate() {
        unsafe {
            let obs = PREDICATE_HARNESS.observe(&ComparisonSpec::scan_col_op_const(
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
            ));

            assert!(
                matches!(
                    obs.decision,
                    QualPushdownDecision::Pushable {
                        contract: PushdownContract::ExactRowFilter,
                        ..
                    }
                ),
                "int4 = int4 must classify ExactRowFilter, got {:?}",
                obs.decision,
            );

            let expected = Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                Reference::new("col"),
                Datum::int(7),
            ));
            assert_eq!(
                obs.translated
                    .expect("int4 = int4 must translate on unfixed code"),
                expected,
                "int4 = int4 must build a Datum::int equality predicate",
            );
        }
    }
}
