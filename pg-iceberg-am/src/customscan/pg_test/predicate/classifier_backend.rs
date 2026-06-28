//! Backend classifier coverage: deterministic matrix and raw PG-node reject cases.

#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;

    use crate::customscan::pg_test::support::classifier_harness::CLASSIFIER;
    use crate::customscan::pg_test::support::fixtures::{
        DATE_EQ_OPNO, FLOAT8EQ_OPNO, INT4EQ_OPNO, INT4LT_OPNO,
        NON_DEFAULT_COLLATION_OID, NUMERIC_EQ_OPNO, OUTER_RELID, SCAN_RELID,
        TEXT_LT_OPNO, TEXTEQ_OPNO, TEXTNE_OPNO, TIMESTAMP_EQ_OPNO, make_int4_const,
        make_int4_var, make_opexpr,
    };
    use crate::customscan::pg_test::support::predicate_harness::{
        ComparisonOpSpec, ComparisonSpec, ConstSpec, OperandSpec, PREDICATE_HARNESS,
        RelabelSpec, ScanColumnSpec,
    };
    use crate::predicate::policy::{
        PredicateCapability, PredicatePushdownPolicy,
    };
    use pg_lakebase_core::expr::nodes::PgComparisonOp;
    use pg_lakebase_core::expr::split::{
        PushdownContract, PushdownCosting, QualPushdownDecision,
    };
    use pgrx::pg_sys;

    fn supported_predicate(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonOp,
    ) -> PredicateCapability {
        PredicatePushdownPolicy::new().capability_for(type_oid, op_key)
    }

    fn is_value_sensitive_type(type_oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::new().is_value_sensitive_type(type_oid)
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum OperandCase {
        ScanCol,
        OuterCol,
        Const,
        ParamExtern,
        ParamExec,
        ScanSystemCol,
        OuterSystemCol,
        ScanWholeRow,
        OuterWholeRow,
        ParamSublink,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum OperandShape {
        ScanColumn,
        AcceptedNonColumn,
        Other,
    }

    #[derive(Clone, Copy, Debug)]
    struct TripleCase {
        label: &'static str,
        col_type: pg_sys::Oid,
        opno: u32,
        opcollid: u32,
        inputcollid: u32,
    }

    impl TripleCase {
        fn op(self, opfuncid: u32, opresulttype: pg_sys::Oid) -> ComparisonOpSpec {
            ComparisonOpSpec {
                opno: self.opno,
                opcollid: self.opcollid,
                inputcollid: self.inputcollid,
                opfuncid,
                opresulttype,
            }
        }

        fn oracle_op(self) -> PgComparisonOp {
            PgComparisonOp {
                opno: pg_sys::Oid::from(self.opno),
                opfuncid: pg_sys::Oid::INVALID,
                opresulttype: pg_sys::Oid::INVALID,
                opcollid: pg_sys::Oid::from(self.opcollid),
                inputcollid: pg_sys::Oid::from(self.inputcollid),
            }
        }
    }

    fn operand_shape(case: OperandCase) -> OperandShape {
        match case {
            OperandCase::ScanCol => OperandShape::ScanColumn,
            OperandCase::OuterCol
            | OperandCase::Const
            | OperandCase::ParamExtern
            | OperandCase::ParamExec => OperandShape::AcceptedNonColumn,
            OperandCase::ScanSystemCol
            | OperandCase::OuterSystemCol
            | OperandCase::ScanWholeRow
            | OperandCase::OuterWholeRow
            | OperandCase::ParamSublink => OperandShape::Other,
        }
    }

    fn has_param_or_outer(case: OperandCase) -> bool {
        matches!(
            case,
            OperandCase::OuterCol | OperandCase::ParamExtern | OperandCase::ParamExec
        )
    }

    fn operand_spec(case: OperandCase, triple: TripleCase) -> OperandSpec {
        match case {
            OperandCase::ScanCol => OperandSpec::ScanCol,
            OperandCase::OuterCol => OperandSpec::OuterCol,
            OperandCase::Const => OperandSpec::Const(const_spec(triple)),
            OperandCase::ParamExtern => OperandSpec::Param {
                kind: pg_sys::ParamKind::PARAM_EXTERN,
                id: 1,
                type_oid: triple.col_type,
                collation: triple.inputcollid,
            },
            OperandCase::ParamExec => OperandSpec::Param {
                kind: pg_sys::ParamKind::PARAM_EXEC,
                id: 2,
                type_oid: triple.col_type,
                collation: triple.inputcollid,
            },
            OperandCase::ScanSystemCol => {
                OperandSpec::SystemCol { relid: SCAN_RELID }
            }
            OperandCase::OuterSystemCol => {
                OperandSpec::SystemCol { relid: OUTER_RELID }
            }
            OperandCase::ScanWholeRow => OperandSpec::WholeRow { relid: SCAN_RELID },
            OperandCase::OuterWholeRow => {
                OperandSpec::WholeRow { relid: OUTER_RELID }
            }
            OperandCase::ParamSublink => OperandSpec::ParamSublink,
        }
    }

    fn const_spec(triple: TripleCase) -> ConstSpec {
        use pgrx::IntoDatum;
        use pgrx::prelude::{AnyNumeric, Date, Timestamp};

        match triple.col_type {
            pg_sys::INT4OID => ConstSpec {
                type_oid: pg_sys::INT4OID,
                collation: triple.inputcollid,
                len: core::mem::size_of::<i32>() as c_int,
                datum: pg_sys::Datum::from(7usize),
                byval: true,
            },
            pg_sys::NUMERICOID => ConstSpec {
                type_oid: pg_sys::NUMERICOID,
                collation: 0,
                len: -1,
                datum: AnyNumeric::try_from(100.5_f64)
                    .expect("100.5 is a valid numeric")
                    .into_datum()
                    .expect("numeric literal into_datum"),
                byval: false,
            },
            pg_sys::FLOAT8OID => ConstSpec {
                type_oid: pg_sys::FLOAT8OID,
                collation: 0,
                len: 8,
                datum: 1.5_f64.into_datum().expect("float8 literal into_datum"),
                byval: true,
            },
            pg_sys::TEXTOID => ConstSpec {
                type_oid: pg_sys::TEXTOID,
                collation: triple.inputcollid,
                len: -1,
                datum: "x".into_datum().expect("text literal into_datum"),
                byval: false,
            },
            pg_sys::DATEOID => ConstSpec {
                type_oid: pg_sys::DATEOID,
                collation: 0,
                len: 4,
                datum: Date::new(2024, 1, 1)
                    .expect("2024-01-01 is a valid date")
                    .into_datum()
                    .expect("date literal into_datum"),
                byval: true,
            },
            pg_sys::TIMESTAMPOID => ConstSpec {
                type_oid: pg_sys::TIMESTAMPOID,
                collation: 0,
                len: 8,
                datum: Timestamp::new(2024, 1, 1, 0, 0, 0.0)
                    .expect("2024-01-01 00:00:00 is a valid timestamp")
                    .into_datum()
                    .expect("timestamp literal into_datum"),
                byval: true,
            },
            other => unreachable!("no const fixture for type OID {other}"),
        }
    }

    fn spec_for(
        triple: TripleCase,
        lhs: OperandCase,
        rhs: OperandCase,
        relabel: RelabelSpec,
        opfuncid: u32,
        opresulttype: pg_sys::Oid,
    ) -> ComparisonSpec<'static> {
        ComparisonSpec::new(
            ScanColumnSpec::synthetic(triple.col_type, triple.inputcollid),
            operand_spec(lhs, triple),
            operand_spec(rhs, triple),
            triple.op(opfuncid, opresulttype),
            relabel,
        )
    }

    fn model_decision(
        lhs: OperandCase,
        rhs: OperandCase,
        triple: TripleCase,
    ) -> QualPushdownDecision {
        let pushable_shape = matches!(
            (operand_shape(lhs), operand_shape(rhs)),
            (OperandShape::ScanColumn, OperandShape::AcceptedNonColumn)
                | (OperandShape::AcceptedNonColumn, OperandShape::ScanColumn)
        );
        if !pushable_shape {
            return QualPushdownDecision::Unsupported;
        }

        match supported_predicate(triple.col_type, triple.oracle_op()) {
            PredicateCapability::ExactRowFilter => QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
            PredicateCapability::ConservativePruning => {
                let costing = if has_param_or_outer(lhs)
                    || has_param_or_outer(rhs)
                    || is_value_sensitive_type(triple.col_type)
                {
                    PushdownCosting::UncostedBestEffort
                } else {
                    PushdownCosting::CostedPruning
                };
                QualPushdownDecision::Pushable {
                    contract: PushdownContract::ConservativePruning,
                    costing,
                }
            }
            PredicateCapability::Unsupported => QualPushdownDecision::Unsupported,
        }
    }

    fn operand_cases() -> [OperandCase; 10] {
        [
            OperandCase::ScanCol,
            OperandCase::OuterCol,
            OperandCase::Const,
            OperandCase::ParamExtern,
            OperandCase::ParamExec,
            OperandCase::ScanSystemCol,
            OperandCase::OuterSystemCol,
            OperandCase::ScanWholeRow,
            OperandCase::OuterWholeRow,
            OperandCase::ParamSublink,
        ]
    }

    fn triple_cases() -> [TripleCase; 11] {
        let default_collation = u32::from(pg_sys::DEFAULT_COLLATION_OID);
        let c_collation = u32::from(pg_sys::C_COLLATION_OID);

        [
            TripleCase {
                label: "int4 = under (0,0)",
                col_type: pg_sys::INT4OID,
                opno: INT4EQ_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "int4 < under (0,0)",
                col_type: pg_sys::INT4OID,
                opno: INT4LT_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "numeric = under (0,0)",
                col_type: pg_sys::NUMERICOID,
                opno: NUMERIC_EQ_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "float8 = under (0,0)",
                col_type: pg_sys::FLOAT8OID,
                opno: FLOAT8EQ_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "text = under DEFAULT_COLLATION_OID",
                col_type: pg_sys::TEXTOID,
                opno: TEXTEQ_OPNO,
                opcollid: 0,
                inputcollid: default_collation,
            },
            TripleCase {
                label: "text = under unresolvable non-default collation",
                col_type: pg_sys::TEXTOID,
                opno: TEXTEQ_OPNO,
                opcollid: 0,
                inputcollid: NON_DEFAULT_COLLATION_OID,
            },
            TripleCase {
                label: "date = under (0,0)",
                col_type: pg_sys::DATEOID,
                opno: DATE_EQ_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "timestamp = under (0,0)",
                col_type: pg_sys::TIMESTAMPOID,
                opno: TIMESTAMP_EQ_OPNO,
                opcollid: 0,
                inputcollid: 0,
            },
            TripleCase {
                label: "text < under C collation",
                col_type: pg_sys::TEXTOID,
                opno: TEXT_LT_OPNO,
                opcollid: 0,
                inputcollid: c_collation,
            },
            TripleCase {
                label: "text <> under C collation",
                col_type: pg_sys::TEXTOID,
                opno: TEXTNE_OPNO,
                opcollid: 0,
                inputcollid: c_collation,
            },
            TripleCase {
                label: "int4 = with non-default collation tags",
                col_type: pg_sys::INT4OID,
                opno: INT4EQ_OPNO,
                opcollid: NON_DEFAULT_COLLATION_OID,
                inputcollid: NON_DEFAULT_COLLATION_OID,
            },
        ]
    }

    /// The backend classifier should match the shared oracle across accepted and
    /// rejected operand shapes, while remaining invariant to order, relabeling,
    /// and diagnostic `OpExpr` fields.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_shape_matrix_matches_oracle_and_invariants() {
        unsafe {
            for triple in triple_cases() {
                for lhs in operand_cases() {
                    for rhs in operand_cases() {
                        let expected = model_decision(lhs, rhs, triple);
                        let got = PREDICATE_HARNESS.classify(&spec_for(
                            triple,
                            lhs,
                            rhs,
                            RelabelSpec::NONE,
                            0,
                            pg_sys::BOOLOID,
                        ));
                        assert_eq!(
                            got, expected,
                            "decision mismatch for {:?} vs {:?} with {}",
                            lhs, rhs, triple.label,
                        );

                        let swapped = PREDICATE_HARNESS.classify(&spec_for(
                            triple,
                            rhs,
                            lhs,
                            RelabelSpec::NONE,
                            0,
                            pg_sys::BOOLOID,
                        ));
                        assert_eq!(
                            got, swapped,
                            "operand order changed the verdict for {:?} vs {:?} with {}",
                            lhs, rhs, triple.label,
                        );

                        let relabeled = PREDICATE_HARNESS.classify(&spec_for(
                            triple,
                            lhs,
                            rhs,
                            RelabelSpec {
                                lhs_depth: 1,
                                rhs_depth: 2,
                            },
                            0,
                            pg_sys::BOOLOID,
                        ));
                        assert_eq!(
                            got, relabeled,
                            "RelabelType wrappers changed the verdict for {:?} vs {:?} with {}",
                            lhs, rhs, triple.label,
                        );

                        let diagnostic = PREDICATE_HARNESS.classify(&spec_for(
                            triple,
                            lhs,
                            rhs,
                            RelabelSpec::NONE,
                            9_999,
                            pg_sys::INT4OID,
                        ));
                        assert_eq!(
                            got, diagnostic,
                            "diagnostic OpExpr fields changed the verdict for {:?} vs {:?} with {}",
                            lhs, rhs, triple.label,
                        );
                    }
                }

                let outer = PREDICATE_HARNESS.classify(&spec_for(
                    triple,
                    OperandCase::ScanCol,
                    OperandCase::OuterCol,
                    RelabelSpec::NONE,
                    0,
                    pg_sys::BOOLOID,
                ));
                let param = PREDICATE_HARNESS.classify(&spec_for(
                    triple,
                    OperandCase::ScanCol,
                    OperandCase::ParamExec,
                    RelabelSpec::NONE,
                    0,
                    pg_sys::BOOLOID,
                ));
                assert_eq!(
                    outer, param,
                    "outer column and supported param must classify identically for {}",
                    triple.label,
                );
            }
        }
    }

    /// Unary `OpExpr` never reaches the comparison classifier path.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_rejects_unary_opexpr() {
        unsafe {
            let var = make_int4_var(SCAN_RELID, 1);
            let expr = make_opexpr(558, pg_sys::INT4OID, 0, 0, &[var]);

            match CLASSIFIER.classify(expr) {
                QualPushdownDecision::Unsupported => {}
                other => {
                    panic!("expected Unsupported for unary OpExpr, got {other:?}")
                }
            }
        }
    }

    /// Nested operator arguments are outside the v1 classifier's accepted leaf shape.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_rejects_nested_opexpr_argument() {
        unsafe {
            let inner_var = make_int4_var(SCAN_RELID, 1);
            let inner_const = make_int4_const(1);
            let inner =
                make_opexpr(551, pg_sys::INT4OID, 0, 0, &[inner_var, inner_const]);
            let outer_const = make_int4_const(2);
            let expr = make_opexpr(
                INT4EQ_OPNO,
                pg_sys::BOOLOID,
                0,
                0,
                &[inner, outer_const],
            );

            match CLASSIFIER.classify(expr) {
                QualPushdownDecision::Unsupported => {}
                other => panic!(
                    "expected Unsupported for `(a + 1) = 2` nested OpExpr shape, got {other:?}"
                ),
            }
        }
    }

    /// `NullTest` on a scan column with a supported type is pushable as `ExactRowFilter`.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_pushes_null_test_on_supported_type() {
        unsafe {
            let var = make_int4_var(SCAN_RELID, 1);
            let nt = pg_sys::palloc0(core::mem::size_of::<pg_sys::NullTest>())
                as *mut pg_sys::NullTest;
            (*nt).xpr.type_ = pg_sys::NodeTag::T_NullTest;
            (*nt).arg = var.cast();
            (*nt).nulltesttype = pg_sys::NullTestType::IS_NULL;
            (*nt).argisrow = false;
            (*nt).location = -1;

            match CLASSIFIER.classify(nt.cast()) {
                QualPushdownDecision::Pushable { contract, costing } => {
                    assert_eq!(
                        contract,
                        PushdownContract::ExactRowFilter,
                        "IS NULL on int4 scan column must be ExactRowFilter",
                    );
                    assert_eq!(
                        costing,
                        PushdownCosting::CostedPruning,
                        "IS NULL on int4 scan column must be CostedPruning",
                    );
                }
                other => panic!(
                    "expected Pushable for IS NULL on int4 scan column, got {other:?}"
                ),
            }
        }
    }

    /// Float IS NULL is pushable although float comparisons are unsupported because
    /// null tests only inspect the null bitmap — no value comparison involved.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_pushes_null_test_on_float_regardless_of_toggle() {
        unsafe {
            // float8 Var at (SCAN_RELID, attno=2)
            let var = pg_sys::makeVar(
                SCAN_RELID as core::ffi::c_int,
                2,
                pg_sys::FLOAT8OID,
                -1,
                pg_sys::Oid::INVALID,
                0,
            );
            let nt = pg_sys::palloc0(core::mem::size_of::<pg_sys::NullTest>())
                as *mut pg_sys::NullTest;
            (*nt).xpr.type_ = pg_sys::NodeTag::T_NullTest;
            (*nt).arg = var.cast();
            (*nt).nulltesttype = pg_sys::NullTestType::IS_NOT_NULL;
            (*nt).argisrow = false;
            (*nt).location = -1;

            match CLASSIFIER.classify(nt.cast()) {
                QualPushdownDecision::Pushable { contract, costing } => {
                    assert_eq!(
                        contract,
                        PushdownContract::ExactRowFilter,
                        "IS NOT NULL on float8 scan column must be ExactRowFilter \
                         (null tests are independent of comparison support)",
                    );
                    assert_eq!(
                        costing,
                        PushdownCosting::CostedPruning,
                        "IS NOT NULL on float8 scan column must be CostedPruning",
                    );
                }
                other => panic!(
                    "expected Pushable for IS NOT NULL on float8 scan column, got {other:?}"
                ),
            }
        }
    }

    /// IS NULL on an unsupported type (bool) must remain Unsupported.
    #[pgrx::pg_test(schema = "tests")]
    fn classifier_rejects_null_test_on_unsupported_type() {
        unsafe {
            // bool Var at (SCAN_RELID, attno=3)
            let var = pg_sys::makeVar(
                SCAN_RELID as core::ffi::c_int,
                3,
                pg_sys::BOOLOID,
                -1,
                pg_sys::Oid::INVALID,
                0,
            );
            let nt = pg_sys::palloc0(core::mem::size_of::<pg_sys::NullTest>())
                as *mut pg_sys::NullTest;
            (*nt).xpr.type_ = pg_sys::NodeTag::T_NullTest;
            (*nt).arg = var.cast();
            (*nt).nulltesttype = pg_sys::NullTestType::IS_NULL;
            (*nt).argisrow = false;
            (*nt).location = -1;

            match CLASSIFIER.classify(nt.cast()) {
                QualPushdownDecision::Unsupported => {}
                other => panic!(
                    "expected Unsupported for IS NULL on bool column, got {other:?}"
                ),
            }
        }
    }
}
