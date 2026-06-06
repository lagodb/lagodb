//! Backend tests for the value-comparison capability matrix.
//!
//! [`PredicatePushdownPolicy::capability_for`] references
//! `get_collation_isdeterministic` in its `text` arm, so even the integer /
//! numeric / temporal / float rows (which never reach that syscache lookup)
//! pull the symbol into the link and cannot run as host `#[test]`s (see
//! `docs/testing.md`). The pure `op_class` mapping and `null_test_capability`
//! stay as host tests in `predicate_pushdown_policy.rs`; the `text` collation
//! semantics live in `capability_backend.rs`. This module owns the
//! type×operator×collation verdict matrix for the non-text categories plus the
//! integer collation gate.

#[pgrx::pg_schema]
mod tests {
    use pg_lakebase_core::expr::nodes::PgComparisonOp;
    use pgrx::pg_sys;
    use pgrx::pg_sys::Oid;

    use crate::customscan::{
        ComparisonOpClass, FLOAT_PUSHDOWN_ENABLED,
        NUMERIC_COMPARISON_PUSHDOWN_ENABLED, PredicateCapability,
        PredicatePushdownPolicy,
    };
    // Single source of truth for the comparison opno verdict table, shared with
    // the host `#[test]` policy suite in `predicate_pushdown_policy.rs`. `op`
    // exposes the per-type rows (`op::INT4`, `op::TEXT`, ...).
    use crate::customscan::predicate_pushdown_policy::test_opno_table as op;
    use crate::customscan::predicate_pushdown_policy::test_opno_table::{
        CLASS_BY_COLUMN, opno_table,
    };

    fn supported_predicate(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonOp,
    ) -> PredicateCapability {
        PredicatePushdownPolicy::new().capability_for(type_oid, op_key)
    }

    /// Build a collation-free `(opno, 0, 0)` comparison triple.
    fn triple(opno: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: Oid::INVALID,
        }
    }

    /// Build a comparison triple carrying a non-zero collation on both slots.
    fn triple_with_collation(opno: u32, collid: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::from(collid),
            inputcollid: Oid::from(collid),
        }
    }

    /// int4/int8 comparison set that must stay `ExactRowFilter` under `(0, 0)`.
    const INTEGER_EXACT_PRESERVATION_SET: &[(pg_sys::Oid, u32)] = &[
        (pg_sys::INT4OID, op::INT4[0]),
        (pg_sys::INT4OID, op::INT4[1]),
        (pg_sys::INT4OID, op::INT4[2]),
        (pg_sys::INT4OID, op::INT4[3]),
        (pg_sys::INT4OID, op::INT4[4]),
        (pg_sys::INT4OID, op::INT4[5]),
        (pg_sys::INT8OID, op::INT8[0]),
        (pg_sys::INT8OID, op::INT8[1]),
        (pg_sys::INT8OID, op::INT8[2]),
        (pg_sys::INT8OID, op::INT8[3]),
        (pg_sys::INT8OID, op::INT8[4]),
        (pg_sys::INT8OID, op::INT8[5]),
    ];

    /// Integers are `Exact` for all six op-classes under the collation-free
    /// `(0, 0)` triple.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_integers_are_exact_under_zero_collation() {
        for &type_oid in &[pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID] {
            let table = opno_table();
            let row = table
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("integer type present in table");
            for &opno in &row.1 {
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    PredicateCapability::ExactRowFilter,
                    "integer type {} opno {opno} must be ExactRowFilter under (0,0)",
                    u32::from(type_oid),
                );
            }
        }
    }

    /// Any non-zero collation on either `opcollid` or `inputcollid` makes an
    /// integer comparison `Unsupported`.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_integer_single_slot_collation_is_unsupported() {
        let collid = Oid::from(50_000u32);

        let int4eq = triple(op::INT4[0]);
        let input_only = PgComparisonOp {
            inputcollid: collid,
            ..int4eq
        };
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, input_only),
            PredicateCapability::Unsupported,
            "non-zero inputcollid alone must make int4eq Unsupported",
        );
        let opcoll_only = PgComparisonOp {
            opcollid: collid,
            ..int4eq
        };
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, opcoll_only),
            PredicateCapability::Unsupported,
            "non-zero opcollid alone must make int4eq Unsupported",
        );

        let int8lt = triple(op::INT8[2]);
        let input_only = PgComparisonOp {
            inputcollid: collid,
            ..int8lt
        };
        assert_eq!(
            supported_predicate(pg_sys::INT8OID, input_only),
            PredicateCapability::Unsupported,
        );
        let opcoll_only = PgComparisonOp {
            opcollid: collid,
            ..int8lt
        };
        assert_eq!(
            supported_predicate(pg_sys::INT8OID, opcoll_only),
            PredicateCapability::Unsupported,
        );
    }

    /// Both collation slots non-zero likewise rejects integer pushdown.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_integer_with_collation_is_unsupported() {
        assert_eq!(
            supported_predicate(
                pg_sys::INT4OID,
                triple_with_collation(op::INT4[0], 50_000),
            ),
            PredicateCapability::Unsupported,
        );
        assert_eq!(
            supported_predicate(
                pg_sys::INT8OID,
                triple_with_collation(op::INT8[2], 50_000),
            ),
            PredicateCapability::Unsupported,
        );
    }

    /// Every int4/int8 comparison stays `Exact` under `(0, 0)`.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_integer_exact_preservation_set_is_exact() {
        assert!(!INTEGER_EXACT_PRESERVATION_SET.is_empty());
        for &(type_oid, opno) in INTEGER_EXACT_PRESERVATION_SET {
            assert_eq!(
                supported_predicate(type_oid, triple(opno)),
                PredicateCapability::ExactRowFilter,
                "integer preservation (type {}, opno {opno}) must stay ExactRowFilter under (0,0)",
                u32::from(type_oid),
            );
        }
    }

    /// Capability key uses `(type_oid, opno, opcollid, inputcollid)` only;
    /// `opfuncid` / `opresulttype` are diagnostic metadata and must not change
    /// the verdict.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_ignores_diagnostic_fields() {
        let text_eq_c = PgComparisonOp {
            opno: Oid::from(op::TEXT[0]),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: pg_sys::C_COLLATION_OID,
        };

        let baselines: &[(pg_sys::Oid, PgComparisonOp)] = &[
            (pg_sys::INT4OID, triple(op::INT4[0])),
            (pg_sys::INT4OID, triple(op::INT4[2])),
            (pg_sys::INT4OID, triple_with_collation(op::INT4[0], 50_000)),
            (pg_sys::NUMERICOID, triple(op::NUMERIC[0])),
            (pg_sys::TEXTOID, text_eq_c),
            (
                pg_sys::TEXTOID,
                PgComparisonOp {
                    opno: Oid::from(op::TEXT[1]),
                    opfuncid: Oid::INVALID,
                    opresulttype: Oid::INVALID,
                    opcollid: Oid::INVALID,
                    inputcollid: pg_sys::C_COLLATION_OID,
                },
            ),
        ];

        for &(type_oid, baseline) in baselines {
            let expected = supported_predicate(type_oid, baseline);

            let planner_like = PgComparisonOp {
                opfuncid: Oid::from(65u32),
                opresulttype: pg_sys::BOOLOID,
                ..baseline
            };
            assert_eq!(
                supported_predicate(type_oid, planner_like),
                expected,
                "planner-like diagnostic OIDs must not change verdict for type {} opno {}",
                u32::from(type_oid),
                u32::from(baseline.opno),
            );

            let arbitrary = PgComparisonOp {
                opfuncid: Oid::from(9_999u32),
                opresulttype: Oid::from(9_999u32),
                ..baseline
            };
            assert_eq!(
                supported_predicate(type_oid, arbitrary),
                expected,
                "arbitrary diagnostic OIDs must not change verdict for type {} opno {}",
                u32::from(type_oid),
                u32::from(baseline.opno),
            );
        }
    }

    /// Temporal types are `ConservativePruning` for `Eq` and the four ordered
    /// classes, `Unsupported` for `<>`. Numeric follows
    /// `NUMERIC_COMPARISON_PUSHDOWN_ENABLED`, float follows
    /// `FLOAT_PUSHDOWN_ENABLED`.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_numeric_temporal_float_matrix() {
        let table = opno_table();
        let temporal = [
            pg_sys::DATEOID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
        ];
        for &type_oid in &temporal {
            let row = table
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("type present in table");
            for (col, &opno) in row.1.iter().enumerate() {
                let class = CLASS_BY_COLUMN[col];
                let expected = if class == ComparisonOpClass::NotEq {
                    PredicateCapability::Unsupported
                } else {
                    PredicateCapability::ConservativePruning
                };
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    expected,
                    "type {} class {class:?} (opno {opno}) mismatch",
                    u32::from(type_oid),
                );
            }
        }

        let numeric_row = table
            .iter()
            .find(|(t, _)| *t == pg_sys::NUMERICOID)
            .expect("numeric present in table");
        for (col, &opno) in numeric_row.1.iter().enumerate() {
            let expected = if NUMERIC_COMPARISON_PUSHDOWN_ENABLED {
                let class = CLASS_BY_COLUMN[col];
                if class == ComparisonOpClass::NotEq {
                    PredicateCapability::Unsupported
                } else {
                    PredicateCapability::ConservativePruning
                }
            } else {
                PredicateCapability::Unsupported
            };
            assert_eq!(
                supported_predicate(pg_sys::NUMERICOID, triple(opno)),
                expected,
                "numeric opno {opno} mismatch (NUMERIC_COMPARISON_PUSHDOWN_ENABLED={NUMERIC_COMPARISON_PUSHDOWN_ENABLED})",
            );
        }

        let float_types = [pg_sys::FLOAT4OID, pg_sys::FLOAT8OID];
        for &type_oid in &float_types {
            let row = table
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("type present in table");
            for (col, &opno) in row.1.iter().enumerate() {
                let expected = if FLOAT_PUSHDOWN_ENABLED {
                    let class = CLASS_BY_COLUMN[col];
                    if class == ComparisonOpClass::NotEq {
                        PredicateCapability::Unsupported
                    } else {
                        PredicateCapability::ConservativePruning
                    }
                } else {
                    PredicateCapability::Unsupported
                };
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    expected,
                    "float type {} opno {opno} mismatch (FLOAT_PUSHDOWN_ENABLED={FLOAT_PUSHDOWN_ENABLED})",
                    u32::from(type_oid),
                );
            }
        }
    }

    /// Only the integer set is ever `Exact`; other value types never are.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_only_integers_are_exact() {
        for (type_oid, opnos) in opno_table() {
            let is_integer = matches!(
                type_oid,
                t if t == pg_sys::INT2OID
                    || t == pg_sys::INT4OID
                    || t == pg_sys::INT8OID
            );
            if is_integer {
                continue;
            }
            for opno in opnos {
                assert_ne!(
                    supported_predicate(type_oid, triple(opno)),
                    PredicateCapability::ExactRowFilter,
                    "non-integer type {} opno {opno} must not be ExactRowFilter",
                    u32::from(type_oid),
                );
            }
        }
    }

    /// An unknown type OID is always `Unsupported`, regardless of operator.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_unknown_type_is_unsupported() {
        assert_eq!(
            supported_predicate(pg_sys::BOOLOID, triple(op::INT4[0])),
            PredicateCapability::Unsupported,
        );
        assert_eq!(
            supported_predicate(pg_sys::BYTEAOID, triple(op::INT4[0])),
            PredicateCapability::Unsupported,
        );
    }

    /// An unrecognized operator makes any clause `Unsupported`.
    #[pgrx::pg_test(schema = "tests")]
    fn supported_predicate_unknown_operator_is_unsupported() {
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, triple(558)),
            PredicateCapability::Unsupported,
            "unmapped opno 558 must be Unsupported even for int4",
        );
    }
}
