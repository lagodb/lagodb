//! Host-only tests for policy logic that does not reference backend symbols.

// Shared comparison-operator fixture data for the policy host tests.
pub(crate) mod host_matrix {
    use lagodb_core::expr::PgComparisonSignature;
    use pgrx::pg_sys;

    use crate::engine::predicate::policy::ComparisonOpClass;

    pub(crate) const CLASS_BY_COLUMN: [ComparisonOpClass; 6] = [
        ComparisonOpClass::Equal,
        ComparisonOpClass::NotEqual,
        ComparisonOpClass::Less,
        ComparisonOpClass::LessEqual,
        ComparisonOpClass::Greater,
        ComparisonOpClass::GreaterEqual,
    ];

    pub(crate) fn operator_row(type_oid: pg_sys::Oid) -> [u32; 6] {
        CLASS_BY_COLUMN.map(|kind| {
            u32::from(
                PgComparisonSignature::for_types(type_oid, type_oid, kind)
                    .expect("host fixture type has a built-in comparison family")
                    .operator_oid(),
            )
        })
    }

    pub(crate) fn int4() -> [u32; 6] {
        operator_row(pg_sys::INT4OID)
    }

    pub(crate) fn int8() -> [u32; 6] {
        operator_row(pg_sys::INT8OID)
    }

    pub(crate) fn text() -> [u32; 6] {
        operator_row(pg_sys::TEXTOID)
    }

    /// Provider test matrix derived from the core PostgreSQL signature table.
    pub(crate) fn opno_table() -> [(pg_sys::Oid, [u32; 6]); 7] {
        [
            (pg_sys::INT2OID, operator_row(pg_sys::INT2OID)),
            (pg_sys::INT4OID, int4()),
            (pg_sys::INT8OID, int8()),
            (pg_sys::DATEOID, operator_row(pg_sys::DATEOID)),
            (pg_sys::TIMESTAMPOID, operator_row(pg_sys::TIMESTAMPOID)),
            (pg_sys::TIMESTAMPTZOID, operator_row(pg_sys::TIMESTAMPTZOID)),
            (pg_sys::TEXTOID, text()),
        ]
    }
}

use std::{array, collections::HashSet};

use lagodb_core::expr::PgComparisonOp;
use lagodb_core::expr::{ColumnRef, ExprType, RuntimeValueSource, RuntimeValueSpec};
use pgrx::pg_sys;
use pgrx::pg_sys::Oid;
use proptest::prelude::*;

use self::host_matrix::{self as op, CLASS_BY_COLUMN, opno_table};
use super::{
    CollationSemantics, ComparisonOpClass, PgPredicatePushdownPolicy,
    PredicatePushdownPolicy, SupportedPredicateCapability,
};
use crate::engine::predicate::plan::PlannedValueType;

fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
    PredicatePushdownPolicy::op_class(opno)
}

fn triple(opno: u32) -> PgComparisonOp {
    PgComparisonOp {
        opno: Oid::from(opno),
        opfuncid: Oid::INVALID,
        opresulttype: Oid::INVALID,
        opcollid: Oid::INVALID,
        inputcollid: Oid::INVALID,
    }
}

fn capability(
    type_oid: pg_sys::Oid,
    op: PgComparisonOp,
    collation: CollationSemantics,
) -> Option<SupportedPredicateCapability> {
    let class = PredicatePushdownPolicy::op_class(op.opno)?;
    PredicatePushdownPolicy::capability_for_class(
        type_oid,
        op.identity(),
        collation,
        class,
    )
}

fn metadata(type_oid: pg_sys::Oid) -> ExprType {
    ExprType {
        type_oid,
        typmod: -1,
        collation: pg_sys::Oid::INVALID,
    }
}

fn planned_value_type(
    declared: pg_sys::Oid,
    column_effective: pg_sys::Oid,
    value_effective: pg_sys::Oid,
) -> Option<PlannedValueType> {
    PgPredicatePushdownPolicy::planned_value_type(
        &ColumnRef {
            scan: lagodb_core::query_contract::ScanId::from_index(0),
            attno: 1,
            declared_type: metadata(declared),
            value_type: metadata(column_effective),
        },
        &RuntimeValueSpec {
            value_type: metadata(value_effective),
            source_kind: RuntimeValueSource::OuterValue,
        },
    )
}

#[test]
fn planned_decoder_requires_a_total_column_value_type_combination() {
    assert_eq!(
        planned_value_type(pg_sys::INT4OID, pg_sys::INT4OID, pg_sys::INT4OID),
        Some(PlannedValueType::Int4),
    );
    assert_eq!(
        planned_value_type(pg_sys::INT4OID, pg_sys::INT4OID, pg_sys::OIDOID),
        None,
        "an unsupported value slot must remain residual",
    );
    assert_eq!(
        planned_value_type(pg_sys::VARCHAROID, pg_sys::TEXTOID, pg_sys::TEXTOID),
        Some(PlannedValueType::String),
        "a binary-compatible varchar-to-text operand remains pushable",
    );
}

#[test]
fn op_class_maps_every_known_opno() {
    for (type_oid, opnos) in opno_table() {
        for (column, &opno) in opnos.iter().enumerate() {
            assert_eq!(
                op_class(Oid::from(opno)),
                Some(CLASS_BY_COLUMN[column]),
                "opno {opno} (type {}, column {column}) must map to {:?}",
                u32::from(type_oid),
                CLASS_BY_COLUMN[column],
            );
        }
    }
}

#[test]
fn op_class_table_has_no_duplicate_opnos() {
    let mut seen = HashSet::new();
    for (_, opnos) in opno_table() {
        for opno in opnos {
            assert!(seen.insert(opno), "opno {opno} appears twice in the table");
        }
    }
}

#[test]
fn op_class_rejects_unknown_opnos() {
    assert_eq!(
        op_class(Oid::from(558u32)),
        None,
        "oidvector <> is not mapped",
    );
    assert_eq!(op_class(Oid::INVALID), None, "InvalidOid is not mapped");
    assert_eq!(
        op_class(Oid::from(9_999_999u32)),
        None,
        "unused OID is not mapped",
    );
}

#[test]
fn supported_predicate_integers_are_exact_under_zero_collation() {
    for (type_oid, opnos) in opno_table().into_iter().take(3) {
        for opno in opnos {
            assert_eq!(
                capability(type_oid, triple(opno), CollationSemantics::None),
                Some(SupportedPredicateCapability::Exact),
                "integer type {} opno {opno} must be exact",
                u32::from(type_oid),
            );
        }
    }
}

fn integer_exact_set() -> [(pg_sys::Oid, u32); 12] {
    let int4 = op::int4();
    let int8 = op::int8();
    array::from_fn(|index| {
        if index < int4.len() {
            (pg_sys::INT4OID, int4[index])
        } else {
            (pg_sys::INT8OID, int8[index - int4.len()])
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn supported_predicate_integer_collation_gate_property(
        idx in 0usize..12,
        collid in 1u32..=u32::MAX,
        tag_input in any::<bool>(),
    ) {
        let (type_oid, opno) = integer_exact_set()[idx];
        let mut tagged = triple(opno);
        if tag_input {
            tagged.inputcollid = Oid::from(collid);
        } else {
            tagged.opcollid = Oid::from(collid);
        }
        prop_assert_eq!(
            capability(type_oid, tagged, CollationSemantics::Deterministic),
            None,
        );
    }
}

#[test]
fn supported_predicate_ignores_diagnostic_fields() {
    let int4 = op::int4();
    let numeric = op::operator_row(pg_sys::NUMERICOID);
    let text = op::text();
    let mut tagged_integer = triple(int4[0]);
    tagged_integer.opcollid = Oid::from(50_000u32);
    let cases = [
        (pg_sys::INT4OID, triple(int4[0]), CollationSemantics::None),
        (pg_sys::INT4OID, triple(int4[2]), CollationSemantics::None),
        (pg_sys::INT4OID, tagged_integer, CollationSemantics::None),
        (
            pg_sys::NUMERICOID,
            triple(numeric[0]),
            CollationSemantics::None,
        ),
        (
            pg_sys::TEXTOID,
            triple(text[0]),
            CollationSemantics::COrPosix,
        ),
        (
            pg_sys::TEXTOID,
            triple(text[1]),
            CollationSemantics::COrPosix,
        ),
    ];

    for (type_oid, baseline, collation) in cases {
        let expected = capability(type_oid, baseline, collation);
        for (opfuncid, opresulttype) in [
            (Oid::from(65u32), pg_sys::BOOLOID),
            (Oid::from(9_999u32), Oid::from(9_999u32)),
        ] {
            assert_eq!(
                capability(
                    type_oid,
                    PgComparisonOp {
                        opfuncid,
                        opresulttype,
                        ..baseline
                    },
                    collation,
                ),
                expected,
            );
        }
    }
}

#[test]
fn supported_predicate_numeric_temporal_float_matrix() {
    for (type_oid, opnos) in opno_table().into_iter().skip(3).take(3) {
        for (column, opno) in opnos.into_iter().enumerate() {
            let expected = if CLASS_BY_COLUMN[column] == ComparisonOpClass::NotEqual {
                None
            } else {
                Some(SupportedPredicateCapability::Conservative)
            };
            assert_eq!(
                capability(type_oid, triple(opno), CollationSemantics::None),
                expected,
            );
        }
    }

    for opno in op::operator_row(pg_sys::NUMERICOID) {
        assert_eq!(
            capability(pg_sys::NUMERICOID, triple(opno), CollationSemantics::None),
            None,
        );
    }
    for (type_oid, opnos) in [
        (pg_sys::FLOAT4OID, [620, 621, 622, 624, 623, 625]),
        (pg_sys::FLOAT8OID, [670, 671, 672, 673, 674, 675]),
    ] {
        for opno in opnos {
            assert_eq!(
                capability(type_oid, triple(opno), CollationSemantics::None),
                None,
            );
        }
    }
}

#[test]
fn supported_predicate_only_integers_are_exact() {
    for (type_oid, opnos) in opno_table().into_iter().skip(3) {
        let collation = if type_oid == pg_sys::TEXTOID {
            CollationSemantics::COrPosix
        } else {
            CollationSemantics::None
        };
        for opno in opnos {
            assert_ne!(
                capability(type_oid, triple(opno), collation),
                Some(SupportedPredicateCapability::Exact),
            );
        }
    }
}

#[test]
fn supported_predicate_unknown_inputs_are_unsupported() {
    let int4 = op::int4();
    for type_oid in [pg_sys::BOOLOID, pg_sys::BYTEAOID] {
        assert_eq!(
            capability(type_oid, triple(int4[0]), CollationSemantics::None),
            None,
        );
    }
    assert_eq!(
        capability(pg_sys::INT4OID, triple(558), CollationSemantics::None),
        None,
    );
}

#[test]
fn text_capability_depends_only_on_resolved_collation_semantics() {
    let text = op::text();
    for type_oid in [pg_sys::TEXTOID, pg_sys::VARCHAROID] {
        for semantics in [
            CollationSemantics::COrPosix,
            CollationSemantics::Deterministic,
        ] {
            assert_eq!(
                capability(type_oid, triple(text[0]), semantics),
                Some(SupportedPredicateCapability::Conservative),
            );
        }
        for semantics in [
            CollationSemantics::None,
            CollationSemantics::NonDeterministic,
        ] {
            assert_eq!(capability(type_oid, triple(text[0]), semantics), None,);
        }
        for opno in text[2..].iter().copied() {
            assert_eq!(
                capability(type_oid, triple(opno), CollationSemantics::COrPosix),
                Some(SupportedPredicateCapability::Conservative),
            );
            assert_eq!(
                capability(type_oid, triple(opno), CollationSemantics::Deterministic),
                None,
            );
        }
        assert_eq!(
            capability(type_oid, triple(text[1]), CollationSemantics::COrPosix),
            None,
        );
    }
}

#[test]
fn null_tests_admit_supported_types_including_float() {
    for type_oid in [
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::NUMERICOID,
        pg_sys::DATEOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TEXTOID,
        pg_sys::VARCHAROID,
        pg_sys::FLOAT4OID,
        pg_sys::FLOAT8OID,
    ] {
        assert!(
            PredicatePushdownPolicy::supports_null_test(type_oid),
            "null tests must be supported for type {}",
            u32::from(type_oid),
        );
    }
}

#[test]
fn null_tests_reject_unsupported_types() {
    for type_oid in [pg_sys::BOOLOID, pg_sys::BYTEAOID, Oid::from(9_999_999u32)] {
        assert!(
            !PredicatePushdownPolicy::supports_null_test(type_oid),
            "null tests must be unsupported for type {}",
            u32::from(type_oid),
        );
    }
}
