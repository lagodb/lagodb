//! Host-only tests for policy logic that does not reference backend symbols.

use std::collections::HashSet;

use pgrx::pg_sys;
use pgrx::pg_sys::Oid;

use super::test_opno_table::{CLASS_BY_COLUMN, opno_table};
use super::{ComparisonOpClass, PredicatePushdownPolicy};

fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
    PredicatePushdownPolicy::op_class(opno)
}

fn is_c_or_posix_collation(oid: pg_sys::Oid) -> bool {
    PredicatePushdownPolicy::is_c_or_posix_collation(oid)
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
fn is_c_or_posix_collation_only_for_c_and_posix() {
    assert!(is_c_or_posix_collation(pg_sys::C_COLLATION_OID));
    assert!(is_c_or_posix_collation(pg_sys::POSIX_COLLATION_OID));
    assert_eq!(u32::from(pg_sys::C_COLLATION_OID), 950);
    assert_eq!(u32::from(pg_sys::POSIX_COLLATION_OID), 951);

    assert!(!is_c_or_posix_collation(pg_sys::Oid::INVALID));
    assert!(!is_c_or_posix_collation(pg_sys::DEFAULT_COLLATION_OID));
    assert!(!is_c_or_posix_collation(Oid::from(50_000u32)));
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
