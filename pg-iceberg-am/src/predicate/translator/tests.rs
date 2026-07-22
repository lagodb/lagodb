use super::*;
use pgrx::pg_sys::Oid;

const INT4_TYPE_OID: u32 = 23;

fn map_comparison_operator(
    op: PgComparisonOp,
) -> Result<PredicateOperator, IcebergTranslationError> {
    IcebergPredicateTranslator::new_unbound_for_tests().map_comparison_operator(op)
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
fn maps_supported_non_integer_operators() {
    assert_eq!(
        map_comparison_operator(op_triple(1098)).unwrap(),
        PredicateOperator::GreaterThanOrEq,
    );
    assert_eq!(
        map_comparison_operator(op_triple(98)).unwrap(),
        PredicateOperator::Eq,
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
    let mut t = IcebergPredicateTranslator::new_unbound_for_tests();
    assert!(matches!(
        t.is_null(null_scalar(INT4_TYPE_OID)),
        Err(IcebergTranslationError::NullTestOnNonColumn)
    ));
}

#[test]
fn is_not_null_with_null_scalar_fails_closed() {
    let mut t = IcebergPredicateTranslator::new_unbound_for_tests();
    assert!(matches!(
        t.is_not_null(null_scalar(INT4_TYPE_OID)),
        Err(IcebergTranslationError::NullTestOnNonColumn)
    ));
}
