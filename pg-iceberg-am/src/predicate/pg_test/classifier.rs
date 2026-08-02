//! Backend smoke tests for raw PG-node parsing into the pure classifier.

#[pgrx::pg_schema]
mod tests {
    use core::mem;

    use pg_lakebase_core::expr::{
        PushdownContract, PushdownCosting, QualPushdownDecision,
    };
    use pgrx::pg_sys;

    use crate::predicate::pg_test::harness::{
        CLASSIFIER, ComparisonOpSpec, ComparisonSpec, ConstSpec, INT4EQ_OPNO,
        PREDICATE_HARNESS, RelabelSpec, SCAN_RELID, ScanColumnSpec, make_int4_const,
        make_int4_var, make_opexpr,
    };

    #[pgrx::pg_test(schema = "tests")]
    fn classifier_parses_relabelled_binary_comparison() {
        let spec = ComparisonSpec {
            relabel: RelabelSpec {
                lhs_depth: 1,
                rhs_depth: 2,
            },
            ..ComparisonSpec::scan_col_op_const(
                ScanColumnSpec::synthetic(pg_sys::INT4OID, 0),
                ComparisonOpSpec {
                    opno: INT4EQ_OPNO,
                    opcollid: 0,
                    inputcollid: 0,
                    opfuncid: 9_999,
                    opresulttype: pg_sys::INT4OID,
                },
                ConstSpec {
                    type_oid: pg_sys::INT4OID,
                    collation: 0,
                    len: mem::size_of::<i32>() as _,
                    datum: pg_sys::Datum::from(7usize),
                    byval: true,
                },
            )
        };

        assert_eq!(
            unsafe { PREDICATE_HARNESS.classify(&spec) },
            QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn classifier_rejects_non_binary_and_nested_opexpr() {
        unsafe {
            let unary = make_opexpr(
                558,
                pg_sys::INT4OID,
                0,
                0,
                &[make_int4_var(SCAN_RELID, 1)],
            );
            assert_eq!(
                CLASSIFIER.classify(unary),
                QualPushdownDecision::Unsupported,
            );

            let inner = make_opexpr(
                551,
                pg_sys::INT4OID,
                0,
                0,
                &[make_int4_var(SCAN_RELID, 1), make_int4_const(1)],
            );
            let nested = make_opexpr(
                INT4EQ_OPNO,
                pg_sys::BOOLOID,
                0,
                0,
                &[inner, make_int4_const(2)],
            );
            assert_eq!(
                CLASSIFIER.classify(nested),
                QualPushdownDecision::Unsupported,
            );
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn classifier_parses_column_null_test() {
        unsafe {
            let null_test = pg_sys::palloc0(mem::size_of::<pg_sys::NullTest>())
                as *mut pg_sys::NullTest;
            (*null_test).xpr.type_ = pg_sys::NodeTag::T_NullTest;
            (*null_test).arg = make_int4_var(SCAN_RELID, 1).cast();
            (*null_test).nulltesttype = pg_sys::NullTestType::IS_NULL;
            (*null_test).argisrow = false;
            (*null_test).location = -1;

            assert_eq!(
                CLASSIFIER.classify(null_test.cast()),
                QualPushdownDecision::Pushable {
                    contract: PushdownContract::ExactRowFilter,
                    costing: PushdownCosting::CostedPruning,
                },
            );
        }
    }
}
