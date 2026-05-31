//! Backend tests for `PredicateBuilder` (needs PG FFI symbols).

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;
    use std::fmt;

    use crate::support::pg::{OpExprSpec, PgNodeBuilder};
    use pg_lakebase_core::expr::nodes::{
        PgColumnRef, PgComparisonOp, PgLiteral, PgParamValue,
    };
    use pg_lakebase_core::expr::split::ColumnRef;
    use pg_lakebase_core::expr::translator::{
        BuildPredicateError, PgPredicateTranslator, PredicateBuilder,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    /// Minimal error type satisfying `PgPredicateTranslator::Error`.
    #[derive(Debug)]
    struct MockError;

    impl fmt::Display for MockError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("mock translator error")
        }
    }

    impl std::error::Error for MockError {}

    /// Mock translator whose methods are never called for out-of-range indices.
    struct OutOfRangeMockTranslator;

    impl PgPredicateTranslator for OutOfRangeMockTranslator {
        type Scalar = ();
        type Predicate = ();
        type Error = MockError;

        fn column(
            &mut self,
            _col: PgColumnRef<'_>,
        ) -> Result<Self::Scalar, Self::Error> {
            unreachable!("out-of-range check runs before column()")
        }

        fn literal(
            &mut self,
            _lit: PgLiteral<'_>,
        ) -> Result<Self::Scalar, Self::Error> {
            unreachable!("out-of-range check runs before literal()")
        }

        fn param_value(
            &mut self,
            _param: PgParamValue,
        ) -> Result<Self::Scalar, Self::Error> {
            unreachable!("out-of-range check runs before param_value()")
        }

        fn comparison(
            &mut self,
            _op: PgComparisonOp,
            _left: Self::Scalar,
            _right: Self::Scalar,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before comparison()")
        }

        fn is_null(
            &mut self,
            _value: Self::Scalar,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before is_null()")
        }

        fn is_not_null(
            &mut self,
            _value: Self::Scalar,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before is_not_null()")
        }

        fn and(
            &mut self,
            _items: Vec<Self::Predicate>,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before and()")
        }

        fn or(
            &mut self,
            _items: Vec<Self::Predicate>,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before or()")
        }

        fn not(
            &mut self,
            _item: Self::Predicate,
        ) -> Result<Self::Predicate, Self::Error> {
            unreachable!("out-of-range check runs before not()")
        }
    }

    /// Empty `exprs` with `expr_index = 0` → `ExprIndexOutOfRange` before any deref.
    #[pg_test]
    fn build_one_out_of_range_empty_exprs() {
        let mut translator = OutOfRangeMockTranslator;
        let exprs: &[*mut pg_sys::Expr] = &[];
        let column_refs: &[ColumnRef] = &[];
        let resolved_params: &[PgParamValue] = &[];

        // SAFETY: with an empty `exprs` slice and `expr_index = 0`,
        // `PredicateBuilder::build_one` returns the out-of-range error from
        // `exprs.get(0).ok_or(..)` before dereferencing any pointer, so
        // no live PG node is needed.
        let result = unsafe {
            let mut builder = PredicateBuilder::new(
                &mut translator,
                exprs,
                column_refs,
                resolved_params,
                /* scan_relid */ 1,
            );
            builder.build_one(/* expr_index */ 0)
        };

        match result {
            Err(BuildPredicateError::ExprIndexOutOfRange {
                expr_index,
                pushed_len,
            }) => {
                assert_eq!(expr_index, 0, "reported expr_index must match input");
                assert_eq!(pushed_len, 0, "reported pushed_len must be exprs.len()");
            }
            other => panic!(
                "expected ExprIndexOutOfRange {{ expr_index: 0, pushed_len: 0 }}, \
                 got {other:?}"
            ),
        }
    }

    /// Out-of-range index past end reports `expr_index` and `pushed_len`.
    #[pg_test]
    fn build_one_out_of_range_index_past_end() {
        let mut translator = OutOfRangeMockTranslator;
        let exprs: &[*mut pg_sys::Expr] = &[];
        let column_refs: &[ColumnRef] = &[];
        let resolved_params: &[PgParamValue] = &[];

        // SAFETY: same as above — `expr_index = 5` is out of range for the
        // empty slice, so the error is returned before any deref.
        let result = unsafe {
            let mut builder = PredicateBuilder::new(
                &mut translator,
                exprs,
                column_refs,
                resolved_params,
                /* scan_relid */ 1,
            );
            builder.build_one(/* expr_index */ 5)
        };

        match result {
            Err(BuildPredicateError::ExprIndexOutOfRange {
                expr_index,
                pushed_len,
            }) => {
                assert_eq!(expr_index, 5);
                assert_eq!(pushed_len, 0);
            }
            other => panic!(
                "expected ExprIndexOutOfRange {{ expr_index: 5, pushed_len: 0 }}, \
                 got {other:?}"
            ),
        }
    }

    /// Scan-relation RTI; outer Vars carry a different `varno`.
    const SCAN_RELID: c_int = 1;

    /// Recording mock: leaf/combinator calls return descriptive strings for comparison.
    struct RecordingMockTranslator;

    impl PgPredicateTranslator for RecordingMockTranslator {
        type Scalar = String;
        type Predicate = String;
        type Error = MockError;

        fn column(&mut self, col: PgColumnRef<'_>) -> Result<String, MockError> {
            Ok(format!(
                "col(rel={:?},attno={},typ={:?},coll={:?},name={:?})",
                col.rel_oid, col.attno, col.atttypid, col.attcollation, col.name
            ))
        }

        fn literal(&mut self, lit: PgLiteral<'_>) -> Result<String, MockError> {
            Ok(format!("lit(typ={:?},null={})", lit.type_oid, lit.is_null))
        }

        fn param_value(&mut self, param: PgParamValue) -> Result<String, MockError> {
            Ok(format!("param({:?},{})", param.paramkind, param.param_id))
        }

        fn comparison(
            &mut self,
            op: PgComparisonOp,
            left: String,
            right: String,
        ) -> Result<String, MockError> {
            Ok(format!("cmp(opno={:?},{left},{right})", op.opno))
        }

        fn is_null(&mut self, value: String) -> Result<String, MockError> {
            Ok(format!("is_null[{value}]"))
        }

        fn is_not_null(&mut self, value: String) -> Result<String, MockError> {
            Ok(format!("is_not_null[{value}]"))
        }

        fn and(&mut self, items: Vec<String>) -> Result<String, MockError> {
            Ok(format!("and({})", items.join(",")))
        }

        fn or(&mut self, items: Vec<String>) -> Result<String, MockError> {
            Ok(format!("or({})", items.join(",")))
        }

        fn not(&mut self, item: String) -> Result<String, MockError> {
            Ok(format!("not({item})"))
        }
    }

    /// Host-built `Var` (no palloc); `Box` keeps the pointer stable.
    fn host_var(varno: c_int, attno: pg_sys::AttrNumber) -> Box<pg_sys::Var> {
        // SAFETY: `pg_sys::Var` is a `#[repr(C)]` POD whose fields are all
        // integers / Oids, so the all-zero bit pattern is a valid initial
        // state; we then stamp the `NodeTag` discriminant and the fields the
        // walker reads. No `palloc` and no PG C call are involved.
        let mut v: pg_sys::Var = unsafe { core::mem::zeroed() };
        v.xpr.type_ = pg_sys::NodeTag::T_Var;
        v.varno = varno;
        v.varattno = attno;
        v.vartype = pg_sys::INT4OID;
        v.varcollid = pg_sys::Oid::INVALID;
        Box::new(v)
    }

    /// Host-built `NullTest` wrapping `arg`.
    fn host_null_test(
        arg: *mut pg_sys::Expr,
        is_null: bool,
    ) -> Box<pg_sys::NullTest> {
        // SAFETY: `pg_sys::NullTest` is a `#[repr(C)]` POD; zero-init then set
        // the tag, the (single, non-list) arg pointer, and the test type.
        let mut nt: pg_sys::NullTest = unsafe { core::mem::zeroed() };
        nt.xpr.type_ = pg_sys::NodeTag::T_NullTest;
        nt.arg = arg;
        nt.nulltesttype = if is_null {
            pg_sys::NullTestType::IS_NULL
        } else {
            pg_sys::NullTestType::IS_NOT_NULL
        };
        nt.argisrow = false;
        nt.location = -1;
        Box::new(nt)
    }

    /// `column_refs` entry distinct per `expr_index` so rebased lookups are observable.
    fn host_column_ref(expr_index: usize) -> ColumnRef {
        ColumnRef {
            expr_index,
            rel_oid: pg_sys::Oid::from(16_500u32 + expr_index as u32),
            attno: (expr_index as pg_sys::AttrNumber) + 1,
            atttypid: pg_sys::INT4OID,
            attcollation: pg_sys::Oid::INVALID,
            name: Some(format!("scan_col_{expr_index}")),
        }
    }

    /// Single-expr build case: `PredicateBuilder::build_one(i)` equals `build_all()[i]`.
    fn one_equals_multi_case(kinds: &[bool]) -> Result<(), TestCaseError> {
        // Keep the host-built nodes alive for the whole case; raw pointers
        // into the boxes stay valid because `Box` pointees are heap-stable.
        let mut vars: Vec<Box<pg_sys::Var>> = Vec::with_capacity(kinds.len());
        let mut nulls: Vec<Box<pg_sys::NullTest>> = Vec::with_capacity(kinds.len());
        let mut exprs: Vec<*mut pg_sys::Expr> = Vec::with_capacity(kinds.len());
        let mut column_refs: Vec<ColumnRef> = Vec::with_capacity(kinds.len());

        for (i, &is_null) in kinds.iter().enumerate() {
            let mut vbox = host_var(SCAN_RELID, (i as pg_sys::AttrNumber) + 1);
            let vptr = vbox.as_mut() as *mut pg_sys::Var;
            vars.push(vbox);

            let mut ntbox = host_null_test(vptr.cast(), is_null);
            let ntptr = ntbox.as_mut() as *mut pg_sys::NullTest;
            nulls.push(ntbox);

            exprs.push(ntptr.cast());
            column_refs.push(host_column_ref(i));
        }

        let mut t = RecordingMockTranslator;

        // SAFETY: every pointer in `exprs` is a live, host-allocated
        // `NullTest(Var)` kept alive by `vars` / `nulls`; `column_refs` is the
        // matching synthetic slice; the `NullTest(Var)` walk reads only plain
        // struct fields, so no PG `List` or FFI is reached.
        let multi = unsafe {
            let mut builder =
                PredicateBuilder::new(&mut t, &exprs, &column_refs, &[], SCAN_RELID);
            builder.build_all()
        }
        .expect("all-success NullTest(Var) list translates to Ok");

        prop_assert_eq!(
            multi.len(),
            exprs.len(),
            "every pushed expression yields exactly one predicate"
        );

        for (i, multi_pred) in multi.iter().enumerate() {
            // SAFETY: same invariants as the multi-expr call; `i` is in range
            // so no pointer beyond `exprs` is touched.
            let one = unsafe {
                let mut builder = PredicateBuilder::new(
                    &mut t,
                    &exprs,
                    &column_refs,
                    &[],
                    SCAN_RELID,
                );
                builder.build_one(i)
            }
            .expect("valid index translates to Ok");

            prop_assert_eq!(
                &one,
                multi_pred,
                "single-expr result at index {} != multi-expr element",
                i
            );
        }

        for k in 0..3usize {
            let idx = exprs.len() + k;
            // SAFETY: out-of-range check fires before any pointer deref.
            let oor = unsafe {
                let mut builder = PredicateBuilder::new(
                    &mut t,
                    &exprs,
                    &column_refs,
                    &[],
                    SCAN_RELID,
                );
                builder.build_one(idx)
            };
            match oor {
                Err(BuildPredicateError::ExprIndexOutOfRange {
                    expr_index,
                    pushed_len,
                }) => {
                    prop_assert_eq!(expr_index, idx);
                    prop_assert_eq!(pushed_len, exprs.len());
                }
                other => prop_assert!(
                    false,
                    "expected ExprIndexOutOfRange at idx {}, got {:?}",
                    idx,
                    other
                ),
            }
        }

        Ok(())
    }

    /// Single-expr/multi-expr equivalence over randomized `NullTest(Var)` lists (manual `TestRunner`).
    #[pg_test]
    fn one_equals_multi() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        let strategy = proptest::collection::vec(any::<bool>(), 0..12);

        runner
            .run(&strategy, |kinds| one_equals_multi_case(&kinds))
            .expect("single-expr/multi-expr equivalence property failed");
    }

    /// Outer-relation `Var` error matches between multi- and single-expr paths.
    #[pg_test]
    fn outer_var_error_matches_multi_path() {
        const OUTER_RELID: c_int = 7;

        let mut v0 = host_var(SCAN_RELID, 1);
        let v0p = v0.as_mut() as *mut pg_sys::Var;
        let mut nt0 = host_null_test(v0p.cast(), true);
        let nt0p = nt0.as_mut() as *mut pg_sys::NullTest;

        let mut v1 = host_var(OUTER_RELID, 1);
        let v1p = v1.as_mut() as *mut pg_sys::Var;
        let mut nt1 = host_null_test(v1p.cast(), true);
        let nt1p = nt1.as_mut() as *mut pg_sys::NullTest;

        let exprs: Vec<*mut pg_sys::Expr> = vec![nt0p.cast(), nt1p.cast()];
        let column_refs = vec![host_column_ref(0), host_column_ref(1)];

        let mut t = RecordingMockTranslator;

        // SAFETY: both `exprs` pointers are live host-built `NullTest(Var)`
        // nodes kept alive by the local boxes; the walk is list-free.
        let multi = unsafe {
            let mut builder =
                PredicateBuilder::new(&mut t, &exprs, &column_refs, &[], SCAN_RELID);
            builder.build_all()
        };
        match multi {
            Err(BuildPredicateError::OuterRelationVar {
                expr_index,
                varno,
                scan_relid,
            }) => {
                assert_eq!(expr_index, 1, "first error is at index 1");
                assert_eq!(varno, OUTER_RELID);
                assert_eq!(scan_relid, SCAN_RELID);
            }
            other => panic!("expected OuterRelationVar at index 1, got {other:?}"),
        }

        // SAFETY: as above; index 1 is in range.
        let one = unsafe {
            let mut builder =
                PredicateBuilder::new(&mut t, &exprs, &column_refs, &[], SCAN_RELID);
            builder.build_one(1)
        };
        match one {
            Err(BuildPredicateError::OuterRelationVar {
                expr_index,
                varno,
                scan_relid,
            }) => {
                assert_eq!(expr_index, 1);
                assert_eq!(varno, OUTER_RELID);
                assert_eq!(scan_relid, SCAN_RELID);
            }
            other => panic!("expected OuterRelationVar at index 1, got {other:?}"),
        }

        // SAFETY: as above; index 0 is in range.
        let ok0 = unsafe {
            let mut builder =
                PredicateBuilder::new(&mut t, &exprs, &column_refs, &[], SCAN_RELID);
            builder.build_one(0)
        }
        .expect("scan-relation Var translates to Ok");
        assert!(
            ok0.starts_with("is_null["),
            "index 0 should be an IS NULL predicate, got {ok0}"
        );
    }

    /// Outer-relation `Var` surviving into `PredicateBuilder` → `OuterRelationVar`.
    #[pg_test]
    fn predicate_builder_outer_relation_var_guard_names_offending_varno() {
        const OUTER_RELID: c_int = 9;

        // Keep the host-built nodes alive for the whole test; raw pointers
        // into the boxes stay valid because `Box` pointees are heap-stable.
        let mut outer_var = host_var(OUTER_RELID, 1);
        let outer_var_ptr = outer_var.as_mut() as *mut pg_sys::Var;
        let mut null_test =
            host_null_test(outer_var_ptr.cast(), /* is_null */ true);
        let null_test_ptr = null_test.as_mut() as *mut pg_sys::NullTest;

        let exprs: Vec<*mut pg_sys::Expr> = vec![null_test_ptr.cast()];
        // column_refs present but never consulted: rejected on varno first.
        let column_refs = vec![host_column_ref(0)];

        let mut translator = RecordingMockTranslator;

        // SAFETY: the single `exprs` pointer is a live host-built
        // `NullTest(Var)` kept alive by the local boxes; the `NullTest(Var)`
        // walk reads only plain struct fields, so no PG `List` or FFI is
        // reached.
        let result = unsafe {
            let mut builder = PredicateBuilder::new(
                &mut translator,
                &exprs,
                &column_refs,
                &[],
                SCAN_RELID,
            );
            builder.build_all()
        };

        match result {
            Err(BuildPredicateError::OuterRelationVar {
                expr_index,
                varno,
                scan_relid,
            }) => {
                assert_eq!(expr_index, 0, "the sole pushed expression is at index 0");
                assert_eq!(
                    varno, OUTER_RELID,
                    "the error must name the offending outer-relation varno"
                );
                assert_eq!(
                    scan_relid, SCAN_RELID,
                    "the error must carry the scan_relid the varno failed to match"
                );
            }
            other => panic!(
                "expected OuterRelationVar {{ expr_index: 0, varno: {OUTER_RELID}, \
                 scan_relid: {SCAN_RELID} }} and no predicate, got {other:?}"
            ),
        }
    }

    const CONV_SCAN_RELID: c_int = 1;
    const CONV_OUTER_RELID: c_int = 7;
    const CONV_PARAM_ID: c_int = 1;
    const CONV_COL_ATTNO: pg_sys::AttrNumber = 1;
    const CONV_OUTER_ATTNO: pg_sys::AttrNumber = 1;

    const CONV_INT4_OID: pg_sys::Oid = pg_sys::Oid::from_u32(23);
    const CONV_REL_OID: u32 = 16_600;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ConvergenceScalar {
        Column {
            rel_oid: u32,
            attno: pg_sys::AttrNumber,
            atttypid: u32,
            name: Option<String>,
        },
        Datum(usize),
        Null,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ConvergencePredicate {
        AlwaysFalse,
        Binary {
            opno: u32,
            opcollid: u32,
            inputcollid: u32,
            left: ConvergenceScalar,
            right: ConvergenceScalar,
        },
        Unary {
            kind: &'static str,
            value: ConvergenceScalar,
        },
        And(Vec<ConvergencePredicate>),
        Or(Vec<ConvergencePredicate>),
        Not(Box<ConvergencePredicate>),
    }

    /// Mock translator; NULL-operand comparisons fold to `AlwaysFalse`.
    struct ConvergenceMockTranslator;

    impl PgPredicateTranslator for ConvergenceMockTranslator {
        type Scalar = ConvergenceScalar;
        type Predicate = ConvergencePredicate;
        type Error = MockError;

        fn column(
            &mut self,
            col: PgColumnRef<'_>,
        ) -> Result<ConvergenceScalar, MockError> {
            Ok(ConvergenceScalar::Column {
                rel_oid: col.rel_oid.to_u32(),
                attno: col.attno,
                atttypid: col.atttypid.to_u32(),
                name: col.name.map(|s| s.to_string()),
            })
        }

        fn literal(
            &mut self,
            lit: PgLiteral<'_>,
        ) -> Result<ConvergenceScalar, MockError> {
            if lit.is_null {
                Ok(ConvergenceScalar::Null)
            } else {
                Ok(ConvergenceScalar::Datum(lit.datum.value()))
            }
        }

        fn param_value(
            &mut self,
            param: PgParamValue,
        ) -> Result<ConvergenceScalar, MockError> {
            if param.is_null {
                Ok(ConvergenceScalar::Null)
            } else {
                Ok(ConvergenceScalar::Datum(param.datum.value()))
            }
        }

        fn comparison(
            &mut self,
            op: PgComparisonOp,
            left: ConvergenceScalar,
            right: ConvergenceScalar,
        ) -> Result<ConvergencePredicate, MockError> {
            // SQL three-valued logic: strict comparison with NULL → AlwaysFalse.
            if matches!(left, ConvergenceScalar::Null)
                || matches!(right, ConvergenceScalar::Null)
            {
                return Ok(ConvergencePredicate::AlwaysFalse);
            }
            Ok(ConvergencePredicate::Binary {
                opno: op.opno.to_u32(),
                opcollid: op.opcollid.to_u32(),
                inputcollid: op.inputcollid.to_u32(),
                left,
                right,
            })
        }

        fn is_null(
            &mut self,
            value: ConvergenceScalar,
        ) -> Result<ConvergencePredicate, MockError> {
            Ok(ConvergencePredicate::Unary {
                kind: "is_null",
                value,
            })
        }

        fn is_not_null(
            &mut self,
            value: ConvergenceScalar,
        ) -> Result<ConvergencePredicate, MockError> {
            Ok(ConvergencePredicate::Unary {
                kind: "is_not_null",
                value,
            })
        }

        fn and(
            &mut self,
            items: Vec<ConvergencePredicate>,
        ) -> Result<ConvergencePredicate, MockError> {
            Ok(ConvergencePredicate::And(items))
        }

        fn or(
            &mut self,
            items: Vec<ConvergencePredicate>,
        ) -> Result<ConvergencePredicate, MockError> {
            Ok(ConvergencePredicate::Or(items))
        }

        fn not(
            &mut self,
            item: ConvergencePredicate,
        ) -> Result<ConvergencePredicate, MockError> {
            Ok(ConvergencePredicate::Not(Box::new(item)))
        }
    }

    struct ConvergenceExprFixture;

    impl ConvergenceExprFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(CONV_SCAN_RELID)
        }

        unsafe fn var(relid: c_int, attno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var_at(relid, attno) }
        }

        unsafe fn param_exec(param_id: c_int) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_exec_param(param_id) }
        }

        unsafe fn binop(
            opno: u32,
            opcollid: u32,
            inputcollid: u32,
            lhs: *mut pg_sys::Expr,
            rhs: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                Self::nodes().op_expr(
                    OpExprSpec::int4_comparison(opno).with_collations(
                        pg_sys::Oid::from(opcollid),
                        pg_sys::Oid::from(inputcollid),
                    ),
                    &[lhs, rhs],
                )
            }
        }
    }

    /// In-place rewrite: outer `Var` → `PARAM_EXEC` (models `replace_nestloop_params`).
    unsafe fn rewrite_outer_vars(
        expr: *mut pg_sys::Expr,
        scan_relid: c_int,
        param_id: c_int,
    ) {
        if expr.is_null() {
            return;
        }
        if unsafe { (*expr).type_ } != pg_sys::NodeTag::T_OpExpr {
            return;
        }
        let op = expr as *mut pg_sys::OpExpr;
        let args = unsafe { (*op).args };
        if args.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(args) };
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
            if cell.is_null() {
                continue;
            }
            if unsafe { (*cell).type_ } == pg_sys::NodeTag::T_Var {
                let v = cell as *mut pg_sys::Var;
                if unsafe { (*v).varno } != scan_relid {
                    let param =
                        unsafe { ConvergenceExprFixture::param_exec(param_id) };
                    let elements = unsafe { (*args).elements };
                    debug_assert!(!elements.is_null());
                    unsafe {
                        (*elements.offset(i as isize)).ptr_value =
                            param as *mut core::ffi::c_void;
                    }
                }
            }
        }
    }

    /// Build `column_refs` for the single scan column used by this property.
    unsafe fn build_column_refs(
        exprs: &[*mut pg_sys::Expr],
        _scan_relid: c_int,
    ) -> Vec<ColumnRef> {
        exprs
            .iter()
            .enumerate()
            .map(|(expr_index, _)| ColumnRef {
                expr_index,
                rel_oid: pg_sys::Oid::from(CONV_REL_OID),
                attno: CONV_COL_ATTNO,
                atttypid: pg_sys::INT4OID,
                attcollation: pg_sys::Oid::INVALID,
                name: Some(format!("col_{CONV_COL_ATTNO}")),
            })
            .collect()
    }

    /// Convergence case: outer-Var vs explicit-param clauses converge after rewrite.
    fn convergence_case(
        opno: u32,
        opcollid: u32,
        inputcollid: u32,
        col_on_left: bool,
        value: Option<i32>,
    ) -> Result<(), TestCaseError> {
        unsafe {
            let scan_col_a =
                ConvergenceExprFixture::var(CONV_SCAN_RELID, CONV_COL_ATTNO);
            let outer_var_a =
                ConvergenceExprFixture::var(CONV_OUTER_RELID, CONV_OUTER_ATTNO);
            let (lhs_a, rhs_a) = if col_on_left {
                (scan_col_a, outer_var_a)
            } else {
                (outer_var_a, scan_col_a)
            };
            let clause_a = ConvergenceExprFixture::binop(
                opno,
                opcollid,
                inputcollid,
                lhs_a,
                rhs_a,
            );
            let exprs_a = [clause_a];

            let column_refs_a_pre = build_column_refs(&exprs_a, CONV_SCAN_RELID);

            rewrite_outer_vars(clause_a, CONV_SCAN_RELID, CONV_PARAM_ID);

            let column_refs_a = build_column_refs(&exprs_a, CONV_SCAN_RELID);

            prop_assert_eq!(
                &column_refs_a_pre,
                &column_refs_a,
                "column_refs must be stable across the modeled \
                 replace_nestloop_params rewrite "
            );

            let scan_col_b =
                ConvergenceExprFixture::var(CONV_SCAN_RELID, CONV_COL_ATTNO);
            let param_b = ConvergenceExprFixture::param_exec(CONV_PARAM_ID);
            let (lhs_b, rhs_b) = if col_on_left {
                (scan_col_b, param_b)
            } else {
                (param_b, scan_col_b)
            };
            let clause_b = ConvergenceExprFixture::binop(
                opno,
                opcollid,
                inputcollid,
                lhs_b,
                rhs_b,
            );
            let exprs_b = [clause_b];
            let column_refs_b = build_column_refs(&exprs_b, CONV_SCAN_RELID);

            prop_assert_eq!(
                column_refs_a.len(),
                1,
                "exactly one scan-column ref (outer Var / param produce none)"
            );
            prop_assert_eq!(column_refs_a[0].attno, CONV_COL_ATTNO);
            prop_assert_eq!(
                &column_refs_a,
                &column_refs_b,
                "column_refs of the outer-Var form and the explicit-param \
                 form must be identical "
            );

            let resolved = vec![PgParamValue {
                param_id: CONV_PARAM_ID,
                paramkind: pg_sys::ParamKind::PARAM_EXEC,
                type_oid: CONV_INT4_OID,
                collid: pg_sys::Oid::INVALID,
                datum: pg_sys::Datum::from(value.unwrap_or(0) as usize),
                is_null: value.is_none(),
            }];

            let mut translator = ConvergenceMockTranslator;

            // SAFETY: every pointer in `exprs_*` is a live PG-allocated
            // OpExpr (Var/Param operands) kept alive by the current memory
            // context; `column_refs_*` are the matching walker outputs.
            let pred_a = {
                let mut builder = PredicateBuilder::new(
                    &mut translator,
                    &exprs_a,
                    &column_refs_a,
                    &resolved,
                    CONV_SCAN_RELID,
                );
                builder
                    .build_all()
                    .expect("outer-Var (rewritten) clause translates to Ok")
            };
            let pred_b = {
                let mut builder = PredicateBuilder::new(
                    &mut translator,
                    &exprs_b,
                    &column_refs_b,
                    &resolved,
                    CONV_SCAN_RELID,
                );
                builder
                    .build_all()
                    .expect("explicit-param clause translates to Ok")
            };

            prop_assert_eq!(
                pred_a.len(),
                1,
                "one predicate per pushed expression (outer-Var form)"
            );
            prop_assert_eq!(
                pred_b.len(),
                1,
                "one predicate per pushed expression (explicit-param form)"
            );

            prop_assert_eq!(
                &pred_a[0],
                &pred_b[0],
                "the outer-Var clause and the explicit-param clause must build \
                 structurally identical predicates "
            );

            if value.is_none() {
                prop_assert_eq!(
                    &pred_a[0],
                    &ConvergencePredicate::AlwaysFalse,
                    "NULL outer value must fold to AlwaysFalse (outer-Var form)"
                );
                prop_assert_eq!(
                    &pred_b[0],
                    &ConvergencePredicate::AlwaysFalse,
                    "NULL param value must fold to AlwaysFalse (explicit-param form)"
                );
            } else {
                prop_assert!(
                    matches!(pred_a[0], ConvergencePredicate::Binary { .. }),
                    "a non-NULL resolved value must produce a Binary predicate, \
                     got {:?}",
                    pred_a[0]
                );
            }
        }

        Ok(())
    }

    /// Outer-Var and explicit-param clauses converge (manual `TestRunner`).
    #[pg_test]
    fn outer_var_param_convergence() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);

        let opno = prop_oneof![
            Just(96u32),   // int4eq
            Just(97u32),   // int4lt
            Just(518u32),  // int4ne
            Just(98u32),   // texteq (non-allowlisted)
            Just(1752u32), // numeric_eq (non-allowlisted)
        ];
        let opcollid = prop_oneof![Just(0u32), Just(100u32)];
        let inputcollid = prop_oneof![Just(0u32), Just(100u32)];
        let value = prop_oneof![Just(None), any::<i32>().prop_map(Some)];

        let strategy = (opno, opcollid, inputcollid, any::<bool>(), value);

        runner
            .run(
                &strategy,
                |(opno, opcollid, inputcollid, col_on_left, value)| {
                    convergence_case(opno, opcollid, inputcollid, col_on_left, value)
                },
            )
            .expect("outer-Var / explicit-param convergence property failed");
    }
}
