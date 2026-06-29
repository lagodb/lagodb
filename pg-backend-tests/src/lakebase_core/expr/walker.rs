//! Backend tests for expr walker classification and pushed-expression column identity.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::support::pg::PgNodeBuilder;
    use pg_lakebase_core::expr::nodes::PgExprRef;
    use pg_lakebase_core::expr::predicate::{
        PlanColumnRef, PlanPredicate, PlanPredicateContext, PlanScalar,
    };
    use pg_lakebase_core::expr::split::{
        ColumnRef, PushdownContract, PushdownCosting, QualPushdownDecision,
    };
    use pg_lakebase_core::expr::walker::{
        ClauseClassification, ClauseClassifier, rewrite_not,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    const INT4_EQ_OID: u32 = 96; // pgrx: Int4EqualOperator
    const INT4_LT_OID: u32 = 97; // pgrx: Int4LessOperator
    const INT4_NE_OID: u32 = 518;

    const BOOL_OID: pg_sys::Oid = pg_sys::Oid::from_u32(16);

    const VARATTNO_EXACT: pg_sys::AttrNumber = 1;
    const VARATTNO_UNSUPPORTED: pg_sys::AttrNumber = 2;
    const VARATTNO_CONSERVATIVE_PRUNING: pg_sys::AttrNumber = 3;

    const SCAN_RELID: core::ffi::c_int = 1;

    fn test_predicate_ctx() -> PlanPredicateContext {
        PlanPredicateContext {
            rel_oid: pg_sys::Oid::INVALID,
            scan_relid: SCAN_RELID,
        }
    }

    struct ExprFixture;

    impl ExprFixture {
        fn nodes(scan_relid: core::ffi::c_int) -> PgNodeBuilder {
            PgNodeBuilder::new(scan_relid)
        }

        unsafe fn int4_var(attno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).int4_var(attno) }
        }

        unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).int4_const(value) }
        }

        /// `Var(attno) op const_value` via planner `make_opclause`.
        unsafe fn int4_op(
            opno: u32,
            attno: pg_sys::AttrNumber,
            const_value: i32,
        ) -> *mut pg_sys::Expr {
            unsafe {
                Self::nodes(SCAN_RELID).int4_var_op_const(opno, attno, const_value)
            }
        }

        unsafe fn bool_expr(
            boolop: pg_sys::BoolExprType::Type,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).bool_expr(boolop, args) }
        }

        unsafe fn null_test(
            arg: *mut pg_sys::Expr,
            nulltesttype: pg_sys::NullTestType::Type,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).null_test(arg, nulltesttype) }
        }

        unsafe fn int4_var_at(
            relid: core::ffi::c_int,
            attno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).int4_var_at(relid, attno) }
        }

        unsafe fn param_exec_int4(param_id: core::ffi::c_int) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).int4_exec_param(param_id) }
        }

        unsafe fn int4_binop(
            opno: u32,
            lhs: *mut pg_sys::Expr,
            rhs: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes(SCAN_RELID).int4_op_clause(opno, lhs, rhs) }
        }
    }

    /// Test leaf classifier dispatching on scan-column `attno` from a parsed predicate.
    fn classify_by_varattno(predicate: &PlanPredicate) -> QualPushdownDecision {
        let attno = scan_column_attno(predicate);
        match attno {
            Some(VARATTNO_EXACT) => QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
            Some(VARATTNO_CONSERVATIVE_PRUNING) => QualPushdownDecision::Pushable {
                contract: PushdownContract::ConservativePruning,
                costing: PushdownCosting::CostedPruning,
            },
            _ => QualPushdownDecision::Unsupported,
        }
    }

    fn scan_column_attno(predicate: &PlanPredicate) -> Option<pg_sys::AttrNumber> {
        let col = match predicate {
            PlanPredicate::Comparison { left, .. } => column_ref(left)?,
            PlanPredicate::IsNull { value } | PlanPredicate::IsNotNull { value } => {
                column_ref(value)?
            }
        };
        Some(col.attno)
    }

    fn column_ref(scalar: &PlanScalar) -> Option<PlanColumnRef> {
        match scalar {
            PlanScalar::Column(c) => Some(*c),
            _ => None,
        }
    }

    unsafe fn boolop_of(expr: *mut pg_sys::Expr) -> pg_sys::BoolExprType::Type {
        unsafe {
            assert!(!expr.is_null(), "expected BoolExpr, got null");
            let r = PgExprRef::from_raw(expr);
            assert_eq!(
                r.node_tag(),
                pg_sys::NodeTag::T_BoolExpr,
                "expected T_BoolExpr",
            );
            let be = expr as *mut pg_sys::BoolExpr;
            (*be).boolop
        }
    }

    unsafe fn boolexpr_argc(expr: *mut pg_sys::Expr) -> i32 {
        unsafe {
            let be = expr as *mut pg_sys::BoolExpr;
            let args = (*be).args;
            if args.is_null() {
                0
            } else {
                pg_sys::list_length(args)
            }
        }
    }

    unsafe fn opno_of(expr: *mut pg_sys::Expr) -> u32 {
        unsafe {
            let r = PgExprRef::from_raw(expr);
            assert_eq!(r.node_tag(), pg_sys::NodeTag::T_OpExpr, "expected T_OpExpr");
            let op = expr as *mut pg_sys::OpExpr;
            (*op).opno.to_u32()
        }
    }

    unsafe fn null_test_kind_of(
        expr: *mut pg_sys::Expr,
    ) -> pg_sys::NullTestType::Type {
        unsafe {
            let r = PgExprRef::from_raw(expr);
            assert_eq!(
                r.node_tag(),
                pg_sys::NodeTag::T_NullTest,
                "expected T_NullTest",
            );
            let nt = expr as *mut pg_sys::NullTest;
            (*nt).nulltesttype
        }
    }

    /// `a = 1 AND unsupported(b)` → partial push; unsupported stays in residual.
    #[pg_test]
    fn classify_and_partial_pushdown() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let unsupported_b =
                ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_UNSUPPORTED, 7);
            let and_expr = ExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[a_eq_1, unsupported_b],
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(and_expr);

            match result {
                ClauseClassification::Pushable { parts, residual } => {
                    assert_eq!(parts.len(), 1);
                    assert_eq!(
                        opno_of(parts[0].expr),
                        INT4_EQ_OID,
                        "pushed should be a=1 (INT4EQ)",
                    );
                    assert_eq!(
                        parts[0].expr, a_eq_1,
                        "pushed should reuse the original a=1 pointer (no copy)",
                    );
                    assert_eq!(parts[0].contract, PushdownContract::ExactRowFilter,);

                    let Some(residual) = residual else {
                        panic!("expected residual for partial AND pushdown");
                    };
                    assert_eq!(
                        opno_of(residual),
                        INT4_EQ_OID,
                        "residual should be the unsupported(b) OpExpr",
                    );
                    assert_eq!(
                        residual, unsupported_b,
                        "residual should reuse the original unsupported pointer",
                    );
                }
                other => panic!("expected Pushable with one part, got {other:?}"),
            }
        }
    }

    /// `a = 1 AND a < 2` (Exact + ConservativePruning) keeps separate pushed parts.
    #[pg_test]
    fn classify_and_exact_and_conservative_keeps_separate_parts() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let a_lt_2 =
                ExprFixture::int4_op(INT4_LT_OID, VARATTNO_CONSERVATIVE_PRUNING, 2);
            let and_expr = ExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[a_eq_1, a_lt_2],
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(and_expr);

            match result {
                ClauseClassification::Pushable { parts, .. } => {
                    assert_eq!(
                        parts.len(),
                        2,
                        "mixed AND must not merge pushed parts"
                    );
                    assert_eq!(parts[0].expr, a_eq_1);
                    assert_eq!(parts[0].contract, PushdownContract::ExactRowFilter);
                    assert_eq!(parts[1].expr, a_lt_2);
                    assert_eq!(
                        parts[1].contract,
                        PushdownContract::ConservativePruning
                    );
                }
                other => panic!(
                    "expected Pushable with two parts for exact+conservative AND, got {other:?}",
                ),
            }
        }
    }

    /// Mixed ExactRowFilter / ConservativePruning OR branches must not promote to ExactRowFilter.
    #[pg_test]
    fn classify_or_exact_all_or_nothing() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let a_lt_2 =
                ExprFixture::int4_op(INT4_LT_OID, VARATTNO_CONSERVATIVE_PRUNING, 2);
            let or_expr = ExprFixture::bool_expr(
                pg_sys::BoolExprType::OR_EXPR,
                &[a_eq_1, a_lt_2],
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(or_expr);

            match result {
                ClauseClassification::PartialPush { pushed, residual } => {
                    assert_eq!(
                        boolop_of(pushed),
                        pg_sys::BoolExprType::OR_EXPR,
                        "widened pushed should be an OR",
                    );
                    assert_eq!(
                        boolexpr_argc(pushed),
                        2,
                        "widened OR should have 2 args"
                    );
                    assert_eq!(
                        residual, or_expr,
                        "residual should be the original OR ",
                    );
                }
                ClauseClassification::Pushable { parts, residual }
                    if residual.is_none()
                        && parts.iter().all(|p| {
                            p.contract == PushdownContract::ExactRowFilter
                        }) =>
                {
                    panic!(
                        "OR with ConservativePruning branch must NOT be promoted to ExactRowFilter",
                    );
                }
                other => panic!("expected PartialPush, got {other:?}"),
            }
        }
    }

    /// `(a = 1 AND unsupported(b)) OR a = 2` widens inner AND before composing OR.
    #[pg_test]
    fn classify_or_conservative_pruning_widening() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let unsupported_b =
                ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_UNSUPPORTED, 7);
            let inner_and = ExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[a_eq_1, unsupported_b],
            );
            let a_eq_2 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 2);
            let or_expr = ExprFixture::bool_expr(
                pg_sys::BoolExprType::OR_EXPR,
                &[inner_and, a_eq_2],
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(or_expr);

            match result {
                ClauseClassification::PartialPush { pushed, residual } => {
                    assert_eq!(
                        boolop_of(pushed),
                        pg_sys::BoolExprType::OR_EXPR,
                        "widened pushed should be an OR",
                    );
                    assert_eq!(
                        boolexpr_argc(pushed),
                        2,
                        "widened OR should have 2 args"
                    );

                    let be = pushed as *mut pg_sys::BoolExpr;
                    let arg0 = pg_sys::list_nth((*be).args, 0) as *mut pg_sys::Expr;
                    let arg1 = pg_sys::list_nth((*be).args, 1) as *mut pg_sys::Expr;
                    assert_eq!(opno_of(arg0), INT4_EQ_OID);
                    assert_eq!(opno_of(arg1), INT4_EQ_OID);

                    assert_eq!(
                        residual, or_expr,
                        "residual should be the original OR ",
                    );
                }
                other => panic!("expected PartialPush, got {other:?}"),
            }
        }
    }

    /// `a = 1 OR unsupported(b)` → entire OR unsupported (no partial push).
    #[pg_test]
    fn classify_or_unsupported_branch_does_not_push() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let unsupported_b =
                ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_UNSUPPORTED, 7);
            let or_expr = ExprFixture::bool_expr(
                pg_sys::BoolExprType::OR_EXPR,
                &[a_eq_1, unsupported_b],
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(or_expr);

            match result {
                ClauseClassification::Unsupported { residual } => {
                    assert_eq!(
                        residual, or_expr,
                        "residual should be the original OR",
                    );
                }
                other => panic!(
                    "expected Unsupported (OR with unsupported branch), got {other:?}",
                ),
            }
        }
    }

    /// `NOT (a = 1)` rewrites to `a <> 1`, then classifies as Exact.
    #[pg_test]
    fn rewrite_not_eq_becomes_ne() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let not_eq =
                ExprFixture::bool_expr(pg_sys::BoolExprType::NOT_EXPR, &[a_eq_1]);

            let rewritten = rewrite_not(not_eq);

            assert_eq!(
                opno_of(rewritten),
                INT4_NE_OID,
                "NOT (a = 1) should rewrite to (a <> 1) via the operator negator",
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(rewritten);

            match result {
                ClauseClassification::Pushable {
                    parts, residual, ..
                } => {
                    assert_eq!(
                        parts[0].expr, rewritten,
                        "Exact pushed pointer should be the rewritten OpExpr",
                    );
                    assert_eq!(
                        opno_of(parts[0].expr),
                        INT4_NE_OID,
                        "Exact pushed should be the rewritten OpExpr",
                    );
                    assert!(
                        residual.is_none(),
                        "Exact pushable removes the clause from residual ",
                    );
                }
                other => {
                    panic!("expected Pushable Exact for rewritten <>, got {other:?}")
                }
            }
        }
    }

    /// `NOT (a IS NULL)` rewrites to `a IS NOT NULL`.
    #[pg_test]
    fn rewrite_not_is_null_becomes_is_not_null() {
        unsafe {
            let var = ExprFixture::int4_var(VARATTNO_EXACT);
            let is_null = ExprFixture::null_test(var, pg_sys::NullTestType::IS_NULL);
            let not_is_null =
                ExprFixture::bool_expr(pg_sys::BoolExprType::NOT_EXPR, &[is_null]);

            let rewritten = rewrite_not(not_is_null);

            let r = PgExprRef::from_raw(rewritten);
            assert_eq!(
                r.node_tag(),
                pg_sys::NodeTag::T_NullTest,
                "NOT (a IS NULL) should rewrite to a NullTest (IS NOT NULL)",
            );
            assert_eq!(
                null_test_kind_of(rewritten),
                pg_sys::NullTestType::IS_NOT_NULL,
                "NOT (a IS NULL) should flip to IS NOT NULL",
            );
        }
    }

    /// DeMorgan: `NOT (a = 1 AND a = 2)` → `(a <> 1) OR (a <> 2)`.
    #[pg_test]
    fn rewrite_not_demorgan_and_to_or() {
        unsafe {
            let a_eq_1 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 1);
            let a_eq_2 = ExprFixture::int4_op(INT4_EQ_OID, VARATTNO_EXACT, 2);
            let inner_and = ExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[a_eq_1, a_eq_2],
            );
            let not_and =
                ExprFixture::bool_expr(pg_sys::BoolExprType::NOT_EXPR, &[inner_and]);

            let rewritten = rewrite_not(not_and);

            assert_eq!(
                boolop_of(rewritten),
                pg_sys::BoolExprType::OR_EXPR,
                "DeMorgan: NOT (A AND B) should rewrite to (NOT A) OR (NOT B)",
            );
            assert_eq!(
                boolexpr_argc(rewritten),
                2,
                "the resulting OR should have 2 args",
            );

            let be = rewritten as *mut pg_sys::BoolExpr;
            let arg0 = pg_sys::list_nth((*be).args, 0) as *mut pg_sys::Expr;
            let arg1 = pg_sys::list_nth((*be).args, 1) as *mut pg_sys::Expr;
            assert_eq!(
                opno_of(arg0),
                INT4_NE_OID,
                "first DeMorgan child should be (a <> 1)",
            );
            assert_eq!(
                opno_of(arg1),
                INT4_NE_OID,
                "second DeMorgan child should be (a <> 2)",
            );
        }
    }

    /// Literal `NOT` over ConservativePruning child (without rewrite) must not auto-push.
    #[pg_test]
    fn classify_not_conservative_pruning_is_not_auto_pushed() {
        unsafe {
            let a_lt_2 =
                ExprFixture::int4_op(INT4_LT_OID, VARATTNO_CONSERVATIVE_PRUNING, 2);
            let not_expr =
                ExprFixture::bool_expr(pg_sys::BoolExprType::NOT_EXPR, &[a_lt_2]);

            // Skip `rewrite_not` to simulate a NOT the rewrite could not eliminate.
            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(not_expr);

            match result {
                ClauseClassification::Unsupported { residual } => {
                    assert_eq!(
                        residual, not_expr,
                        "literal NOT over ConservativePruning child must remain in residual",
                    );
                }
                other => panic!(
                    "expected Unsupported for literal NOT over ConservativePruning child, got {other:?}",
                ),
            }
        }
    }

    /// Volatile `FuncExpr` forces Unsupported before leaf classification.
    #[pg_test]
    fn classify_volatile_function_is_unsupported() {
        unsafe {
            let random_func = pg_sys::makeFuncExpr(
                pg_sys::Oid::from(1598_u32), // F_RANDOM_
                pg_sys::Oid::from(701_u32),  // FLOAT8OID (real return type)
                ptr::null_mut(),             // no args
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
                pg_sys::CoercionForm::COERCE_EXPLICIT_CALL,
            );
            // Pair volatile FuncExpr with Exact Var so the gate (not missing Var) fires.
            let var = ExprFixture::int4_var(VARATTNO_EXACT);
            let cmp = pg_sys::make_opclause(
                pg_sys::Oid::from(INT4_LT_OID),
                BOOL_OID,
                false,
                var,
                random_func.cast(),
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(cmp);

            match result {
                ClauseClassification::Unsupported { residual } => {
                    assert_eq!(
                        residual, cmp,
                        "volatile-tainted clause should be returned as Unsupported with the original residual",
                    );
                }
                other => panic!(
                    "expected Unsupported (volatile function gate), got {other:?}",
                ),
            }
        }
    }

    /// `SubPlan` operand forces Unsupported.
    #[pg_test]
    fn classify_subplan_is_unsupported() {
        unsafe {
            // Only `xpr.type_ = T_SubPlan` is read by the walker.
            let subplan = pg_sys::palloc0(core::mem::size_of::<pg_sys::SubPlan>())
                as *mut pg_sys::SubPlan;
            (*subplan).xpr.type_ = pg_sys::NodeTag::T_SubPlan;

            let var = ExprFixture::int4_var(VARATTNO_EXACT);
            let cmp = pg_sys::make_opclause(
                pg_sys::Oid::from(INT4_EQ_OID),
                BOOL_OID,
                false,
                subplan.cast(),
                var,
                pg_sys::Oid::INVALID,
                pg_sys::Oid::INVALID,
            );

            let mut classify = classify_by_varattno;
            let ctx = test_predicate_ctx();
            let mut classifier = ClauseClassifier::new(&ctx, &mut classify);
            let result = classifier.classify(cmp);

            match result {
                ClauseClassification::Unsupported { residual } => {
                    assert_eq!(
                        residual, cmp,
                        "SubPlan-tainted clause should be returned as Unsupported with the original residual",
                    );
                }
                other => panic!(
                    "expected Unsupported (SubPlan gate), got {other:?}",
                ),
            }
        }
    }

    const OUTER_RELID: core::ffi::c_int = 2;
    const POST_SETREFS_RTI: core::ffi::c_int = 5;

    unsafe fn walk_and_collect(
        exprs: &[*mut pg_sys::Expr],
        scan_relid: core::ffi::c_int,
    ) -> Vec<(usize, pg_sys::AttrNumber)> {
        let mut hits: Vec<(usize, pg_sys::AttrNumber)> = Vec::new();
        for (expr_index, &expr) in exprs.iter().enumerate() {
            for var in unsafe { pg_collect_scan_vars(expr, scan_relid) } {
                hits.push((expr_index, unsafe { (*var).varattno }));
            }
        }
        hits
    }

    unsafe fn pg_collect_scan_vars(
        expr: *mut pg_sys::Expr,
        scan_relid: core::ffi::c_int,
    ) -> Vec<*mut pg_sys::Var> {
        if expr.is_null() {
            return Vec::new();
        }
        let flags = (pg_sys::PVC_RECURSE_AGGREGATES
            | pg_sys::PVC_RECURSE_WINDOWFUNCS
            | pg_sys::PVC_RECURSE_PLACEHOLDERS)
            as core::ffi::c_int;
        let vars =
            unsafe { pg_sys::pull_var_clause(expr.cast::<pg_sys::Node>(), flags) };
        if vars.is_null() {
            return Vec::new();
        }
        let len = unsafe { pg_sys::list_length(vars) };
        let mut out = Vec::with_capacity(len as usize);
        for i in 0..len {
            let var = unsafe { pg_sys::list_nth(vars, i) } as *mut pg_sys::Var;
            if var.is_null() {
                continue;
            }
            let belongs_to_scan =
                unsafe { (*var).varno == scan_relid && (*var).varlevelsup == 0 };
            if belongs_to_scan {
                out.push(var);
            }
        }
        out
    }

    /// In-place outer `Var` → `PARAM_EXEC` rewrite (models `replace_nestloop_params`).
    unsafe fn replace_nestloop_params_equivalent(
        expr: *mut pg_sys::Expr,
        scan_relid: core::ffi::c_int,
        next_param_id: &mut core::ffi::c_int,
    ) -> *mut pg_sys::Expr {
        if expr.is_null() {
            return expr;
        }
        let tag = unsafe { (*expr).type_ };
        match tag {
            pg_sys::NodeTag::T_Var => {
                let v = expr as *mut pg_sys::Var;
                if unsafe { (*v).varno } != scan_relid {
                    let pid = *next_param_id;
                    *next_param_id += 1;
                    return unsafe { ExprFixture::param_exec_int4(pid) };
                }
                expr
            }
            pg_sys::NodeTag::T_OpExpr => {
                let op = expr as *mut pg_sys::OpExpr;
                let args = unsafe { (*op).args };
                unsafe {
                    rewrite_args_in_place(args, scan_relid, next_param_id);
                }
                expr
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let be = expr as *mut pg_sys::BoolExpr;
                let args = unsafe { (*be).args };
                unsafe {
                    rewrite_args_in_place(args, scan_relid, next_param_id);
                }
                expr
            }
            pg_sys::NodeTag::T_NullTest => {
                let nt = expr as *mut pg_sys::NullTest;
                let arg = unsafe { (*nt).arg };
                let new_arg = unsafe {
                    replace_nestloop_params_equivalent(arg, scan_relid, next_param_id)
                };
                unsafe { (*nt).arg = new_arg };
                expr
            }
            pg_sys::NodeTag::T_RelabelType => {
                let rl = expr as *mut pg_sys::RelabelType;
                let arg = unsafe { (*rl).arg };
                let new_arg = unsafe {
                    replace_nestloop_params_equivalent(arg, scan_relid, next_param_id)
                };
                unsafe { (*rl).arg = new_arg };
                expr
            }
            _ => expr,
        }
    }

    unsafe fn rewrite_args_in_place(
        list: *mut pg_sys::List,
        scan_relid: core::ffi::c_int,
        next_param_id: &mut core::ffi::c_int,
    ) {
        if list.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(list) };
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(list, i) } as *mut pg_sys::Expr;
            let new_cell = unsafe {
                replace_nestloop_params_equivalent(cell, scan_relid, next_param_id)
            };
            if new_cell != cell {
                let elements = unsafe { (*list).elements };
                debug_assert!(!elements.is_null());
                unsafe {
                    (*elements.offset(i as isize)).ptr_value =
                        new_cell as *mut core::ffi::c_void;
                }
            }
        }
    }

    /// Renumber scan-relation `Var.varno` (models `set_customscan_references`).
    unsafe fn renumber_scan_vars(
        expr: *mut pg_sys::Expr,
        old_relid: core::ffi::c_int,
        new_relid: core::ffi::c_int,
    ) {
        if expr.is_null() {
            return;
        }
        let tag = unsafe { (*expr).type_ };
        match tag {
            pg_sys::NodeTag::T_Var => {
                let v = expr as *mut pg_sys::Var;
                if unsafe { (*v).varno } == old_relid {
                    unsafe { (*v).varno = new_relid };
                }
            }
            pg_sys::NodeTag::T_OpExpr => {
                let op = expr as *mut pg_sys::OpExpr;
                unsafe { renumber_args(op.cast(), old_relid, new_relid) };
            }
            pg_sys::NodeTag::T_BoolExpr => {
                let be = expr as *mut pg_sys::BoolExpr;
                unsafe { renumber_args(be.cast(), old_relid, new_relid) };
            }
            pg_sys::NodeTag::T_NullTest => {
                let nt = expr as *mut pg_sys::NullTest;
                unsafe { renumber_scan_vars((*nt).arg, old_relid, new_relid) };
            }
            pg_sys::NodeTag::T_RelabelType => {
                let rl = expr as *mut pg_sys::RelabelType;
                unsafe { renumber_scan_vars((*rl).arg, old_relid, new_relid) };
            }
            _ => {}
        }
    }

    unsafe fn renumber_args(
        node: *mut pg_sys::Node,
        old_relid: core::ffi::c_int,
        new_relid: core::ffi::c_int,
    ) {
        let tag = unsafe { (*node).type_ };
        let args = match tag {
            pg_sys::NodeTag::T_OpExpr => unsafe {
                (*(node as *mut pg_sys::OpExpr)).args
            },
            pg_sys::NodeTag::T_BoolExpr => unsafe {
                (*(node as *mut pg_sys::BoolExpr)).args
            },
            _ => return,
        };
        if args.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(args) };
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
            unsafe { renumber_scan_vars(cell, old_relid, new_relid) };
        }
    }

    /// `(expr_index, attno)` column identity is stable across nestloop rewrite and setrefs.
    #[pg_test]
    fn walker_column_identity_stable_across_replace_nestloop_params() {
        unsafe {
            let scan_a = ExprFixture::int4_var_at(SCAN_RELID, 1);
            let outer_x = ExprFixture::int4_var_at(OUTER_RELID, 10);
            let leaf_a_eq_outerx =
                ExprFixture::int4_binop(INT4_EQ_OID, scan_a, outer_x);

            let scan_b = ExprFixture::int4_var_at(SCAN_RELID, 2);
            let scan_c = ExprFixture::int4_var_at(SCAN_RELID, 3);
            let leaf_b_lt_c = ExprFixture::int4_binop(INT4_LT_OID, scan_b, scan_c);

            let expr0 = ExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[leaf_a_eq_outerx, leaf_b_lt_c],
            );

            let scan_d = ExprFixture::int4_var_at(SCAN_RELID, 4);
            let scan_e = ExprFixture::int4_var_at(SCAN_RELID, 5);
            let leaf_d_eq_e = ExprFixture::int4_binop(INT4_EQ_OID, scan_d, scan_e);

            let outer_y = ExprFixture::int4_var_at(OUTER_RELID, 11);
            let scan_f = ExprFixture::int4_var_at(SCAN_RELID, 6);
            let leaf_outery_eq_f =
                ExprFixture::int4_binop(INT4_EQ_OID, outer_y, scan_f);

            let expr1 = ExprFixture::bool_expr(
                pg_sys::BoolExprType::OR_EXPR,
                &[leaf_d_eq_e, leaf_outery_eq_f],
            );

            let pushed = vec![expr0, expr1];

            let plan_time = walk_and_collect(&pushed, SCAN_RELID);

            let expected: Vec<(usize, pg_sys::AttrNumber)> =
                vec![(0, 1), (0, 2), (0, 3), (1, 4), (1, 5), (1, 6)];
            assert_eq!(
                plan_time, expected,
                "plan-time walk should emit (expr_index, varattno) in pre-order, \
                 left-to-right, skipping outer Vars ",
            );

            let mut next_param_id: core::ffi::c_int = 1;
            for &expr in &pushed {
                replace_nestloop_params_equivalent(
                    expr,
                    SCAN_RELID,
                    &mut next_param_id,
                );
            }
            assert!(
                next_param_id >= 3,
                "expected replace_nestloop_params to allocate at least 2 fresh \
                 paramids (one per outer Var); got next_param_id = {next_param_id}",
            );

            for &expr in &pushed {
                renumber_scan_vars(expr, SCAN_RELID, POST_SETREFS_RTI);
            }

            let runtime = walk_and_collect(&pushed, POST_SETREFS_RTI);

            assert_eq!(
                runtime, expected,
                "runtime walk after replace_nestloop_params + \
                 set_customscan_references must emit the same \
                 (expr_index, varattno) sequence as plan time \
                 ",
            );

            assert_eq!(
                plan_time, runtime,
                "plan-time and runtime walks must agree pointwise on every \
                 (expr_index, varattno) pair",
            );
        }
    }

    /// PG-free expression recipe for proptest (realized inside `#[pg_test]`).
    #[derive(Clone, Debug)]
    enum ExprSpec {
        ScanVar(pg_sys::AttrNumber),
        OuterVar(pg_sys::AttrNumber),
        Param(core::ffi::c_int),
        Const(i32),
        Op {
            opno: u32,
            left: Box<ExprSpec>,
            right: Box<ExprSpec>,
        },
        And(Vec<ExprSpec>),
        Or(Vec<ExprSpec>),
        Not(Box<ExprSpec>),
        NullTest(Box<ExprSpec>, bool),
    }

    fn arb_expr_spec() -> impl Strategy<Value = ExprSpec> {
        let leaf = prop_oneof![
            (1i16..=20).prop_map(ExprSpec::ScanVar),
            (1i16..=20).prop_map(ExprSpec::OuterVar),
            (1i32..=64).prop_map(ExprSpec::Param),
            any::<i32>().prop_map(ExprSpec::Const),
        ];
        leaf.prop_recursive(
            5,  // up to 5 levels deep
            48, // up to ~48 total nodes
            4,  // up to 4 children per collection
            |inner| {
                prop_oneof![
                    (
                        prop_oneof![Just(INT4_EQ_OID), Just(INT4_LT_OID)],
                        inner.clone(),
                        inner.clone(),
                    )
                        .prop_map(|(opno, l, r)| ExprSpec::Op {
                            opno,
                            left: Box::new(l),
                            right: Box::new(r),
                        }),
                    proptest::collection::vec(inner.clone(), 2..4)
                        .prop_map(ExprSpec::And),
                    proptest::collection::vec(inner.clone(), 2..4)
                        .prop_map(ExprSpec::Or),
                    inner.clone().prop_map(|c| ExprSpec::Not(Box::new(c))),
                    (inner.clone(), any::<bool>())
                        .prop_map(|(c, b)| ExprSpec::NullTest(Box::new(c), b)),
                ]
            },
        )
    }

    /// Oracle: pre-order scan-Var attnos (skips outer Vars, Params, Consts).
    fn collect_scan_attnos(spec: &ExprSpec, out: &mut Vec<pg_sys::AttrNumber>) {
        match spec {
            ExprSpec::ScanVar(a) => out.push(*a),
            ExprSpec::OuterVar(_) | ExprSpec::Param(_) | ExprSpec::Const(_) => {}
            ExprSpec::Op { left, right, .. } => {
                collect_scan_attnos(left, out);
                collect_scan_attnos(right, out);
            }
            ExprSpec::And(items) | ExprSpec::Or(items) => {
                for it in items {
                    collect_scan_attnos(it, out);
                }
            }
            ExprSpec::Not(c) => collect_scan_attnos(c, out),
            ExprSpec::NullTest(c, _) => collect_scan_attnos(c, out),
        }
    }

    /// Realize `ExprSpec` into live PG nodes; `outer_as_param` models nestloop rewrite.
    ///
    /// # Safety
    ///
    /// Must run inside a PG backend (allocates nodes in `CurrentMemoryContext`).
    unsafe fn build_from_spec(
        spec: &ExprSpec,
        outer_as_param: bool,
        next_param_id: &mut core::ffi::c_int,
    ) -> *mut pg_sys::Expr {
        unsafe {
            match spec {
                ExprSpec::ScanVar(a) => ExprFixture::int4_var_at(SCAN_RELID, *a),
                ExprSpec::OuterVar(a) => {
                    if outer_as_param {
                        let pid = *next_param_id;
                        *next_param_id += 1;
                        ExprFixture::param_exec_int4(pid)
                    } else {
                        ExprFixture::int4_var_at(OUTER_RELID, *a)
                    }
                }
                ExprSpec::Param(pid) => ExprFixture::param_exec_int4(*pid),
                ExprSpec::Const(v) => ExprFixture::int4_const(*v),
                ExprSpec::Op { opno, left, right } => {
                    let l = build_from_spec(left, outer_as_param, next_param_id);
                    let r = build_from_spec(right, outer_as_param, next_param_id);
                    ExprFixture::int4_binop(*opno, l, r)
                }
                ExprSpec::And(items) => {
                    let mut args: Vec<*mut pg_sys::Expr> =
                        Vec::with_capacity(items.len());
                    for it in items {
                        args.push(build_from_spec(it, outer_as_param, next_param_id));
                    }
                    ExprFixture::bool_expr(pg_sys::BoolExprType::AND_EXPR, &args)
                }
                ExprSpec::Or(items) => {
                    let mut args: Vec<*mut pg_sys::Expr> =
                        Vec::with_capacity(items.len());
                    for it in items {
                        args.push(build_from_spec(it, outer_as_param, next_param_id));
                    }
                    ExprFixture::bool_expr(pg_sys::BoolExprType::OR_EXPR, &args)
                }
                ExprSpec::Not(c) => {
                    let child = build_from_spec(c, outer_as_param, next_param_id);
                    ExprFixture::bool_expr(pg_sys::BoolExprType::NOT_EXPR, &[child])
                }
                ExprSpec::NullTest(c, is_null) => {
                    let child = build_from_spec(c, outer_as_param, next_param_id);
                    let kind = if *is_null {
                        pg_sys::NullTestType::IS_NULL
                    } else {
                        pg_sys::NullTestType::IS_NOT_NULL
                    };
                    ExprFixture::null_test(child, kind)
                }
            }
        }
    }

    /// Collect scan-column identity via PG core and derive `column_refs`.
    ///
    /// # Safety
    ///
    /// Every pointer in `exprs` must be a live PG `Expr`.
    unsafe fn walk_collect_seq_and_refs(
        exprs: &[*mut pg_sys::Expr],
        scan_relid: core::ffi::c_int,
    ) -> (Vec<(usize, pg_sys::AttrNumber)>, Vec<ColumnRef>) {
        let mut seq: Vec<(usize, pg_sys::AttrNumber)> = Vec::new();
        let mut refs: Vec<ColumnRef> = Vec::new();
        for (expr_index, &expr) in exprs.iter().enumerate() {
            for var in unsafe { pg_collect_scan_vars(expr, scan_relid) } {
                let attno = unsafe { (*var).varattno };
                seq.push((expr_index, attno));
                refs.push(ColumnRef {
                    expr_index,
                    rel_oid: pg_sys::Oid::INVALID,
                    attno,
                    atttypid: unsafe { (*var).vartype },
                    attcollation: unsafe { (*var).varcollid },
                    name: None,
                });
            }
        }
        (seq, refs)
    }

    /// Scan-column identity stable across outer-Var → PARAM_EXEC rewrite.
    fn column_identity_case(specs: &[ExprSpec]) -> Result<(), TestCaseError> {
        unsafe {
            let mut expected: Vec<(usize, pg_sys::AttrNumber)> = Vec::new();
            for (expr_index, spec) in specs.iter().enumerate() {
                let mut attnos: Vec<pg_sys::AttrNumber> = Vec::new();
                collect_scan_attnos(spec, &mut attnos);
                for attno in attnos {
                    expected.push((expr_index, attno));
                }
            }

            let mut orig_param_id: core::ffi::c_int = 1;
            let original: Vec<*mut pg_sys::Expr> = specs
                .iter()
                .map(|s| {
                    build_from_spec(
                        s,
                        /* outer_as_param */ false,
                        &mut orig_param_id,
                    )
                })
                .collect();
            let (orig_seq, orig_refs) =
                walk_collect_seq_and_refs(&original, SCAN_RELID);

            prop_assert_eq!(
                &orig_seq,
                &expected,
                "plan-time walk must emit (expr_index, varattno) in \
                 pre-order, left-to-right, visiting only scan Vars "
            );

            let mut next_param_id: core::ffi::c_int = 1;
            let rewritten: Vec<*mut pg_sys::Expr> = specs
                .iter()
                .map(|s| {
                    let e = build_from_spec(
                        s,
                        /* outer_as_param */ true,
                        &mut next_param_id,
                    );
                    renumber_scan_vars(e, SCAN_RELID, POST_SETREFS_RTI);
                    e
                })
                .collect();
            let (rewritten_seq, rewritten_refs) =
                walk_collect_seq_and_refs(&rewritten, POST_SETREFS_RTI);

            prop_assert_eq!(
                &rewritten_seq,
                &orig_seq,
                "the (expr_index, varattno) sequence must be identical \
                 before and after replace_nestloop_params rewrites outer Vars into \
                 PARAM_EXEC Params "
            );

            prop_assert_eq!(
                &rewritten_refs,
                &orig_refs,
                "column_refs must be identical across the rewrite, with one entry \
                 per scan Var and none for any outer Var "
            );
            prop_assert_eq!(
                orig_refs.len(),
                expected.len(),
                "exactly one column_refs entry per scan Var; none for outer Vars, \
                 Params, or Consts "
            );
        }
        Ok(())
    }

    /// Scan-column identity stability (manual `TestRunner`, >=256 cases).
    #[pg_test]
    fn walker_column_identity_stable_property() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);
        let strategy = proptest::collection::vec(arb_expr_spec(), 1..=4);

        runner
            .run(&strategy, |specs| column_identity_case(&specs))
            .expect(
                "scan-column identity stability across the nestloop-param \
                 rewrite failed",
            );
    }
}
