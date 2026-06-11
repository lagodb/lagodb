//! Backend tests for `collect_param_refs` (the pushed-Param walker).

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::customscan::exec::support::ExecExprFixture;
    use pg_lakebase_core::customscan::exec::collect_param_refs;
    use pg_lakebase_core::expr::runtime_params::{ExecParamRef, ExternParamRef};
    use pgrx::pg_sys;
    use pgrx::pg_test;

    /// EXTERN params go to `extern_params` but not the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_extern_singleton() {
        unsafe {
            let p = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 7);
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(p, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 1);
            assert_eq!(exec_params.len(), 0);
            assert_eq!(extern_params[0].param_id, 7);
            assert!(
                !pg_sys::bms_is_member(7, bm),
                "a PARAM_EXTERN id must not enter the chgParam bitmap",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// EXEC params go to `exec_params` and enter the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_exec_singleton() {
        unsafe {
            let p = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 3);
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(p, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 3);
            assert!(pg_sys::bms_is_member(3, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Walker recurses OpExpr/BoolExpr; EXTERN params skip the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_recurses_into_opexpr_and_boolexpr() {
        unsafe {
            // (var(1) = $1) AND (var(2) = $2)
            let var1 = ExecExprFixture::var(1);
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 1);
            let op1 = ExecExprFixture::op_expr(var1, p1);

            let var2 = ExecExprFixture::var(2);
            let p2 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 2);
            let op2 = ExecExprFixture::op_expr(var2, p2);

            let and = ExecExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[op1, op2],
            );

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(and, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 2);
            assert_eq!(extern_params[0].param_id, 1);
            assert_eq!(extern_params[1].param_id, 2);
            assert!(
                !pg_sys::bms_is_member(1, bm),
                "PARAM_EXTERN id 1 must not enter the chgParam bitmap",
            );
            assert!(
                !pg_sys::bms_is_member(2, bm),
                "PARAM_EXTERN id 2 must not enter the chgParam bitmap",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// Walker recurses through `NullTest` and `RelabelType` wrappers.
    #[pg_test]
    fn collect_param_refs_recurses_into_nulltest_and_relabel() {
        unsafe {
            // RelabelType( $1::int4 ) IS NULL
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 5);
            let labelled = ExecExprFixture::relabel(p1);
            let nt =
                ExecExprFixture::null_test(labelled, pg_sys::NullTestType::IS_NULL);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(nt, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 5);
            assert!(pg_sys::bms_is_member(5, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Walker recurses through expression nodes covered by PG core walker.
    #[pg_test]
    fn collect_param_refs_recurses_into_func_expr() {
        unsafe {
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 8);
            let func = ExecExprFixture::func_expr(p1);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(func, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 8);
            assert!(pg_sys::bms_is_member(8, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Duplicate EXTERN ids stay in `extern_params` but not the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_duplicate_param_ids_dedupe_only_in_bitmap() {
        unsafe {
            // ($1 = $1)
            let p_left = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let p_right = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let op = ExecExprFixture::op_expr(p_left, p_right);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(op, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(
                extern_params.len(),
                2,
                "duplicate param ids surface verbatim in extern_params (Vec carries duplicates)"
            );
            assert_eq!(extern_params[0].param_id, 4);
            assert_eq!(extern_params[1].param_id, 4);
            assert!(
                !pg_sys::bms_is_member(4, bm),
                "a PARAM_EXTERN id must not enter the chgParam bitmap, \
                 even when referenced more than once",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// `Var` and `Const` are leaves; empty bitmap stays NULL (PG convention).
    #[pg_test]
    fn collect_param_refs_var_and_const_are_leaves() {
        unsafe {
            let var = ExecExprFixture::var(1);
            let lit = ExecExprFixture::int4_const(42);
            let op = ExecExprFixture::op_expr(var, lit);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(op, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 0);
            assert!(
                bm.is_null(),
                "an empty bitmap must remain NULL (PG's empty-set convention)"
            );
        }
    }

    /// NULL expression is a no-op (defense in depth for corrupted plan trees).
    #[pg_test]
    fn collect_param_refs_null_is_noop() {
        unsafe {
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();
            collect_param_refs(
                ptr::null_mut(),
                &mut extern_params,
                &mut exec_params,
                &mut bm,
            );
            assert!(extern_params.is_empty());
            assert!(exec_params.is_empty());
            assert!(bm.is_null());
        }
    }
}
