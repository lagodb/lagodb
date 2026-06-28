//! Backend tests for the pushed-expression parameter domain.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::lakebase_core::customscan::exec::support::ExecExprFixture;
    use pg_lakebase_core::customscan::exec::RuntimeParamRefs;
    use pgrx::{pg_sys, pg_test};

    unsafe fn collect(expr: *mut pg_sys::Expr) -> RuntimeParamRefs {
        unsafe { RuntimeParamRefs::collect_from_exprs(&[expr]) }
    }

    #[pg_test]
    fn separates_extern_and_exec_params() {
        unsafe {
            let external = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 7);
            let executable = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 3);
            let expr = ExecExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[external, executable],
            );
            let refs = collect(expr);

            assert_eq!(refs.extern_params()[0].param_id, 7);
            assert_eq!(refs.exec_params()[0].param_id, 3);
            assert!(pg_sys::bms_is_member(3, refs.exec_param_ids()));
            assert!(!pg_sys::bms_is_member(7, refs.exec_param_ids()));
        }
    }

    #[pg_test]
    fn walks_common_expression_wrappers() {
        unsafe {
            let param = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 5);
            let relabelled = ExecExprFixture::relabel(param);
            let null_test =
                ExecExprFixture::null_test(relabelled, pg_sys::NullTestType::IS_NULL);
            let wrapped = ExecExprFixture::func_expr(null_test);
            let refs = collect(wrapped);

            assert_eq!(refs.exec_params().len(), 1);
            assert_eq!(refs.exec_params()[0].param_id, 5);
        }
    }

    #[pg_test]
    fn deduplicates_repeated_param_ids() {
        unsafe {
            let left = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let right = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let refs = collect(ExecExprFixture::op_expr(left, right));

            assert_eq!(refs.extern_params().len(), 1);
            assert_eq!(refs.extern_params()[0].param_id, 4);
            assert!(refs.exec_param_ids().is_null());
        }
    }

    #[pg_test]
    fn non_param_expression_has_no_refs() {
        unsafe {
            let expr = ExecExprFixture::op_expr(
                ExecExprFixture::var(1),
                ExecExprFixture::int4_const(42),
            );
            let refs = collect(expr);

            assert!(refs.extern_params().is_empty());
            assert!(refs.exec_params().is_empty());
            assert!(refs.exec_param_ids().is_null());
        }
    }

    #[pg_test]
    fn null_expression_is_empty() {
        unsafe {
            let refs = collect(core::ptr::null_mut());
            assert!(refs.extern_params().is_empty());
            assert!(refs.exec_params().is_empty());
            assert!(refs.exec_param_ids().is_null());
        }
    }
}
