//! Shared synthetic-node fixtures for the `exec` backend tests.
//!
//! Only fixtures used by more than one `exec` submodule live here:
//! - [`ExecExprFixture`] (used by `slice` and `param_refs`),
//! - [`make_estate_stub`] / [`make_econtext_stub`] (used by `rescan` and
//!   `runtime_params`).
//!
//! Group-local fixtures stay in their own submodule.

use core::ffi::c_int;

use crate::lakebase_core::support::pg::{INT4_EQ_OPNO, PgNodeBuilder};
use pgrx::pg_sys;

/// Builder facade over [`PgNodeBuilder`] for expression-walker fixtures.
pub(crate) struct ExecExprFixture;

impl ExecExprFixture {
    fn nodes() -> PgNodeBuilder {
        PgNodeBuilder::new(1)
    }

    pub(crate) unsafe fn param(
        kind: pg_sys::ParamKind::Type,
        param_id: c_int,
    ) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_param(kind, param_id) }
    }

    pub(crate) unsafe fn var(attno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_var(attno) }
    }

    pub(crate) unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_const(value) }
    }

    pub(crate) unsafe fn op_expr(
        left: *mut pg_sys::Expr,
        right: *mut pg_sys::Expr,
    ) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_op_clause(INT4_EQ_OPNO, left, right) }
    }

    pub(crate) unsafe fn bool_expr(
        boolop: pg_sys::BoolExprType::Type,
        children: &[*mut pg_sys::Expr],
    ) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().bool_expr(boolop, children) }
    }

    pub(crate) unsafe fn null_test(
        arg: *mut pg_sys::Expr,
        nulltesttype: pg_sys::NullTestType::Type,
    ) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().null_test(arg, nulltesttype) }
    }

    pub(crate) unsafe fn relabel(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().relabel_int4(arg) }
    }

    pub(crate) unsafe fn func_expr(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_func_expr(arg) }
    }

    pub(crate) unsafe fn expr_list(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
        unsafe { Self::nodes().expr_list(cells) }
    }
}

/// `EState` stub with `es_param_list_info` and `es_snapshot` set.
pub(crate) unsafe fn make_estate_stub(
    param_list_info: pg_sys::ParamListInfo,
) -> *mut pg_sys::EState {
    unsafe {
        let estate = pg_sys::palloc0(core::mem::size_of::<pg_sys::EState>())
            as *mut pg_sys::EState;
        (*estate).type_ = pg_sys::NodeTag::T_EState;
        (*estate).es_param_list_info = param_list_info;
        // Zeroed SnapshotData shell is sufficient; the stub provider never dereferences it.
        let snapshot = pg_sys::palloc0(core::mem::size_of::<pg_sys::SnapshotData>())
            as pg_sys::Snapshot;
        (*estate).es_snapshot = snapshot;
        estate
    }
}

/// `ExprContext` stub; per-tuple memory is the test's current context.
pub(crate) unsafe fn make_econtext_stub() -> *mut pg_sys::ExprContext {
    unsafe {
        let econtext = pg_sys::palloc0(core::mem::size_of::<pg_sys::ExprContext>())
            as *mut pg_sys::ExprContext;
        (*econtext).type_ = pg_sys::NodeTag::T_ExprContext;
        (*econtext).ecxt_per_tuple_memory = pg_sys::CurrentMemoryContext;
        (*econtext).ecxt_per_query_memory = pg_sys::CurrentMemoryContext;
        econtext
    }
}
