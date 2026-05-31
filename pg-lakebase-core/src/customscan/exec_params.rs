//! Executor-side Param reference collection for pushed `custom_exprs`.

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::expr::runtime_params::{
    ExecParamRef, ExternParamRef, RuntimeParamResolver,
};

pub(super) struct RuntimeParamRefs {
    extern_params: Vec<ExternParamRef>,
    exec_params: Vec<ExecParamRef>,
    exec_param_ids: *mut pg_sys::Bitmapset,
}

impl RuntimeParamRefs {
    fn new() -> Self {
        Self {
            extern_params: Vec::new(),
            exec_params: Vec::new(),
            exec_param_ids: ptr::null_mut(),
        }
    }

    /// # Safety
    ///
    /// Every expression pointer must be NULL or live in the current plan tree.
    pub(super) unsafe fn collect_from_exprs(exprs: &[*mut pg_sys::Expr]) -> Self {
        let mut refs = Self::new();
        for &expr in exprs {
            unsafe { refs.collect_from_expr(expr) };
        }
        refs
    }

    /// # Safety
    ///
    /// `expr` must be NULL or a live expression tree.
    unsafe fn collect_from_expr(&mut self, expr: *mut pg_sys::Expr) {
        unsafe {
            collect_param_refs(
                expr,
                &mut self.extern_params,
                &mut self.exec_params,
                &mut self.exec_param_ids,
            );
        }
    }

    /// Copy the EXEC-param bitmap into `target_ctx`. The original allocation is
    /// intentionally left to its current PG memory context, matching the prior
    /// BeginCustomScan behavior.
    ///
    /// # Safety
    ///
    /// `target_ctx` must be a live PostgreSQL memory context.
    pub(super) unsafe fn relocate_exec_param_ids_to(
        &mut self,
        target_ctx: pg_sys::MemoryContext,
    ) {
        self.exec_param_ids =
            unsafe { relocate_bitmap_to_context(self.exec_param_ids, target_ctx) };
    }

    #[inline]
    pub(super) fn exec_param_ids(&self) -> *mut pg_sys::Bitmapset {
        self.exec_param_ids
    }

    /// # Safety
    ///
    /// `estate` and `econtext` must be the live executor state for this scan.
    pub(super) unsafe fn resolve(
        &self,
        estate: *mut pg_sys::EState,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<Vec<crate::expr::nodes::PgParamValue>, PgReportError> {
        unsafe {
            RuntimeParamResolver::new(estate, econtext)
                .resolve(&self.extern_params, &self.exec_params)
        }
    }

    pub(super) unsafe fn free_exec_param_ids(&mut self) {
        if !self.exec_param_ids.is_null() {
            unsafe { pg_sys::bms_free(self.exec_param_ids) };
            self.exec_param_ids = ptr::null_mut();
        }
    }
}

/// Walk pushed expr for Param refs; only PARAM_EXEC ids enter the chgParam bitmap.
#[doc(hidden)]
pub unsafe fn collect_param_refs(
    expr: *mut pg_sys::Expr,
    extern_params: &mut Vec<ExternParamRef>,
    exec_params: &mut Vec<ExecParamRef>,
    pushed_param_ids: &mut *mut pg_sys::Bitmapset,
) {
    let mut state = ParamRefsCollector {
        extern_params,
        exec_params,
        pushed_param_ids,
    };
    unsafe {
        param_refs_walker(
            expr.cast::<pg_sys::Node>(),
            (&mut state as *mut ParamRefsCollector).cast(),
        );
    }
}

struct ParamRefsCollector {
    extern_params: *mut Vec<ExternParamRef>,
    exec_params: *mut Vec<ExecParamRef>,
    pushed_param_ids: *mut *mut pg_sys::Bitmapset,
}

#[pg_guard]
unsafe extern "C-unwind" fn param_refs_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let state = unsafe { &mut *(context.cast::<ParamRefsCollector>()) };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Param => {
            let param = node.cast::<pg_sys::Param>();
            let param_id = unsafe { (*param).paramid };
            let paramtype = unsafe { (*param).paramtype };
            let paramcollid = unsafe { (*param).paramcollid };
            match unsafe { (*param).paramkind } {
                pg_sys::ParamKind::PARAM_EXTERN => unsafe {
                    (*state.extern_params).push(ExternParamRef {
                        param_id,
                        expected_type: paramtype,
                        collid: paramcollid,
                    });
                },
                pg_sys::ParamKind::PARAM_EXEC => unsafe {
                    (*state.exec_params).push(ExecParamRef {
                        param_id,
                        expected_type: paramtype,
                        collid: paramcollid,
                    });
                    *state.pushed_param_ids =
                        pg_sys::bms_add_member(*state.pushed_param_ids, param_id);
                },
                _ => {}
            }
            false
        }
        pg_sys::NodeTag::T_RestrictInfo => {
            let rinfo = node.cast::<pg_sys::RestrictInfo>();
            let clause = unsafe { (*rinfo).clause }.cast::<pg_sys::Node>();
            unsafe { param_refs_walker(clause, context) }
        }
        _ => unsafe {
            pg_sys::expression_tree_walker(node, Some(param_refs_walker), context)
        },
    }
}

/// Copy param bitmap into `target_ctx`. `bitmap` may be NULL.
unsafe fn relocate_bitmap_to_context(
    bitmap: *mut pg_sys::Bitmapset,
    target_ctx: pg_sys::MemoryContext,
) -> *mut pg_sys::Bitmapset {
    if bitmap.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: caller upholds `target_ctx` validity.
    let old = unsafe { pg_sys::MemoryContextSwitchTo(target_ctx) };
    let copied = unsafe { pg_sys::bms_copy(bitmap) };
    let _ = unsafe { pg_sys::MemoryContextSwitchTo(old) };
    copied
}
