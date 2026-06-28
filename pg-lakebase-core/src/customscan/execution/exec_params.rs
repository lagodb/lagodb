//! Executor-side parameter reference collection for pushed `custom_exprs`.

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::expr::runtime_params::{
    ExecParamRef, ExternParamRef, RuntimeParamResolver,
};

/// Deduplicated parameter references and the EXEC-param change bitmap for one scan.
#[doc(hidden)]
pub struct RuntimeParamRefs {
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
    pub unsafe fn collect_from_exprs(exprs: &[*mut pg_sys::Expr]) -> Self {
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
            let mut collector = ParamRefsCollector { refs: self };
            param_refs_walker(
                expr.cast::<pg_sys::Node>(),
                (&mut collector as *mut ParamRefsCollector).cast(),
            );
        }
    }

    /// Move the EXEC-param bitmap into `target_ctx`, freeing the original copy.
    ///
    /// # Safety
    ///
    /// `target_ctx` must be a live PostgreSQL memory context.
    pub unsafe fn relocate_exec_param_ids_to(
        &mut self,
        target_ctx: pg_sys::MemoryContext,
    ) {
        let original = self.exec_param_ids;
        self.exec_param_ids =
            unsafe { relocate_bitmap_to_context(self.exec_param_ids, target_ctx) };
        if !original.is_null() {
            unsafe { pg_sys::bms_free(original) };
        }
    }

    #[inline]
    pub fn exec_param_ids(&self) -> *mut pg_sys::Bitmapset {
        self.exec_param_ids
    }

    pub fn extern_params(&self) -> &[ExternParamRef] {
        &self.extern_params
    }

    pub fn exec_params(&self) -> &[ExecParamRef] {
        &self.exec_params
    }

    /// Return whether any referenced EXEC parameter changed.
    ///
    /// # Safety
    ///
    /// `chg_param` must be NULL or point to a live PostgreSQL `Bitmapset`.
    pub unsafe fn changed(&self, chg_param: *mut pg_sys::Bitmapset) -> bool {
        unsafe { pg_sys::bms_overlap(chg_param, self.exec_param_ids) }
    }

    /// # Safety
    ///
    /// `estate` and `econtext` must be the live executor state for this scan.
    pub unsafe fn resolve(
        &self,
        estate: *mut pg_sys::EState,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<Vec<crate::expr::nodes::PgParamValue>, PgReportError> {
        unsafe {
            RuntimeParamResolver::new(estate, econtext)
                .resolve(&self.extern_params, &self.exec_params)
        }
    }

    fn record_extern(&mut self, entry: ExternParamRef) {
        if let Some(existing) = self
            .extern_params
            .iter()
            .find(|existing| existing.param_id == entry.param_id)
        {
            debug_assert_eq!(existing.expected_type, entry.expected_type);
            debug_assert_eq!(existing.collid, entry.collid);
            return;
        }
        self.extern_params.push(entry);
    }

    unsafe fn record_exec(&mut self, entry: ExecParamRef) {
        if let Some(existing) = self
            .exec_params
            .iter()
            .find(|existing| existing.param_id == entry.param_id)
        {
            debug_assert_eq!(existing.expected_type, entry.expected_type);
            debug_assert_eq!(existing.collid, entry.collid);
            return;
        }
        self.exec_param_ids =
            unsafe { pg_sys::bms_add_member(self.exec_param_ids, entry.param_id) };
        self.exec_params.push(entry);
    }

    fn free_exec_param_ids(&mut self) {
        if !self.exec_param_ids.is_null() {
            unsafe { pg_sys::bms_free(self.exec_param_ids) };
            self.exec_param_ids = ptr::null_mut();
        }
    }
}

impl Drop for RuntimeParamRefs {
    fn drop(&mut self) {
        self.free_exec_param_ids();
    }
}

struct ParamRefsCollector {
    refs: *mut RuntimeParamRefs,
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
                    (*state.refs).record_extern(ExternParamRef {
                        param_id,
                        expected_type: paramtype,
                        collid: paramcollid,
                    });
                },
                pg_sys::ParamKind::PARAM_EXEC => unsafe {
                    (*state.refs).record_exec(ExecParamRef {
                        param_id,
                        expected_type: paramtype,
                        collid: paramcollid,
                    });
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
