//! Executor-side `PARAM_EXEC` dependency tracking for binding expressions.

use core::ffi::c_void;
use core::ptr;

use pgrx::{pg_guard, pg_sys};

/// `PARAM_EXEC` dependency bitmap for one binding-expression set.
pub(crate) struct RuntimeParamRefs {
    exec_param_ids: *mut pg_sys::Bitmapset,
}

impl RuntimeParamRefs {
    fn new() -> Self {
        Self {
            exec_param_ids: ptr::null_mut(),
        }
    }

    /// # Safety
    ///
    /// `exprs` must be NIL or a live plan-owned `List<Expr>`.
    pub(crate) unsafe fn collect_from_list(exprs: *mut pg_sys::List) -> Self {
        let mut refs = Self::new();
        if exprs.is_null() {
            return refs;
        }
        let length = unsafe { pg_sys::list_length(exprs) };
        for index in 0..length {
            let expr = unsafe { pg_sys::list_nth(exprs, index) } as *mut pg_sys::Expr;
            unsafe { refs.collect_from_expr(expr) };
        }
        refs
    }

    unsafe fn collect_from_expr(&mut self, expr: *mut pg_sys::Expr) {
        let mut collector = ParamRefsCollector { refs: self };
        unsafe {
            param_refs_walker(
                expr.cast::<pg_sys::Node>(),
                (&mut collector as *mut ParamRefsCollector).cast(),
            );
        }
    }

    /// # Safety
    ///
    /// `target_ctx` must be a live PostgreSQL memory context.
    pub(crate) unsafe fn relocate_exec_param_ids_to(
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

    /// # Safety
    ///
    /// `chg_param` must be NULL or point to a live PostgreSQL `Bitmapset`.
    pub(crate) unsafe fn changed(&self, chg_param: *mut pg_sys::Bitmapset) -> bool {
        unsafe { pg_sys::bms_overlap(chg_param, self.exec_param_ids) }
    }

    fn record_exec(&mut self, param_id: core::ffi::c_int) {
        self.exec_param_ids =
            unsafe { pg_sys::bms_add_member(self.exec_param_ids, param_id) };
    }
}

impl Drop for RuntimeParamRefs {
    fn drop(&mut self) {
        if !self.exec_param_ids.is_null() {
            unsafe { pg_sys::bms_free(self.exec_param_ids) };
            self.exec_param_ids = ptr::null_mut();
        }
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
            if unsafe { (*param).paramkind } == pg_sys::ParamKind::PARAM_EXEC {
                unsafe { (*state.refs).record_exec((*param).paramid) };
            }
            false
        }
        _ => unsafe {
            pg_sys::expression_tree_walker(node, Some(param_refs_walker), context)
        },
    }
}

unsafe fn relocate_bitmap_to_context(
    bitmap: *mut pg_sys::Bitmapset,
    target_ctx: pg_sys::MemoryContext,
) -> *mut pg_sys::Bitmapset {
    if bitmap.is_null() {
        return ptr::null_mut();
    }
    let old = unsafe { pg_sys::MemoryContextSwitchTo(target_ctx) };
    let copied = unsafe { pg_sys::bms_copy(bitmap) };
    let _ = unsafe { pg_sys::MemoryContextSwitchTo(old) };
    copied
}
