//! Helper functions for pg-lakebase-core Memory Context management
//!

use pgrx::{
    memcxt::PgMemoryContexts,
    pg_sys::{AsPgCStr, MemoryContext},
    prelude::*,
};

//  pg-lakebase-core root memory context name
const ROOT_MEMCTX_NAME: &str = "TamRootMemCtx";

// search memory context by name under specified MemoryContext
unsafe fn find_memctx_under(
    name: &str,
    under: PgMemoryContexts,
) -> Option<PgMemoryContexts> {
    unsafe {
        let mut ctx = (*under.value()).firstchild;
        while !ctx.is_null() {
            if let Ok(ctx_name) = std::ffi::CStr::from_ptr((*ctx).name).to_str() {
                if ctx_name == name {
                    return Some(PgMemoryContexts::For(ctx));
                }
            }
            ctx = (*ctx).nextchild;
        }
        None
    }
}

// search for root memory context under CacheMemoryContext, create a new one if not exists
unsafe fn ensure_root_tam_memctx() -> PgMemoryContexts {
    unsafe {
        find_memctx_under(ROOT_MEMCTX_NAME, PgMemoryContexts::CacheMemoryContext)
            .unwrap_or_else(|| {
                // Optimization: Use static string directly, avoiding pstrdup allocation
                let ctx = pg_sys::AllocSetContextCreateExtended(
                    PgMemoryContexts::CacheMemoryContext.value(),
                    ROOT_MEMCTX_NAME.as_pg_cstr(),
                    pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
                );
                PgMemoryContexts::For(ctx)
            })
    }
}

/// Create a new memory context under TamRootMemCtx.
/// The `name` is allocated in the root context and must be freed in `delete_tam_memctx`.
pub(super) unsafe fn create_tam_memctx(name: &str) -> MemoryContext {
    unsafe {
        let mut root = ensure_root_tam_memctx();
        // Allocate name in root context so it outlives the new context but is managed
        let name = root.switch_to(|_| name.as_pg_cstr());
        pg_sys::AllocSetContextCreateExtended(
            root.value(),
            name,
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        )
    }
}

/// Delete a memory context created by `create_tam_memctx`.
/// This function manually frees the context name to prevent leaks in the root context.
pub(super) unsafe fn delete_tam_memctx(ctx: MemoryContext) {
    unsafe {
        if !ctx.is_null() {
            // Free the name string which was allocated in the root context
            pg_sys::pfree((*ctx).name as _);
            pg_sys::MemoryContextDelete(ctx)
        }
    }
}
