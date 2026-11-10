//! `expression_tree_mutator` across the supported PostgreSQL versions.
//!
//! PostgreSQL 16 renamed the function to `expression_tree_mutator_impl` and
//! gave its callback a prototype; PostgreSQL 15 declares the callback
//! unprototyped, so its binding takes a parameterless function pointer.

use std::ffi::c_void;

use pgrx::pg_sys;

/// The callback PostgreSQL calls with each child node and the context.
pub type ExprMutator =
    unsafe extern "C-unwind" fn(*mut pg_sys::Node, *mut c_void) -> *mut pg_sys::Node;

/// PostgreSQL `expression_tree_mutator`: a copy of `node` whose children are
/// replaced by `mutator`'s results.
///
/// # Safety
///
/// `node` must be NULL or a live expression tree, and `context` must be what
/// `mutator` expects.
pub unsafe fn expression_tree_mutator(
    node: *mut pg_sys::Node,
    mutator: ExprMutator,
    context: *mut c_void,
) -> *mut pg_sys::Node {
    #[cfg(feature = "pg15")]
    {
        // SAFETY: PostgreSQL 15 declares the callback as `Node *(*) ()` and
        // calls it as `mutator(node, context)`, the prototype of
        // `ExprMutator`; only the binding's spelling of the type differs.
        let mutator = unsafe {
            std::mem::transmute::<
                ExprMutator,
                unsafe extern "C-unwind" fn() -> *mut pg_sys::Node,
            >(mutator)
        };
        unsafe { pg_sys::expression_tree_mutator(node, Some(mutator), context) }
    }
    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    unsafe {
        pg_sys::expression_tree_mutator_impl(node, Some(mutator), context)
    }
}
