//! Shared synthetic-node fixtures for the `exec` backend tests.
//!
//! Only fixtures used by more than one `exec` submodule live here:
//! - [`ExecExprFixture`] (used by `slice`).
//!
//! Group-local fixtures stay in their own submodule.

use crate::lakebase_core::support::pg::PgNodeBuilder;
use pgrx::pg_sys;

/// Builder facade over [`PgNodeBuilder`] for expression-walker fixtures.
pub(crate) struct ExecExprFixture;

impl ExecExprFixture {
    fn nodes() -> PgNodeBuilder {
        PgNodeBuilder::new(1)
    }

    pub(crate) unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
        unsafe { Self::nodes().int4_const(value) }
    }

    pub(crate) unsafe fn expr_list(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
        unsafe { Self::nodes().expr_list(cells) }
    }
}
