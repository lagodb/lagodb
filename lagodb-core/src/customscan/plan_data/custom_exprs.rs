//! Validated plan-data view of `CustomScan.custom_exprs`.

use core::ffi::{c_int, c_void};
use core::ptr;

use pgrx::pg_sys;

/// Invalid PostgreSQL-owned expression-section layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PgExpressionSectionsError {
    #[error(
        "custom_exprs is NULL but binding_count={binding_count} pushed_count={pushed_count}"
    )]
    Missing {
        binding_count: usize,
        pushed_count: usize,
    },
    #[error(
        "custom_exprs length mismatch (got {actual}, expected {binding_count} binding + {pushed_count} pushed expressions)"
    )]
    LengthMismatch {
        actual: usize,
        binding_count: usize,
        pushed_count: usize,
    },
}

/// Runtime view of `CustomScan.custom_exprs`.
///
/// The plan stores binding expressions first and the original expressions for
/// provider-accepted filters second. Exact recheck expressions are selected
/// from the pushed section using the decoded planned-filter contracts.
/// The counts in `custom_private` are authoritative; this object keeps the
/// boundary explicit after validation so Begin, ReScan, and Explain do not each
/// reimplement the same list slicing rules.
#[doc(hidden)]
pub struct PgExpressionSections {
    runtime_bindings: Vec<*mut pg_sys::Expr>,
    relation_pushdown_provenance: Vec<*mut pg_sys::Expr>,
}

impl PgExpressionSections {
    pub(crate) fn validate_counts(
        list_len: Option<usize>,
        binding_count: usize,
        pushed_count: usize,
    ) -> Result<usize, PgExpressionSectionsError> {
        let len = match list_len {
            Some(len) => len,
            None if binding_count == 0 && pushed_count == 0 => return Ok(0),
            None => {
                return Err(PgExpressionSectionsError::Missing {
                    binding_count,
                    pushed_count,
                });
            }
        };
        if binding_count > len || pushed_count != len - binding_count {
            return Err(PgExpressionSectionsError::LengthMismatch {
                actual: len,
                binding_count,
                pushed_count,
            });
        }
        Ok(len)
    }

    /// Build the sole PostgreSQL-owned expression list layout used by relation
    /// and query CustomScans.
    ///
    /// # Safety
    /// Every expression must be live in the current planner memory context.
    pub unsafe fn encode(
        runtime_bindings: &[*mut pg_sys::Expr],
        relation_pushdown_provenance: &[*mut pg_sys::Expr],
    ) -> *mut pg_sys::List {
        let mut list = unsafe { Self::build_list(runtime_bindings) };
        for &expression in relation_pushdown_provenance {
            list = unsafe { pg_sys::lappend(list, expression.cast::<c_void>()) };
        }
        list
    }

    /// # Safety
    ///
    /// `list` must be NULL only when both counts are zero, or a live PG
    /// `List<Expr>` with exactly `binding_count + pushed_count` cells.
    pub unsafe fn from_custom_exprs(
        list: *mut pg_sys::List,
        binding_count: usize,
        pushed_count: usize,
    ) -> Result<Self, PgExpressionSectionsError> {
        let list_len = if list.is_null() {
            None
        } else {
            // SAFETY: caller upholds `list` validity.
            Some(unsafe { (*list).length } as usize)
        };
        let total = Self::validate_counts(list_len, binding_count, pushed_count)?;
        if total == 0 {
            return Ok(Self {
                runtime_bindings: Vec::new(),
                relation_pushdown_provenance: Vec::new(),
            });
        }

        let mut bindings = Vec::with_capacity(binding_count);
        for i in 0..binding_count {
            // SAFETY: 0 <= i < length.
            let cell =
                unsafe { pg_sys::list_nth(list, i as c_int) } as *mut pg_sys::Expr;
            bindings.push(cell);
        }
        let mut pushed = Vec::with_capacity(pushed_count);
        for i in 0..pushed_count {
            // SAFETY: binding_count <= binding_count + i < length.
            let cell = unsafe { pg_sys::list_nth(list, (binding_count + i) as c_int) }
                as *mut pg_sys::Expr;
            pushed.push(cell);
        }
        Ok(Self {
            runtime_bindings: bindings,
            relation_pushdown_provenance: pushed,
        })
    }

    #[inline]
    pub fn runtime_bindings(&self) -> &[*mut pg_sys::Expr] {
        &self.runtime_bindings
    }

    #[inline]
    pub fn relation_pushdown_provenance(&self) -> &[*mut pg_sys::Expr] {
        &self.relation_pushdown_provenance
    }

    /// Build the binding-expression prefix as a PG list for ExprState init.
    ///
    /// # Safety
    ///
    /// Binding expression pointers must be live in the current executor plan.
    pub unsafe fn runtime_binding_list(&self) -> *mut pg_sys::List {
        unsafe { Self::build_list(&self.runtime_bindings) }
    }

    /// Build a PG `List` from expr pointers.
    unsafe fn build_list(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
        let mut out: *mut pg_sys::List = ptr::null_mut();
        for &cell in cells {
            // SAFETY: `lappend` allocates a fresh list cell in the current
            // memory context.
            out = unsafe { pg_sys::lappend(out, cell.cast::<c_void>()) };
        }
        out
    }
}
