//! Validated plan-data view of `CustomScan.custom_exprs`.

use core::ffi::{c_int, c_void};
use core::ptr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;

pub(crate) fn validate_custom_expr_section_counts(
    list_len: Option<usize>,
    binding_count: usize,
    pushed_count: usize,
) -> Result<usize, CustomScanError> {
    let total = binding_count + pushed_count;
    if total == 0 {
        return Ok(total);
    }
    let Some(len) = list_len else {
        return Err(CustomScanError::custom_exprs_missing(
            binding_count,
            pushed_count,
        ));
    };
    if len != total {
        return Err(CustomScanError::custom_exprs_length_mismatch(len, total));
    }
    Ok(total)
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
pub struct CustomExprSections {
    bindings: Vec<*mut pg_sys::Expr>,
    pushed: Vec<*mut pg_sys::Expr>,
}

impl CustomExprSections {
    /// # Safety
    ///
    /// `list` must be NULL only when both counts are zero, or a live PG
    /// `List<Expr>` with exactly `binding_count + pushed_count` cells.
    pub unsafe fn from_custom_exprs(
        list: *mut pg_sys::List,
        binding_count: usize,
        pushed_count: usize,
    ) -> Result<Self, CustomScanError> {
        let list_len = if list.is_null() {
            None
        } else {
            // SAFETY: caller upholds `list` validity.
            Some(unsafe { (*list).length } as usize)
        };
        let total = validate_custom_expr_section_counts(
            list_len,
            binding_count,
            pushed_count,
        )?;
        if total == 0 {
            return Ok(Self {
                bindings: Vec::new(),
                pushed: Vec::new(),
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
        Ok(Self { bindings, pushed })
    }

    #[inline]
    pub fn bindings(&self) -> &[*mut pg_sys::Expr] {
        &self.bindings
    }

    #[inline]
    pub fn pushed(&self) -> &[*mut pg_sys::Expr] {
        &self.pushed
    }

    /// Build the binding-expression prefix as a PG list for ExprState init.
    ///
    /// # Safety
    ///
    /// Binding expression pointers must be live in the current executor plan.
    pub(crate) unsafe fn binding_list(&self) -> *mut pg_sys::List {
        unsafe { build_list_from_slice(&self.bindings) }
    }
}

/// Build a PG `List` from expr pointers.
unsafe fn build_list_from_slice(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
    let mut out: *mut pg_sys::List = ptr::null_mut();
    for &cell in cells {
        // SAFETY: `lappend` allocates a fresh list cell in the current
        // memory context.
        out = unsafe { pg_sys::lappend(out, cell.cast::<c_void>()) };
    }
    out
}
