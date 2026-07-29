//! Validated plan-data view of `CustomScan.custom_exprs`.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;

pub(crate) fn validate_custom_expr_section_counts(
    list_len: Option<usize>,
    pushed_count: usize,
    recheck_count: usize,
) -> Result<usize, CustomScanError> {
    let total = pushed_count + recheck_count;
    if total == 0 {
        return Ok(total);
    }
    let Some(len) = list_len else {
        return Err(CustomScanError::slice_null_with_nonzero_count(
            pushed_count,
            recheck_count,
        ));
    };
    if len != total {
        return Err(CustomScanError::slice_length_mismatch(len, total));
    }
    Ok(total)
}

/// Runtime view of `CustomScan.custom_exprs`.
///
/// The plan stores pushed expressions first and EPQ recheck expressions second.
/// The counts in `custom_private` are authoritative; this object keeps the
/// boundary explicit after validation so Begin, ReScan, and Explain do not each
/// reimplement the same list slicing rules.
#[doc(hidden)]
pub struct CustomExprSections {
    pushed: Vec<*mut pg_sys::Expr>,
    recheck: Vec<*mut pg_sys::Expr>,
}

impl CustomExprSections {
    /// # Safety
    ///
    /// `list` must be NULL only when both counts are zero, or a live PG
    /// `List<Expr>` with exactly `pushed_count + recheck_count` cells.
    pub unsafe fn from_custom_exprs(
        list: *mut pg_sys::List,
        pushed_count: usize,
        recheck_count: usize,
    ) -> Result<Self, CustomScanError> {
        let list_len = if list.is_null() {
            None
        } else {
            // SAFETY: caller upholds `list` validity.
            Some(unsafe { (*list).length } as usize)
        };
        let total = validate_custom_expr_section_counts(
            list_len,
            pushed_count,
            recheck_count,
        )?;
        if total == 0 {
            return Ok(Self {
                pushed: Vec::new(),
                recheck: Vec::new(),
            });
        }

        let mut pushed = Vec::with_capacity(pushed_count);
        for i in 0..pushed_count {
            // SAFETY: 0 <= i < length.
            let cell =
                unsafe { pg_sys::list_nth(list, i as c_int) } as *mut pg_sys::Expr;
            pushed.push(cell);
        }
        let mut recheck = Vec::with_capacity(recheck_count);
        for i in 0..recheck_count {
            // SAFETY: pushed_count <= pushed_count + i < length.
            let cell = unsafe { pg_sys::list_nth(list, (pushed_count + i) as c_int) }
                as *mut pg_sys::Expr;
            recheck.push(cell);
        }
        Ok(Self { pushed, recheck })
    }

    #[inline]
    pub fn pushed(&self) -> &[*mut pg_sys::Expr] {
        &self.pushed
    }

    #[inline]
    pub fn recheck(&self) -> &[*mut pg_sys::Expr] {
        &self.recheck
    }

    /// Build a PG `List` from the recheck section in the current memory context.
    ///
    /// # Safety
    ///
    /// Recheck expression pointers must be live in the current executor plan.
    pub(crate) unsafe fn recheck_list(&self) -> *mut pg_sys::List {
        unsafe { build_list_from_slice(&self.recheck) }
    }
}

/// Build a PG `List` from expr pointers.
unsafe fn build_list_from_slice(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
    let mut out: *mut pg_sys::List = core::ptr::null_mut();
    for &cell in cells {
        // SAFETY: `lappend` allocates a fresh list cell in the current
        // memory context.
        out = unsafe { pg_sys::lappend(out, cell.cast::<core::ffi::c_void>()) };
    }
    out
}
