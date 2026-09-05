//! Explain metadata for one query table-scan predicate.

use std::ffi::CStr;

/// Exact query filter text paired with the provider planning disposition.
#[derive(Clone, Copy)]
pub struct TableScanFilterExplain<'plan> {
    exact_expression: &'plan CStr,
    pushed_expression: Option<&'plan CStr>,
}

impl<'plan> TableScanFilterExplain<'plan> {
    pub const fn new(
        exact_expression: &'plan CStr,
        pushed_expression: Option<&'plan CStr>,
    ) -> Self {
        Self {
            exact_expression,
            pushed_expression,
        }
    }

    #[inline]
    pub const fn exact_expression(self) -> &'plan CStr {
        self.exact_expression
    }

    #[inline]
    pub const fn pushed_expression(self) -> Option<&'plan CStr> {
        self.pushed_expression
    }
}
