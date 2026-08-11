//! Plan-stage binding expressions and executor-stage value views.

use pgrx::pg_sys;

use super::FilterValueSlot;

/// PostgreSQL expression aligned with one fragment-local value slot.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterBindingExpr {
    pub expr: *mut pg_sys::Expr,
    pub metadata: FilterValueSlot,
}

/// One value evaluated by PostgreSQL for the current Begin/ReScan pass.
#[derive(Debug, Clone, Copy)]
pub struct FilterValue {
    datum: pg_sys::Datum,
    is_null: bool,
    metadata: FilterValueSlot,
}

impl FilterValue {
    /// # Safety
    ///
    /// A non-NULL pass-by-reference `datum` must remain valid for the lifetime
    /// of every [`FilterValueBindings`] view containing this value.
    pub(crate) unsafe fn from_raw(
        datum: pg_sys::Datum,
        is_null: bool,
        metadata: FilterValueSlot,
    ) -> Self {
        Self {
            datum,
            is_null,
            metadata,
        }
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.is_null
    }

    #[inline]
    pub fn metadata(self) -> FilterValueSlot {
        self.metadata
    }

    /// # Safety
    ///
    /// A pass-by-reference datum must not be retained after the provider bind
    /// callback returns unless copied with the PostgreSQL type's semantics.
    #[inline]
    pub unsafe fn datum(self) -> pg_sys::Datum {
        self.datum
    }
}

/// Borrowed values for one planned predicate, indexed by local slot id.
#[derive(Clone, Copy)]
pub struct FilterValueBindings<'a> {
    values: &'a [FilterValue],
}

impl<'a> FilterValueBindings<'a> {
    pub(crate) fn new(values: &'a [FilterValue]) -> Self {
        Self { values }
    }

    #[inline]
    pub fn len(self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn value(self, id: super::FilterValueSlotId) -> FilterValue {
        self.values[id.index()]
    }
}
