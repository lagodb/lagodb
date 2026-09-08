//! Plan-stage expression bindings and executor-stage runtime value views.

use pgrx::pg_sys;

use crate::expr::{ExprType, RuntimeValueId, RuntimeValueSpec};

/// PostgreSQL expression aligned with one fragment-local value slot.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeValueExpr {
    pub(crate) expr: *mut pg_sys::Expr,
    pub(crate) metadata: RuntimeValueSpec,
}

impl RuntimeValueExpr {
    pub const fn new(expr: *mut pg_sys::Expr, metadata: RuntimeValueSpec) -> Self {
        Self { expr, metadata }
    }

    #[inline]
    pub const fn expr(self) -> *mut pg_sys::Expr {
        self.expr
    }

    #[inline]
    pub const fn metadata(self) -> RuntimeValueSpec {
        self.metadata
    }

    /// Record a planning-time proof that this expression can be compared
    /// exactly in a narrower effective scalar domain.
    pub(crate) fn specialize_value_type(&mut self, value_type: ExprType) {
        self.metadata.value_type = value_type;
    }
}

/// One value evaluated by PostgreSQL for the current Begin/ReScan pass.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeValue {
    datum: pg_sys::Datum,
    is_null: bool,
    metadata: RuntimeValueSpec,
}

impl RuntimeValue {
    /// # Safety
    ///
    /// A non-NULL pass-by-reference `datum` must remain valid for the lifetime
    /// of every [`RuntimeValueBindings`] view containing this value.
    pub unsafe fn from_raw(
        datum: pg_sys::Datum,
        is_null: bool,
        metadata: RuntimeValueSpec,
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
    pub fn metadata(self) -> RuntimeValueSpec {
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

/// Borrowed values for one planned expression, indexed by its local slot id.
#[derive(Clone, Copy)]
pub struct RuntimeValueBindings<'a> {
    values: &'a [RuntimeValue],
}

impl<'a> RuntimeValueBindings<'a> {
    pub fn new(values: &'a [RuntimeValue]) -> Self {
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
    pub fn value(self, id: RuntimeValueId) -> RuntimeValue {
        self.values[id.index()]
    }
}
