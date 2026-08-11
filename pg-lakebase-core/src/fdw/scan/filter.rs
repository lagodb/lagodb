//! ForeignScan adapter for planned-filter persistence and runtime binding.

use core::ffi::c_void;
use core::ptr;
use std::error::Error;

use pgrx::pg_sys;

use crate::diag::SqlStateError;
use crate::expr::pushdown::{
    BoundFilterSet, EncodedFilterData, FilterDataCodec, FilterDataError,
    FilterPushdown, NegotiatedFilterSet, RuntimeFilterError, RuntimeFilterState,
};

use super::error::ForeignScanError;
use super::private::DecodedScanPrivate;

pub(crate) struct ForeignScanFilters<P: FilterPushdown> {
    runtime: RuntimeFilterState<P>,
}

pub(crate) struct ForeignFilterExprs {
    pub(crate) bindings: *mut pg_sys::List,
    pub(crate) provider: *mut pg_sys::List,
}

impl ForeignFilterExprs {
    /// Split the framework binding prefix from provider activation expressions.
    ///
    /// # Safety
    ///
    /// `expressions` must be NIL or a live plan-owned `List<Expr>`.
    pub(crate) unsafe fn split(
        expressions: *mut pg_sys::List,
        binding_count: usize,
    ) -> Result<Self, ForeignScanError> {
        let length = if expressions.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(expressions) as usize }
        };
        if binding_count > length {
            return Err(ForeignScanError::framework(
                "FDW filter binding prefix exceeds fdw_exprs length",
            ));
        }
        let mut bindings = ptr::null_mut();
        let mut provider = ptr::null_mut();
        for index in 0..length {
            let expression = unsafe { pg_sys::list_nth(expressions, index as i32) };
            if index < binding_count {
                bindings =
                    unsafe { pg_sys::lappend(bindings, expression.cast::<c_void>()) };
            } else {
                provider =
                    unsafe { pg_sys::lappend(provider, expression.cast::<c_void>()) };
            }
        }
        Ok(Self { bindings, provider })
    }
}

impl<P: FilterPushdown> ForeignScanFilters<P> {
    pub(crate) fn encode(
        filters: &NegotiatedFilterSet<P::PlannedPredicate>,
    ) -> Result<EncodedFilterData, ForeignScanError> {
        Ok(FilterDataCodec::<P>::encode(filters)?)
    }

    /// # Safety
    ///
    /// The decoded lists and binding expressions must belong to the live
    /// ForeignScan plan, and `parent` must be its initialized PlanState.
    pub(crate) unsafe fn initialize<D>(
        decoded: &DecodedScanPrivate<D>,
        binding_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, ForeignScanError> {
        let (planned, bindings) = unsafe {
            FilterDataCodec::<P>::decode(
                decoded.planned_filters_raw,
                decoded.planned_filter_count,
                decoded.binding_slots_raw,
                decoded.binding_count,
            )
        }?;
        let runtime = unsafe {
            RuntimeFilterState::<P>::initialize(
                planned,
                bindings,
                binding_exprs,
                parent,
            )
        }?;
        Ok(Self { runtime })
    }

    pub(crate) unsafe fn bind_initial(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.runtime.bind_initial(econtext) }?;
        Ok(())
    }

    pub(crate) unsafe fn rebind_dynamic(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.runtime.rebind_dynamic(econtext) }?;
        Ok(())
    }

    pub(crate) unsafe fn filters_changed(
        &self,
        chg_param: *mut pg_sys::Bitmapset,
    ) -> bool {
        unsafe { self.runtime.filters_changed(chg_param) }
    }

    pub(crate) fn bound(&self) -> BoundFilterSet<'_, P::BoundPredicate> {
        self.runtime.bound()
    }

    pub(crate) fn recheck_count(&self) -> usize {
        self.runtime.recheck_count()
    }
}

impl<E> From<FilterDataError<E>> for ForeignScanError
where
    E: SqlStateError + Error + Send + Sync + 'static,
{
    fn from(error: FilterDataError<E>) -> Self {
        match error {
            FilterDataError::PlanData(error) => error.into(),
            FilterDataError::Provider(error) => ForeignScanError::provider(error),
            FilterDataError::Invalid(error) => ForeignScanError::framework(error),
        }
    }
}

impl<E> From<RuntimeFilterError<E>> for ForeignScanError
where
    E: SqlStateError + Error + Send + Sync + 'static,
{
    fn from(error: RuntimeFilterError<E>) -> Self {
        match error {
            RuntimeFilterError::Provider(error) => ForeignScanError::provider(error),
            RuntimeFilterError::BindingCountMismatch
            | RuntimeFilterError::ExactValueNotRepresentable { .. } => {
                ForeignScanError::framework(error)
            }
        }
    }
}
