//! CustomScan adapter for planned-filter persistence and executor binding.

use core::{ffi::c_void, ptr};
use std::error::Error;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::custom_private::EncodedPrivate;
use crate::diag::SqlStateError;
use crate::expr::contract::PushdownContract;
use crate::expr::pushdown::{
    BoundFilterSet, EncodedFilterData, FilterDataCodec, FilterDataError,
    FilterPushdown, NegotiatedFilterSet, RuntimeFilterError, RuntimeFilterState,
};

pub(crate) struct CustomScanFilters<P: FilterPushdown> {
    runtime: RuntimeFilterState<P>,
}

pub(crate) struct FilterExplainContracts {
    contracts: Box<[PushdownContract]>,
}

impl FilterExplainContracts {
    pub(crate) fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = PushdownContract> + '_ {
        self.contracts.iter().copied()
    }
}

impl<P: FilterPushdown> CustomScanFilters<P> {
    pub(crate) fn encode(
        filters: &NegotiatedFilterSet<P::PlannedPredicate>,
    ) -> Result<EncodedFilterData, CustomScanError> {
        Ok(FilterDataCodec::<P>::encode(filters)?)
    }

    /// Decode the planned contracts used to classify pushed EXPLAIN expressions.
    ///
    /// # Safety
    ///
    /// The envelope lists must belong to the live CustomScan plan.
    pub(crate) unsafe fn decode_explain_contracts(
        envelope: &EncodedPrivate,
    ) -> Result<FilterExplainContracts, CustomScanError> {
        let (planned, _) = unsafe {
            FilterDataCodec::<P>::decode(
                envelope.planned_filters_raw,
                envelope.planned_filter_count,
                envelope.binding_slots_raw,
                envelope.binding_count,
            )
        }?;
        Ok(FilterExplainContracts {
            contracts: planned.into_iter().map(|filter| filter.contract).collect(),
        })
    }

    pub(crate) fn explain_contracts(&self) -> FilterExplainContracts {
        FilterExplainContracts {
            contracts: self.runtime.contracts().collect(),
        }
    }

    /// Build the EPQ recheck list from original pushed expressions whose
    /// persisted contract is Exact.
    ///
    /// # Safety
    ///
    /// `pushed` must be the setrefs-adjusted pushed-expression section paired
    /// with this runtime state's planned records.
    pub(crate) unsafe fn recheck_list(
        &self,
        pushed: &[*mut pg_sys::Expr],
    ) -> *mut pg_sys::List {
        let mut recheck: *mut pg_sys::List = ptr::null_mut();
        for (&expr, contract) in pushed.iter().zip(self.runtime.contracts()) {
            if contract.requires_recheck() {
                recheck = unsafe { pg_sys::lappend(recheck, expr.cast::<c_void>()) };
            }
        }
        recheck
    }

    /// # Safety
    ///
    /// The envelope lists and binding expressions must belong to the live
    /// CustomScan plan, and `parent` must be its initialized PlanState.
    pub(crate) unsafe fn initialize(
        envelope: &EncodedPrivate,
        binding_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, CustomScanError> {
        let (planned, bindings) = unsafe {
            FilterDataCodec::<P>::decode(
                envelope.planned_filters_raw,
                envelope.planned_filter_count,
                envelope.binding_slots_raw,
                envelope.binding_count,
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

    /// Bind every value slot and planned predicate once at executor Begin.
    ///
    /// # Safety
    ///
    /// `econtext` must be the live expression context used to initialize the
    /// binding expression states.
    pub(crate) unsafe fn bind_initial(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), CustomScanError> {
        unsafe { self.runtime.bind_initial(econtext) }?;
        Ok(())
    }

    pub(crate) unsafe fn rebind_dynamic(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), CustomScanError> {
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
}

impl<E> From<FilterDataError<E>> for CustomScanError
where
    E: SqlStateError + Error + Send + Sync + 'static,
{
    fn from(error: FilterDataError<E>) -> Self {
        match error {
            FilterDataError::PlanData(error) => error.into(),
            FilterDataError::Provider(error) => CustomScanError::provider(error),
            FilterDataError::Invalid(error) => CustomScanError::framework(error),
        }
    }
}

impl<E> From<RuntimeFilterError<E>> for CustomScanError
where
    E: SqlStateError + Error + Send + Sync + 'static,
{
    fn from(error: RuntimeFilterError<E>) -> Self {
        match error {
            RuntimeFilterError::Provider(error) => CustomScanError::provider(error),
            RuntimeFilterError::BindingCountMismatch
            | RuntimeFilterError::ExactValueNotRepresentable { .. } => {
                CustomScanError::framework(error)
            }
        }
    }
}
