//! Backend-only harnesses for exercising the core CustomScan callbacks.

use core::marker::PhantomData;
use core::ptr;

use pg_lakebase_core::customscan::provider::{
    CustomScanError, LakebaseCustomScanProvider,
};
use pg_lakebase_core::customscan::state::{
    CustomScanStateWrapper, create_custom_scan_state_trampoline,
};
use pg_lakebase_core::expr::pushdown::{
    FilterFragment, FilterPlan, FilterPushdownPlanner,
};
use pgrx::pg_sys;

pub(crate) struct RejectAllFilterPlanner;

impl FilterPushdownPlanner for RejectAllFilterPlanner {
    type PlannedPredicate = ();
    type Error = CustomScanError;

    fn try_plan_filter(
        &mut self,
        _fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        Ok(FilterPlan::Unsupported)
    }
}

macro_rules! impl_reject_all_filters {
    ($provider:ty) => {
        impl pg_lakebase_core::expr::pushdown::FilterPushdown for $provider {
            type Planner =
                $crate::lakebase_core::customscan::support::RejectAllFilterPlanner;
            type PlannedPredicate = ();
            type BoundPredicate = ();
            type Error = pg_lakebase_core::customscan::provider::CustomScanError;

            fn begin_filter_planning(
                _context: &pg_lakebase_core::expr::pushdown::FilterPlanningContext,
            ) -> Result<Self::Planner, Self::Error> {
                Ok($crate::lakebase_core::customscan::support::RejectAllFilterPlanner)
            }

            fn encode_planned(
                _predicate: &Self::PlannedPredicate,
                _writer: &mut pg_lakebase_core::plan_data::PlanDataWriter,
            ) -> Result<(), Self::Error> {
                Ok(())
            }

            fn decode_planned(
                _reader: &mut pg_lakebase_core::plan_data::PlanDataReader<'_>,
                _binding_count: usize,
            ) -> Result<Self::PlannedPredicate, Self::Error> {
                Ok(())
            }

            fn bind_filter(
                _predicate: &Self::PlannedPredicate,
                _values: pg_lakebase_core::expr::pushdown::FilterValueBindings<'_>,
            ) -> Result<
                pg_lakebase_core::expr::pushdown::FilterBindResult<Self::BoundPredicate>,
                Self::Error,
            > {
                Ok(pg_lakebase_core::expr::pushdown::FilterBindResult::ValueNotRepresentable)
            }
        }
    };
}

pub(crate) use impl_reject_all_filters;

/// Backend test owner for a PostgreSQL-allocated `CustomScanStateWrapper`.
pub(crate) struct TestScanState<P: LakebaseCustomScanProvider> {
    wrapper: *mut pg_sys::CustomScanState,
    _provider: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider> TestScanState<P> {
    /// Allocate the same wrapper used by PostgreSQL's CreateCustomScanState.
    ///
    /// # Safety
    ///
    /// The returned state must only be used while the current PostgreSQL memory
    /// context remains alive.
    pub(crate) unsafe fn new() -> Self {
        Self {
            wrapper: unsafe {
                create_custom_scan_state_trampoline::<P>(ptr::null_mut()).cast()
            },
            _provider: PhantomData,
        }
    }

    pub(crate) fn node_ptr(&self) -> *mut pg_sys::CustomScanState {
        self.wrapper
    }

    unsafe fn wrapper_mut(&mut self) -> &mut CustomScanStateWrapper<P> {
        unsafe { CustomScanStateWrapper::from_node_ptr(self.wrapper) }
    }

    unsafe fn wrapper_ref(&self) -> &CustomScanStateWrapper<P> {
        unsafe { CustomScanStateWrapper::from_node_ptr(self.wrapper) }
    }

    pub(crate) unsafe fn base_mut(&mut self) -> &mut pg_sys::CustomScanState {
        unsafe { self.wrapper_mut().test_base_mut() }
    }

    pub(crate) unsafe fn scan_state_ptr(&mut self) -> *mut pg_sys::ScanState {
        unsafe { self.wrapper_mut().test_scan_state_ptr() }
    }

    pub(crate) unsafe fn install_provider_state(&mut self, state: P::State) {
        unsafe { self.wrapper_mut().test_install_provider_state(state) };
    }

    pub(crate) unsafe fn provider_state(&self) -> &P::State {
        unsafe { self.wrapper_ref().test_provider_state() }
    }
}
