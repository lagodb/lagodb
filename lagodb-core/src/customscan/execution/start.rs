//! One-time CustomScan provider startup after executor parameter initialization.

use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::execution::scan::exec_custom_scan_trampoline;
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::provider::{
    LagodbCustomScanProvider, StartContext, method_tables_for,
};
use crate::handles::{RelationHandle, SnapshotHandle};
use pgrx::{pg_guard, pg_sys};

/// Start only after ExecInitNode and the outer plan have populated parameters.
/// Switch the method table on success so normal row production has no startup
/// check. A pre-start ReScan leaves this callback installed.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn first_exec_custom_scan_trampoline<
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    if let Err(error) = unsafe { wrapper.start_provider() } {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::Start)
            .report();
    }
    unsafe { (*node).methods = method_tables_for::<P>().exec() };
    unsafe { exec_custom_scan_trampoline::<P>(node) }
}

impl<P: LagodbCustomScanProvider> CustomScanStateWrapper<P> {
    unsafe fn start_provider(&mut self) -> Result<(), CustomScanError> {
        let econtext = self.base.ss.ps.ps_ExprContext;
        let estate = self.base.ss.ps.state;
        let filters = self.filters.as_mut().expect("Begin installed scan filters");
        let previous_context =
            unsafe { pg_sys::MemoryContextSwitchTo((*estate).es_query_cxt) };
        let result = (|| {
            unsafe { filters.bind_dynamic_initial(econtext) }?;
            let envelope = self
                .cached_envelope
                .as_ref()
                .expect("Begin installed scan envelope");
            let context = StartContext::<P>::new(
                self.provider_state
                    .as_mut()
                    .expect("Begin initialized provider state"),
                envelope.purpose,
                filters.bound(),
                unsafe { RelationHandle::from_raw(self.base.ss.ss_currentRelation) },
                unsafe { SnapshotHandle::from_raw((*estate).es_snapshot) },
            );
            P::start(context)
        })();
        unsafe { pg_sys::MemoryContextSwitchTo(previous_context) };
        self.provider_started = result.is_ok();
        result
    }
}
