//! Binding of Modify-purpose CustomScans to the outer ModifyTable state.

use core::marker::PhantomData;

use crate::access::mutation::ModifyScanBinding;
use crate::api::TableAccessMethod;
use crate::customscan::error::CustomScanError;
use crate::customscan::execution::exec::provider_scan_purpose;
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::provider::{ScanPurpose, method_tables_for};
use crate::handles::RelationHandle;
use pgrx::pg_sys;

use super::contract::LagodbCustomModifyProvider;

/// One-time outer-executor binding for a provider scan in `Modify` purpose.
pub struct ModifyBindContext<'a, P: LagodbCustomModifyProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub relation: RelationHandle<'a>,
    pub binding:
        ModifyScanBinding<<P::AccessMethod as TableAccessMethod>::ModifyQueryState>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LagodbCustomModifyProvider> ModifyBindContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        binding: ModifyScanBinding<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> Self {
        Self {
            state,
            relation,
            binding,
            _marker: PhantomData,
        }
    }
}

/// Bind a provider scan to the stable relation state owned by its outer
/// ModifyTable executor.
///
/// # Safety
///
/// `node` must be this provider's initialized query-live CustomScanState, and
/// `binding` must remain valid until the inner plan is ended.
pub(crate) unsafe fn bind_modify_scan<P: LagodbCustomModifyProvider>(
    node: *mut pg_sys::CustomScanState,
    binding: ModifyScanBinding<
        <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
    >,
) -> Result<(), CustomScanError> {
    if node.is_null() || unsafe { (*node).methods } != method_tables_for::<P>().exec()
    {
        return Err(CustomScanError::modify_binding(
            "attempted to bind a CustomScan owned by another provider",
        ));
    }
    let plan = unsafe { (*node).ss.ps.plan };
    if unsafe { provider_scan_purpose::<P>(plan) }? != Some(ScanPurpose::Modify) {
        return Err(CustomScanError::modify_binding(
            "attempted to bind a CustomScan not planned for Modify",
        ));
    }

    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    let Some(provider_state) = wrapper.active_provider_state_mut() else {
        return Err(CustomScanError::modify_binding(
            "Modify CustomScan binding occurred before provider begin completed",
        ));
    };
    let relation = unsafe { (*node).ss.ss_currentRelation };
    if relation.is_null() {
        return Err(CustomScanError::modify_binding(
            "Modify CustomScan has no open relation",
        ));
    }
    P::bind_modify(ModifyBindContext::new(
        provider_state,
        unsafe { RelationHandle::from_raw(relation) },
        binding,
    ))
}
