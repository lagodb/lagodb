//! PG17 provider-neutral Custom ModifyTable framework.

mod bridge;
mod execution;
mod methods;
mod modify_table;
mod planning;
mod registry;

use crate::customscan::provider::{LakebaseCustomModifyProvider, RelPathContext};

/// Register one scan provider as a Custom ModifyTable provider.
///
/// Call this during `_PG_init`, after registering the provider with the scan
/// framework.
pub fn register_provider<P: LakebaseCustomModifyProvider>() {
    registry::register::<P>();
    planning::install_hooks();
}

/// Whether a registered ModifyTable provider owns this target relation.
pub(crate) fn has_provider_for(context: &RelPathContext) -> bool {
    registry::has_provider(context)
}
