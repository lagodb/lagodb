//! PG17 provider-neutral Custom ModifyTable framework.

mod binding;
mod bridge;
mod contract;
mod execution;
mod methods;
mod modify_table;
mod planning;
mod registry;

pub use binding::ModifyBindContext;
pub use contract::{LagodbCustomModifyProvider, ModifyCapabilities};

use crate::customscan::provider::RelationContext;

/// Register one scan provider as a Custom ModifyTable provider.
///
/// Call this during `_PG_init`, after registering the provider with the scan
/// framework.
pub fn register_provider<P: LagodbCustomModifyProvider>() {
    registry::register::<P>();
    planning::install_hooks();
}

/// Whether a registered ModifyTable provider owns this target relation.
pub(crate) fn has_provider_for(context: &RelationContext<'_>) -> bool {
    registry::has_provider(context)
}
