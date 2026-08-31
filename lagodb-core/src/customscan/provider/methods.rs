//! Per-provider PostgreSQL method tables, allocated and cached as one unit.

use core::any::TypeId;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::customscan::execution::{exec, explain, state};
use crate::customscan::planning::{builder, final_plan};
use crate::customscan::{CustomScanMethodTables, SerialCustomScanCallbacks};

use super::LagodbCustomScanProvider;

/// Stable method-table type used by the relation provider facade.
pub type ProviderMethodTables = CustomScanMethodTables;

#[derive(Clone, Copy)]
struct MethodTablesRef(&'static ProviderMethodTables);

thread_local! {
    static METHOD_TABLES: RefCell<HashMap<TypeId, MethodTablesRef>> = RefCell::new(HashMap::new());
}

/// Return the stable PostgreSQL callback tables for provider `P`.
pub fn method_tables_for<P: LagodbCustomScanProvider>()
-> &'static ProviderMethodTables {
    let key = TypeId::of::<P>();
    if let Some(tables) = METHOD_TABLES.with_borrow(|cache| cache.get(&key).copied())
    {
        return tables.0;
    }

    let tables = ProviderMethodTables::serial(
        P::NAME,
        SerialCustomScanCallbacks {
            plan: final_plan::plan_custom_path_trampoline::<P>,
            reparameterize: Some(
                builder::reparameterize_custom_path_by_child_trampoline::<P>,
            ),
            create_state: state::create_custom_scan_state_trampoline::<P>,
            begin: exec::begin_custom_scan_trampoline::<P>,
            execute: exec::exec_custom_scan_trampoline::<P>,
            end: exec::end_custom_scan_trampoline::<P>,
            rescan: exec::rescan_custom_scan_trampoline::<P>,
            explain: explain::explain_custom_scan_trampoline::<P>,
        },
    );

    let leaked = Box::leak(Box::new(tables));
    METHOD_TABLES.with_borrow_mut(|cache| {
        cache.insert(key, MethodTablesRef(leaked));
    });
    leaked
}
