//! Per-provider PostgreSQL method tables, allocated and cached as one unit.

use core::any::TypeId;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use pgrx::pg_sys;

use super::LakebaseCustomScanProvider;

/// The three PostgreSQL callback tables owned by one provider type.
pub struct ProviderMethodTables {
    path: pg_sys::CustomPathMethods,
    scan: pg_sys::CustomScanMethods,
    exec: pg_sys::CustomExecMethods,
}

// SAFETY: every `CustomName` points to immutable process-lifetime bytes and
// all remaining fields are function pointers.
unsafe impl Send for ProviderMethodTables {}
unsafe impl Sync for ProviderMethodTables {}

impl ProviderMethodTables {
    pub fn path(&'static self) -> &'static pg_sys::CustomPathMethods {
        &self.path
    }

    pub fn scan(&'static self) -> &'static pg_sys::CustomScanMethods {
        &self.scan
    }

    pub fn exec(&'static self) -> &'static pg_sys::CustomExecMethods {
        &self.exec
    }
}

#[derive(Clone, Copy)]
struct MethodTablesRef(&'static ProviderMethodTables);

// SAFETY: `ProviderMethodTables` is process-lifetime immutable callback data.
unsafe impl Send for MethodTablesRef {}
unsafe impl Sync for MethodTablesRef {}

static METHOD_TABLES: OnceLock<Mutex<HashMap<TypeId, MethodTablesRef>>> =
    OnceLock::new();

/// Return the stable PostgreSQL callback tables for provider `P`.
pub fn method_tables_for<P: LakebaseCustomScanProvider>()
-> &'static ProviderMethodTables {
    let cache = METHOD_TABLES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .expect("customscan method-table cache mutex poisoned");
    let key = TypeId::of::<P>();
    if let Some(entry) = guard.get(&key) {
        return entry.0;
    }

    let name = P::NAME.as_ptr();
    let tables = ProviderMethodTables {
        path: pg_sys::CustomPathMethods {
            CustomName: name,
            PlanCustomPath: Some(
                crate::customscan::builder::plan_custom_path_trampoline::<P>,
            ),
            ReparameterizeCustomPathByChild: Some(
                crate::customscan::builder::reparameterize_custom_path_by_child_trampoline::<P>,
            ),
        },
        scan: pg_sys::CustomScanMethods {
            CustomName: name,
            CreateCustomScanState: Some(
                crate::customscan::state::create_custom_scan_state_trampoline::<P>,
            ),
        },
        exec: pg_sys::CustomExecMethods {
            CustomName: name,
            BeginCustomScan: Some(
                crate::customscan::exec::begin_custom_scan_trampoline::<P>,
            ),
            ReScanCustomScan: Some(
                crate::customscan::exec::rescan_custom_scan_trampoline::<P>,
            ),
            ExecCustomScan: Some(
                crate::customscan::exec::exec_custom_scan_trampoline::<P>,
            ),
            EndCustomScan: Some(
                crate::customscan::exec::end_custom_scan_trampoline::<P>,
            ),
            MarkPosCustomScan: None,
            RestrPosCustomScan: None,
            EstimateDSMCustomScan: None,
            InitializeDSMCustomScan: None,
            ReInitializeDSMCustomScan: None,
            InitializeWorkerCustomScan: None,
            ShutdownCustomScan: None,
            ExplainCustomScan: Some(
                crate::customscan::explain::explain_custom_scan_trampoline::<P>,
            ),
        },
    };

    let leaked = Box::leak(Box::new(tables));
    guard.insert(key, MethodTablesRef(leaked));
    leaked
}
