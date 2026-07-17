//! Per-provider Custom ModifyTable callback tables.

use std::any::TypeId;
use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::pg_sys;

use crate::customscan::provider::LakebaseCustomModifyProvider;

pub(super) struct ModifyMethodTables {
    pub modify_path: pg_sys::CustomPathMethods,
    pub modify_scan: pg_sys::CustomScanMethods,
    pub modify_exec: pg_sys::CustomExecMethods,
}

unsafe impl Send for ModifyMethodTables {}
unsafe impl Sync for ModifyMethodTables {}

#[derive(Clone, Copy)]
struct TablesRef(&'static ModifyMethodTables);

thread_local! {
    static TABLES: RefCell<HashMap<TypeId, TablesRef>> = RefCell::new(HashMap::new());
}

pub(super) fn tables<P: LakebaseCustomModifyProvider>() -> &'static ModifyMethodTables
{
    let key = TypeId::of::<P>();
    if let Some(tables) = TABLES.with_borrow(|cache| cache.get(&key).copied()) {
        return tables.0;
    }

    let name = P::MODIFY_NAME.as_ptr();
    let tables = Box::leak(Box::new(ModifyMethodTables {
        modify_path: pg_sys::CustomPathMethods {
            CustomName: name,
            PlanCustomPath: Some(super::planning::plan_modify_table::<P>),
            ReparameterizeCustomPathByChild: None,
        },
        modify_scan: pg_sys::CustomScanMethods {
            CustomName: name,
            CreateCustomScanState: Some(super::modify_table::create_state::<P>),
        },
        modify_exec: pg_sys::CustomExecMethods {
            CustomName: name,
            BeginCustomScan: Some(super::modify_table::begin::<P>),
            ExecCustomScan: Some(super::modify_table::exec::<P>),
            EndCustomScan: Some(super::modify_table::end::<P>),
            ReScanCustomScan: Some(super::modify_table::rescan::<P>),
            MarkPosCustomScan: None,
            RestrPosCustomScan: None,
            EstimateDSMCustomScan: None,
            InitializeDSMCustomScan: None,
            ReInitializeDSMCustomScan: None,
            InitializeWorkerCustomScan: None,
            ShutdownCustomScan: None,
            ExplainCustomScan: Some(super::modify_table::explain),
        },
    }));
    TABLES.with_borrow_mut(|cache| {
        cache.insert(key, TablesRef(tables));
    });
    tables
}

pub(super) fn register<P: LakebaseCustomModifyProvider>() {
    let tables = tables::<P>();
    // SAFETY: called from `_PG_init`; the leaked table and provider name are
    // process-lifetime stable.
    unsafe {
        pg_sys::RegisterCustomScanMethods(&tables.modify_scan);
    }
}
