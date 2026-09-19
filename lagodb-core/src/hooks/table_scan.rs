//! Provider-local staging for the table-scan facet.

use std::cell::Cell;

use crate::runtime_api::{TableScanDescriptor, TableScanWorkerDescriptor};

thread_local! {
    static TABLE_SCAN: Cell<Option<TableScanDescriptor>> =
        const { Cell::new(None) };
    static TABLE_SCAN_WORKER: Cell<Option<TableScanWorkerDescriptor>> =
        const { Cell::new(None) };
}

pub(super) fn register(descriptor: TableScanDescriptor) {
    assert!(
        !super::hooks_frozen(),
        "table scan must be registered before provider hooks are frozen"
    );
    TABLE_SCAN.with(|slot| {
        assert!(
            slot.replace(Some(descriptor)).is_none(),
            "this provider DSO already registered a table-scan facet"
        );
    });
}

pub(super) fn descriptor() -> Option<TableScanDescriptor> {
    TABLE_SCAN.get()
}

pub(super) fn register_worker(descriptor: TableScanWorkerDescriptor) {
    assert!(
        !super::hooks_frozen(),
        "table scan worker must be registered before provider hooks are frozen"
    );
    TABLE_SCAN_WORKER.with(|slot| {
        assert!(
            slot.replace(Some(descriptor)).is_none(),
            "this provider DSO already registered a table-scan worker facet"
        );
    });
}

pub(super) fn worker_descriptor() -> Option<TableScanWorkerDescriptor> {
    TABLE_SCAN_WORKER.get()
}
