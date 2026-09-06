//! Provider-local staging for the table-scan facet.

use std::cell::Cell;

use crate::runtime_api::TableScanDescriptor;

thread_local! {
    static TABLE_SCAN: Cell<Option<TableScanDescriptor>> =
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
