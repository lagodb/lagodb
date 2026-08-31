//! Provider-local staging for the query source facet.

use std::cell::Cell;

use crate::runtime_api::QuerySourceDescriptor;

thread_local! {
    static QUERY_SOURCE: Cell<Option<QuerySourceDescriptor>> =
        const { Cell::new(None) };
}

pub(super) fn register(descriptor: QuerySourceDescriptor) {
    assert!(
        !super::hooks_frozen(),
        "query source must be registered before provider hooks are frozen"
    );
    QUERY_SOURCE.with(|slot| {
        assert!(
            slot.replace(Some(descriptor)).is_none(),
            "this provider DSO already registered a query source facet"
        );
    });
}

pub(super) fn descriptor() -> Option<QuerySourceDescriptor> {
    QUERY_SOURCE.get()
}
