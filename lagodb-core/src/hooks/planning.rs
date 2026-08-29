//! Provider-local staging for runtime-routed planning facets.

use std::cell::Cell;

use crate::runtime_api::{ModifyPlannerDescriptor, RelationScanPlannerDescriptor};

#[derive(Clone, Copy)]
pub(super) struct PlanningDescriptors {
    pub(super) relation_scan: Option<RelationScanPlannerDescriptor>,
    pub(super) modify: Option<ModifyPlannerDescriptor>,
}

thread_local! {
    static RELATION_SCAN: Cell<Option<RelationScanPlannerDescriptor>> =
        const { Cell::new(None) };
    static MODIFY: Cell<Option<ModifyPlannerDescriptor>> =
        const { Cell::new(None) };
}

pub(crate) fn register_relation_scan(descriptor: RelationScanPlannerDescriptor) {
    assert!(
        !super::hooks_frozen(),
        "relation planning must be registered before provider hooks are frozen"
    );
    RELATION_SCAN.with(|slot| {
        assert!(
            slot.replace(Some(descriptor)).is_none(),
            "this provider DSO already registered a relation planning facet"
        );
    });
}

pub(crate) fn register_modify(descriptor: ModifyPlannerDescriptor) {
    assert!(
        !super::hooks_frozen(),
        "modify planning must be registered before provider hooks are frozen"
    );
    MODIFY.with(|slot| {
        assert!(
            slot.replace(Some(descriptor)).is_none(),
            "this provider DSO already registered a modify planning facet"
        );
    });
}

pub(super) fn descriptors() -> PlanningDescriptors {
    PlanningDescriptors {
        relation_scan: RELATION_SCAN.get(),
        modify: MODIFY.get(),
    }
}
