//! Allocation-free snapshots of registered planning descriptor facets.

use std::ffi::c_void;

use crate::descriptor_registry::{
    DescriptorRegistry, DescriptorSnapshot, PreparedDescriptors,
};
use lagodb_core::runtime_api::{
    ModifyPlannerDescriptor, REGISTER_INVALID_DESCRIPTOR,
    RelationScanPlannerDescriptor, RoutedModifyPlannerPost,
    RoutedModifyQueryPreparation, RoutedModifyUpperPlanner,
    RoutedRelationScanPlanner,
};

thread_local! {
    static RELATION_SCAN: DescriptorRegistry<StoredRelationScanPlanner> =
        const { DescriptorRegistry::new() };
    static MODIFY: DescriptorRegistry<StoredModifyPlanner> =
        const { DescriptorRegistry::new() };
}

#[derive(Clone, Copy)]
pub(super) struct StoredRelationScanPlanner {
    pub(super) context: *mut c_void,
    pub(super) plan_relation: RoutedRelationScanPlanner,
}

impl StoredRelationScanPlanner {
    fn from_descriptor(descriptor: &RelationScanPlannerDescriptor) -> Option<Self> {
        Some(Self {
            context: descriptor.context,
            plan_relation: descriptor.plan_relation?,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct StoredModifyPlanner {
    pub(super) context: *mut c_void,
    pub(super) prepare_query: RoutedModifyQueryPreparation,
    pub(super) planner_post: RoutedModifyPlannerPost,
    pub(super) create_upper_paths: RoutedModifyUpperPlanner,
}

impl StoredModifyPlanner {
    fn from_descriptor(descriptor: &ModifyPlannerDescriptor) -> Option<Self> {
        Some(Self {
            context: descriptor.context,
            prepare_query: descriptor.prepare_query?,
            planner_post: descriptor.planner_post?,
            create_upper_paths: descriptor.create_upper_paths?,
        })
    }
}

pub(crate) struct PreparedPlanningHooks {
    relation_scan: PreparedDescriptors<StoredRelationScanPlanner>,
    modify: PreparedDescriptors<StoredModifyPlanner>,
}

impl PreparedPlanningHooks {
    pub(crate) fn prepare(
        relation_scan: Option<&RelationScanPlannerDescriptor>,
        modify: Option<&ModifyPlannerDescriptor>,
    ) -> Result<Self, u32> {
        let relation_scan = relation_scan
            .map(|descriptor| {
                StoredRelationScanPlanner::from_descriptor(descriptor)
                    .ok_or(REGISTER_INVALID_DESCRIPTOR)
            })
            .transpose()?;
        let modify = modify
            .map(|descriptor| {
                StoredModifyPlanner::from_descriptor(descriptor)
                    .ok_or(REGISTER_INVALID_DESCRIPTOR)
            })
            .transpose()?;
        Ok(Self {
            relation_scan: PreparedDescriptors::new(relation_scan),
            modify: PreparedDescriptors::new(modify),
        })
    }

    pub(crate) fn commit(self) {
        let _ = RELATION_SCAN.with(|registry| registry.commit(self.relation_scan));
        let _ = MODIFY.with(|registry| registry.commit(self.modify));
    }
}

pub(super) fn relation_scan_snapshot() -> DescriptorSnapshot<StoredRelationScanPlanner>
{
    RELATION_SCAN.with(|registry| registry.snapshot())
}

pub(super) fn modify_snapshot() -> DescriptorSnapshot<StoredModifyPlanner> {
    MODIFY.with(|registry| registry.snapshot())
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use lagodb_core::runtime_api::{CALLBACK_OK, CallbackErrorReport};
    use pgrx::pg_sys;

    use super::*;

    unsafe extern "C-unwind" fn relation(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _rel: *mut pg_sys::RelOptInfo,
        _rti: pg_sys::Index,
        _rte: *mut pg_sys::RangeTblEntry,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
    }

    unsafe extern "C-unwind" fn prepare_query(
        _context: *mut c_void,
        _parse: *mut pg_sys::Query,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
    }

    unsafe extern "C-unwind" fn planner_post(
        _context: *mut c_void,
        _planned: *mut pg_sys::PlannedStmt,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
    }

    unsafe extern "C-unwind" fn upper(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _stage: pg_sys::UpperRelationKind::Type,
        _input_rel: *mut pg_sys::RelOptInfo,
        _output_rel: *mut pg_sys::RelOptInfo,
        _extra: *mut c_void,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
    }

    #[test]
    fn relation_descriptor_requires_callback() {
        let mut descriptor = RelationScanPlannerDescriptor {
            context: ptr::null_mut(),
            plan_relation: Some(relation),
        };
        assert!(StoredRelationScanPlanner::from_descriptor(&descriptor).is_some());
        descriptor.plan_relation = None;
        assert!(StoredRelationScanPlanner::from_descriptor(&descriptor).is_none());
    }

    #[test]
    fn modify_descriptor_requires_each_typed_callback() {
        let mut descriptor = ModifyPlannerDescriptor {
            context: ptr::null_mut(),
            prepare_query: Some(prepare_query),
            planner_post: Some(planner_post),
            create_upper_paths: Some(upper),
        };
        assert!(StoredModifyPlanner::from_descriptor(&descriptor).is_some());
        descriptor.planner_post = None;
        assert!(StoredModifyPlanner::from_descriptor(&descriptor).is_none());
    }
}
