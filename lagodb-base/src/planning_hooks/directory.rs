//! Allocation-free snapshots of registered planning descriptor facets.

use std::ffi::c_void;
use std::mem::size_of;

use crate::descriptor_directory::{
    DescriptorDirectory, DescriptorNode, DescriptorSnapshot,
};
use lagodb_core::runtime_api::{
    ModifyPlannerDescriptor, RelationScanPlannerDescriptor, RoutedModifyPlannerPost,
    RoutedModifyPlannerPre, RoutedModifyUpperPlanner, RoutedRelationScanPlanner,
};

thread_local! {
    static RELATION_SCAN: DescriptorDirectory<StoredRelationScanPlanner> =
        const { DescriptorDirectory::new() };
    static MODIFY: DescriptorDirectory<StoredModifyPlanner> =
        const { DescriptorDirectory::new() };
}

#[derive(Clone, Copy)]
pub(super) struct StoredRelationScanPlanner {
    pub(super) context: *mut c_void,
    pub(super) plan_relation: RoutedRelationScanPlanner,
}

impl StoredRelationScanPlanner {
    fn from_descriptor(descriptor: &RelationScanPlannerDescriptor) -> Option<Self> {
        if descriptor.struct_size != size_of::<RelationScanPlannerDescriptor>() as u32
        {
            return None;
        }
        Some(Self {
            context: descriptor.context,
            plan_relation: descriptor.plan_relation?,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct StoredModifyPlanner {
    pub(super) context: *mut c_void,
    pub(super) planner_pre: RoutedModifyPlannerPre,
    pub(super) planner_post: RoutedModifyPlannerPost,
    pub(super) create_upper_paths: RoutedModifyUpperPlanner,
}

impl StoredModifyPlanner {
    fn from_descriptor(descriptor: &ModifyPlannerDescriptor) -> Option<Self> {
        if descriptor.struct_size != size_of::<ModifyPlannerDescriptor>() as u32 {
            return None;
        }
        Some(Self {
            context: descriptor.context,
            planner_pre: descriptor.planner_pre?,
            planner_post: descriptor.planner_post?,
            create_upper_paths: descriptor.create_upper_paths?,
        })
    }
}

pub(crate) struct PreparedPlanningHooks {
    relation_scan: Option<Box<DescriptorNode<StoredRelationScanPlanner>>>,
    modify: Option<Box<DescriptorNode<StoredModifyPlanner>>>,
}

pub(crate) fn prepare(
    relation_scan: Option<&RelationScanPlannerDescriptor>,
    modify: Option<&ModifyPlannerDescriptor>,
) -> Option<PreparedPlanningHooks> {
    let relation_scan = match relation_scan {
        Some(descriptor) => {
            Some(StoredRelationScanPlanner::from_descriptor(descriptor)?)
        }
        None => None,
    };
    let modify = match modify {
        Some(descriptor) => Some(StoredModifyPlanner::from_descriptor(descriptor)?),
        None => None,
    };
    Some(PreparedPlanningHooks {
        relation_scan: relation_scan.map(DescriptorNode::new),
        modify: modify.map(DescriptorNode::new),
    })
}

pub(crate) fn commit(prepared: PreparedPlanningHooks) {
    let _ = RELATION_SCAN.with(|directory| directory.commit(prepared.relation_scan));
    let _ = MODIFY.with(|directory| directory.commit(prepared.modify));
}

pub(super) fn relation_scan_snapshot() -> DescriptorSnapshot<StoredRelationScanPlanner>
{
    RELATION_SCAN.with(|directory| directory.snapshot())
}

pub(super) fn modify_snapshot() -> DescriptorSnapshot<StoredModifyPlanner> {
    MODIFY.with(|directory| directory.snapshot())
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use lagodb_core::runtime_api::{FFI_OPERATION_OK, FfiErrorRecord};
    use pgrx::pg_sys;

    use super::*;

    unsafe extern "C-unwind" fn relation(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _rel: *mut pg_sys::RelOptInfo,
        _rti: pg_sys::Index,
        _rte: *mut pg_sys::RangeTblEntry,
        _error: *mut FfiErrorRecord,
    ) -> u32 {
        FFI_OPERATION_OK
    }

    unsafe extern "C-unwind" fn planner_pre(
        _context: *mut c_void,
        _parse: *mut pg_sys::Query,
        _error: *mut FfiErrorRecord,
    ) -> u32 {
        FFI_OPERATION_OK
    }

    unsafe extern "C-unwind" fn planner_post(
        _context: *mut c_void,
        _planned: *mut pg_sys::PlannedStmt,
        _error: *mut FfiErrorRecord,
    ) -> u32 {
        FFI_OPERATION_OK
    }

    unsafe extern "C-unwind" fn upper(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _stage: pg_sys::UpperRelationKind::Type,
        _input_rel: *mut pg_sys::RelOptInfo,
        _output_rel: *mut pg_sys::RelOptInfo,
        _extra: *mut c_void,
        _error: *mut FfiErrorRecord,
    ) -> u32 {
        FFI_OPERATION_OK
    }

    #[test]
    fn relation_descriptor_requires_exact_layout_and_callback() {
        let mut descriptor = RelationScanPlannerDescriptor {
            struct_size: size_of::<RelationScanPlannerDescriptor>() as u32,
            context: ptr::null_mut(),
            plan_relation: Some(relation),
        };
        assert!(StoredRelationScanPlanner::from_descriptor(&descriptor).is_some());
        descriptor.struct_size += 1;
        assert!(StoredRelationScanPlanner::from_descriptor(&descriptor).is_none());
        descriptor.struct_size = size_of::<RelationScanPlannerDescriptor>() as u32;
        descriptor.plan_relation = None;
        assert!(StoredRelationScanPlanner::from_descriptor(&descriptor).is_none());
    }

    #[test]
    fn modify_descriptor_requires_each_typed_callback() {
        let mut descriptor = ModifyPlannerDescriptor {
            struct_size: size_of::<ModifyPlannerDescriptor>() as u32,
            context: ptr::null_mut(),
            planner_pre: Some(planner_pre),
            planner_post: Some(planner_post),
            create_upper_paths: Some(upper),
        };
        assert!(StoredModifyPlanner::from_descriptor(&descriptor).is_some());
        descriptor.planner_post = None;
        assert!(StoredModifyPlanner::from_descriptor(&descriptor).is_none());
    }
}
