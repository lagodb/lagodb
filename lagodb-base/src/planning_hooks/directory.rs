//! Allocation-free snapshots of registered planning descriptor facets.

use std::cell::Cell;
use std::ffi::c_void;
use std::mem::size_of;
use std::ptr;

use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    ModifyPlannerDescriptor, RelationScanPlannerDescriptor, RoutedModifyPlannerPost,
    RoutedModifyPlannerPre, RoutedModifyUpperPlanner, RoutedRelationScanPlanner,
};

struct DescriptorNode<T: Copy> {
    descriptor: T,
    next: Cell<*const DescriptorNode<T>>,
}

struct DescriptorDirectory<T: Copy> {
    head: Cell<*const DescriptorNode<T>>,
    tail: Cell<*const DescriptorNode<T>>,
}

impl<T: Copy> DescriptorDirectory<T> {
    const fn new() -> Self {
        Self {
            head: Cell::new(ptr::null()),
            tail: Cell::new(ptr::null()),
        }
    }

    fn commit(&self, node: Option<Box<DescriptorNode<T>>>) {
        let Some(node) = node else {
            return;
        };
        let node = Box::into_raw(node);
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: the tail is a backend-lifetime node published by this
            // single-threaded directory.
            unsafe { (*tail).next.set(node) };
        }
    }

    fn snapshot(&self) -> DescriptorSnapshot<T> {
        DescriptorSnapshot {
            first: self.head.get(),
            last: self.tail.get(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct DescriptorSnapshot<T: Copy> {
    first: *const DescriptorNode<T>,
    last: *const DescriptorNode<T>,
}

impl<T: Copy> DescriptorSnapshot<T> {
    pub(super) fn try_for_each(
        self,
        mut callback: impl FnMut(T) -> Result<(), PgReportError>,
    ) -> Result<(), PgReportError> {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: nodes remain live for the backend lifetime. The captured
            // tail keeps recursive registration outside this event snapshot.
            let node = unsafe { &*current };
            callback(node.descriptor)?;
            if current == self.last {
                break;
            }
            current = node.next.get();
        }
        Ok(())
    }
}

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
        relation_scan: relation_scan.map(|descriptor| {
            Box::new(DescriptorNode {
                descriptor,
                next: Cell::new(ptr::null()),
            })
        }),
        modify: modify.map(|descriptor| {
            Box::new(DescriptorNode {
                descriptor,
                next: Cell::new(ptr::null()),
            })
        }),
    })
}

pub(crate) fn commit(prepared: PreparedPlanningHooks) {
    RELATION_SCAN.with(|directory| directory.commit(prepared.relation_scan));
    MODIFY.with(|directory| directory.commit(prepared.modify));
}

pub(super) fn relation_scan_snapshot() -> DescriptorSnapshot<StoredRelationScanPlanner>
{
    RELATION_SCAN.with(DescriptorDirectory::snapshot)
}

pub(super) fn modify_snapshot() -> DescriptorSnapshot<StoredModifyPlanner> {
    MODIFY.with(DescriptorDirectory::snapshot)
}

#[cfg(test)]
mod tests {
    use lagodb_core::runtime_api::{PLANNING_CALLBACK_OK, PlanErrorRecord};
    use pgrx::pg_sys;

    use super::*;

    unsafe extern "C-unwind" fn relation(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _rel: *mut pg_sys::RelOptInfo,
        _rti: pg_sys::Index,
        _rte: *mut pg_sys::RangeTblEntry,
        _error: *mut PlanErrorRecord,
    ) -> u32 {
        PLANNING_CALLBACK_OK
    }

    unsafe extern "C-unwind" fn planner_pre(
        _context: *mut c_void,
        _parse: *mut pg_sys::Query,
        _error: *mut PlanErrorRecord,
    ) -> u32 {
        PLANNING_CALLBACK_OK
    }

    unsafe extern "C-unwind" fn planner_post(
        _context: *mut c_void,
        _planned: *mut pg_sys::PlannedStmt,
        _error: *mut PlanErrorRecord,
    ) -> u32 {
        PLANNING_CALLBACK_OK
    }

    unsafe extern "C-unwind" fn upper(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _stage: pg_sys::UpperRelationKind::Type,
        _input_rel: *mut pg_sys::RelOptInfo,
        _output_rel: *mut pg_sys::RelOptInfo,
        _extra: *mut c_void,
        _error: *mut PlanErrorRecord,
    ) -> u32 {
        PLANNING_CALLBACK_OK
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
