//! Plan-stage relation identity lookup.

use core::ffi::c_int;

use pgrx::pg_sys;

#[derive(Debug, Clone, Copy)]
pub struct PlanRelationResolver {
    root: *mut pg_sys::PlannerInfo,
}

impl PlanRelationResolver {
    #[inline]
    pub fn new(root: *mut pg_sys::PlannerInfo) -> Self {
        Self { root }
    }

    /// `pg_class` OID for a scan RTI, mirroring PostgreSQL's
    /// `planner_rt_fetch`.
    ///
    /// # Safety
    ///
    /// `self.root` must point to a live `PlannerInfo`, `relid` must be a valid
    /// one-based RTI, and the corresponding `RangeTblEntry` must be non-NULL.
    /// When `root.simple_rte_array` is NULL, `root.parse->rtable` must be live.
    pub unsafe fn rel_oid(self, relid: pg_sys::Index) -> pg_sys::Oid {
        let simple_rte_array = unsafe { (*self.root).simple_rte_array };
        let rte = if simple_rte_array.is_null() {
            let parse = unsafe { (*self.root).parse };
            let rtable = unsafe { (*parse).rtable };
            unsafe { pg_sys::list_nth(rtable, (relid - 1) as c_int) }
                .cast::<pg_sys::RangeTblEntry>()
        } else {
            unsafe { *simple_rte_array.add(relid as usize) }
        };
        unsafe { (*rte).relid }
    }
}
