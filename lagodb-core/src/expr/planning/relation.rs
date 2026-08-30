//! Plan-stage relation metadata and attno-indexed planner facts.

use core::ffi::c_int;
use core::ptr::NonNull;

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

/// Planner-local index of relation user-column `Var` nodes by attribute number.
///
/// PostgreSQL user attributes are positive and use `attnum - 1` as an array
/// index.  This type deliberately represents only those attributes; whole-row
/// and system-column Vars are handled by the surrounding planner contract.
#[derive(Default)]
pub(crate) struct RelationVarsByAttno {
    vars: Vec<Option<NonNull<pg_sys::Var>>>,
    count: usize,
}

impl RelationVarsByAttno {
    #[inline]
    fn index(attno: pg_sys::AttrNumber) -> Option<usize> {
        usize::try_from(attno).ok()?.checked_sub(1)
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn insert(&mut self, var: NonNull<pg_sys::Var>) {
        let Some(index) = Self::index(unsafe { var.as_ref().varattno }) else {
            return;
        };
        if self.vars.len() <= index {
            self.vars.resize(index + 1, None);
        }
        if self.vars[index].is_none() {
            self.vars[index] = Some(var);
            self.count += 1;
        }
    }

    #[inline]
    pub(crate) fn get(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Option<NonNull<pg_sys::Var>> {
        Self::index(attno).and_then(|index| self.vars.get(index).copied().flatten())
    }

    pub(crate) fn take(
        &mut self,
        attno: pg_sys::AttrNumber,
    ) -> Option<NonNull<pg_sys::Var>> {
        let var = Self::index(attno)
            .and_then(|index| self.vars.get_mut(index).and_then(Option::take));
        if var.is_some() {
            self.count -= 1;
        }
        var
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (pg_sys::AttrNumber, NonNull<pg_sys::Var>)> + '_ {
        self.vars.iter().enumerate().filter_map(|(index, var)| {
            let var = (*var)?;
            let attno = pg_sys::AttrNumber::try_from(index + 1).ok()?;
            Some((attno, var))
        })
    }

    pub(crate) fn attnos(&self) -> impl Iterator<Item = pg_sys::AttrNumber> + '_ {
        self.iter().map(|(attno, _)| attno)
    }
}
