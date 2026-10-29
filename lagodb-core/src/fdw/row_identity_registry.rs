//! Planner-side access to PostgreSQL's row-identity registry.

use core::ffi::{CStr, c_int};

use pgrx::pg_sys;

use super::row_identity::{ForeignRowIdentityError, ForeignRowIdentityKind};

/// Row identities registered for one result relation during planning.
pub(crate) struct RowIdentityRegistry {
    root: *mut pg_sys::PlannerInfo,
    rtindex: pg_sys::Index,
}

impl RowIdentityRegistry {
    /// # Safety
    ///
    /// `root` and its planner lists must be live for the current planner
    /// callback, and `rtindex` must identify a result relation in that root.
    pub(crate) unsafe fn new(
        root: *mut pg_sys::PlannerInfo,
        rtindex: pg_sys::Index,
    ) -> Self {
        Self { root, rtindex }
    }

    pub(crate) unsafe fn has_item_pointer(
        &self,
    ) -> Result<bool, ForeignRowIdentityError> {
        for kind in unsafe { self.kinds() } {
            if kind? == ForeignRowIdentityKind::ItemPointer {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Collect every row identity registered for this result relation.
    pub(crate) unsafe fn identities(
        &self,
    ) -> Result<Box<[ForeignRowIdentityKind]>, ForeignRowIdentityError> {
        unsafe { self.kinds() }
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
    }

    /// Read registrations from the same two planner stores used by
    /// `add_row_identity_var`: `processed_tlist` for a direct target and
    /// `row_identity_vars` for inheritance or partition leaves.
    ///
    /// # Safety
    ///
    /// The planner root and its lists must remain live while the returned
    /// iterator is consumed.
    unsafe fn kinds(
        &self,
    ) -> impl Iterator<Item = Result<ForeignRowIdentityKind, ForeignRowIdentityError>>
    {
        let parse = unsafe { (*self.root).parse };
        let direct_target =
            unsafe { (*parse).resultRelation as pg_sys::Index } == self.rtindex;
        let entries = if direct_target {
            unsafe { (*self.root).processed_tlist }
        } else {
            unsafe { (*self.root).row_identity_vars }
        };
        let length = unsafe { pg_sys::list_length(entries) };
        let rtindex = self.rtindex;

        (0..length).filter_map(move |index| {
            let name = if direct_target {
                let target_entry = unsafe { pg_sys::list_nth(entries, index) }
                    as *mut pg_sys::TargetEntry;
                if !unsafe { (*target_entry).resjunk }
                    || unsafe { (*target_entry).resname.is_null() }
                {
                    return None;
                }
                unsafe { CStr::from_ptr((*target_entry).resname) }
            } else {
                let identity = unsafe { pg_sys::list_nth(entries, index) }
                    as *mut pg_sys::RowIdentityVarInfo;
                if !unsafe {
                    pg_sys::bms_is_member(rtindex as c_int, (*identity).rowidrels)
                } {
                    return None;
                }
                unsafe { CStr::from_ptr((*identity).rowidname) }
            };
            ForeignRowIdentityKind::from_target_name(name).transpose()
        })
    }
}
