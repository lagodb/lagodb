//! `pg_inherits` queries through PostgreSQL's inheritance helpers.

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

// catalog/pg_inherits.h is not included in pgrx's generated bindings. The
// declarations live in their own namespace so the safe wrappers can keep
// PostgreSQL's names.
mod ffi {
    use pgrx::pg_sys;

    unsafe extern "C-unwind" {
        pub(super) fn find_all_inheritors(
            parent_rel_id: pg_sys::Oid,
            lock_mode: i32,
            numparents: *mut *mut pg_sys::List,
        ) -> *mut pg_sys::List;
        pub(super) fn find_inheritance_children(
            parent_rel_id: pg_sys::Oid,
            lock_mode: i32,
        ) -> *mut pg_sys::List;
        pub(super) fn has_superclass(relation_id: pg_sys::Oid) -> bool;
    }
}

/// Whether a relation has inheritance children, without locking any of them.
///
/// This is tablecmds.c's own existence check. Unlike `relhassubclass` it does
/// not outlive the last child. The caller must hold a lock on the parent that
/// keeps new children out.
pub fn has_inheritance_children(parent_rel_id: pg_sys::Oid) -> Result<bool, PgError> {
    unsafe {
        PgTryBuilder::new(move || {
            let children = pg_guard_ffi_boundary(|| {
                ffi::find_inheritance_children(parent_rel_id, pg_sys::NoLock as _)
            });
            let has_children = !children.is_null();
            pg_sys::list_free(children);
            Ok(has_children)
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }
}

/// Whether a relation inherits from another, without locking its parents.
///
/// The caller must hold a lock on the relation that keeps new parents out.
pub fn has_inheritance_parents(relation_id: pg_sys::Oid) -> Result<bool, PgError> {
    unsafe {
        PgTryBuilder::new(move || {
            Ok(pg_guard_ffi_boundary(|| ffi::has_superclass(relation_id)))
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }
}

pub fn find_all_inheritors(
    parent_rel_id: pg_sys::Oid,
    lockmode: pg_sys::LOCKMODE,
) -> Result<Vec<pg_sys::Oid>, PgError> {
    unsafe {
        PgTryBuilder::new(move || {
            let relations = pg_guard_ffi_boundary(|| {
                ffi::find_all_inheritors(
                    parent_rel_id,
                    lockmode,
                    std::ptr::null_mut(),
                )
            });
            let len = pg_sys::list_length(relations);
            let mut result = Vec::with_capacity(len as usize);
            for index in 0..len {
                result.push(pg_sys::list_nth_oid(relations, index));
            }
            pg_sys::list_free(relations);
            Ok(result)
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }
}
