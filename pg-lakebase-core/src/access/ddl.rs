//! DDL operation callback wrappers for AmDdl trait
//!
//! This module provides FFI boundary functions for DDL operations like CREATE, TRUNCATE, etc.
//! All unsafe operations are handled here, keeping the AmDdl trait implementation safe.

use crate::api::TableAccessMethod;
use crate::diag::ReportableError;
use crate::handles::{RelFileLocator, RelationHandle};
use pgrx::prelude::*;

#[pg_guard]
pub extern "C-unwind" fn relation_set_new_filelocator<A>(
    rel: pg_sys::Relation,
    newrlocator: *const pg_sys::RelFileLocator,
    persistence: ::core::ffi::c_char,
    freeze_xid: *mut pg_sys::TransactionId,
    minmulti: *mut pg_sys::MultiXactId,
) where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };

    let locator = unsafe {
        RelFileLocator::from_raw(newrlocator)
            .expect("newrlocator pointer must not be null")
    };

    let persistence_u8 = persistence as u8;
    let (xid, multi) =
        A::relation_set_new_filelocator(&rel_handle, &locator, persistence_u8)
            .report_unwrap();

    unsafe {
        *freeze_xid = xid;
        *minmulti = multi;
    }
}

#[pg_guard]
pub extern "C-unwind" fn relation_nontransactional_truncate<A>(rel: pg_sys::Relation)
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    A::relation_nontransactional_truncate(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_copy_data<A>(
    rel: pg_sys::Relation,
    newrlocator: *const pg_sys::RelFileLocator,
) where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let locator = unsafe {
        RelFileLocator::from_raw(newrlocator)
            .expect("newrlocator pointer must not be null")
    };

    A::relation_copy_data(&rel_handle, &locator).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_copy_for_cluster<A>(
    old_table: pg_sys::Relation,
    new_table: pg_sys::Relation,
    old_index: pg_sys::Relation,
    use_sort: bool,
    oldest_xmin: pg_sys::TransactionId,
    xid_cutoff: *mut pg_sys::TransactionId,
    multi_cutoff: *mut pg_sys::MultiXactId,
    num_tuples: *mut f64,
    tups_vacuumed: *mut f64,
    tups_recently_dead: *mut f64,
) where
    A: TableAccessMethod,
{
    let old_table_handle = unsafe { RelationHandle::from_raw(old_table) };
    let new_table_handle = unsafe { RelationHandle::from_raw(new_table) };
    let old_index_handle = (!old_index.is_null())
        .then(|| unsafe { RelationHandle::from_raw(old_index) });

    let (xid, multi, tuples, vacuumed, dead) = A::relation_copy_for_cluster(
        &old_table_handle,
        &new_table_handle,
        old_index_handle.as_ref(),
        use_sort,
        oldest_xmin,
    )
    .report_unwrap();

    unsafe {
        *xid_cutoff = xid;
        *multi_cutoff = multi;
        *num_tuples = tuples;
        *tups_vacuumed = vacuumed;
        *tups_recently_dead = dead;
    }
}

pub fn register<A>(routine: &mut pg_sys::TableAmRoutine)
where
    A: TableAccessMethod,
{
    routine.relation_set_new_filelocator = Some(relation_set_new_filelocator::<A>);
    routine.relation_nontransactional_truncate =
        Some(relation_nontransactional_truncate::<A>);
    routine.relation_copy_data = Some(relation_copy_data::<A>);
    routine.relation_copy_for_cluster = Some(relation_copy_for_cluster::<A>);
}
