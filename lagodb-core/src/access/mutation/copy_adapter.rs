//! Provider-side adaptation of the existing AmCopySession contract.

use std::ffi::c_void;
use std::marker::PhantomData;

use pgrx::pg_sys;

use crate::api::TableAccessMethod;
use crate::runtime_api::{CallbackErrorReport, TableCopyCallbacks};

use super::session::CopyRelationSession;

struct CopyAdapter<A>(PhantomData<A>);

impl TableCopyCallbacks {
    /// Bind the table provider's COPY capability to its concrete TableAM.
    /// The returned callback table must be retained for the backend lifetime.
    pub const fn for_am<A: TableAccessMethod>() -> Self {
        Self {
            bind: CopyAdapter::<A>::bind,
            insert: CopyAdapter::<A>::insert,
            insert_batch: CopyAdapter::<A>::insert_batch,
            finish_bulk: CopyAdapter::<A>::finish_bulk,
            publish: CopyAdapter::<A>::publish,
            complete: CopyAdapter::<A>::complete,
            release: CopyAdapter::<A>::release,
        }
    }
}

// All pointers here belong to this exact-build provider: the caller keeps
// callback arguments live and consumes the error payload synchronously.
impl<A: TableAccessMethod> CopyAdapter<A> {
    unsafe extern "C-unwind" fn bind(
        relation: pg_sys::Relation,
        cid: pg_sys::CommandId,
        output: *mut *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                let binding = CopyRelationSession::<A>::bind(relation, cid)?;
                *output = Box::into_raw(Box::new(binding)).cast();
                Ok(())
            })
        }
    }

    unsafe extern "C-unwind" fn insert(
        handle: *mut c_void,
        slot: *mut pg_sys::TupleTableSlot,
        options: i32,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                (*handle.cast::<CopyRelationSession<A>>()).insert(slot, options)
            })
        }
    }

    unsafe extern "C-unwind" fn insert_batch(
        handle: *mut c_void,
        slots: *mut *mut pg_sys::TupleTableSlot,
        count: i32,
        options: i32,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                (*handle.cast::<CopyRelationSession<A>>())
                    .insert_batch(slots, count, options)
            })
        }
    }

    unsafe extern "C-unwind" fn finish_bulk(
        handle: *mut c_void,
        options: i32,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                (*handle.cast::<CopyRelationSession<A>>()).finish_bulk(options)
            })
        }
    }

    unsafe extern "C-unwind" fn publish(
        handle: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| (*handle.cast::<CopyRelationSession<A>>()).publish())
        }
    }

    unsafe extern "C-unwind" fn complete(
        handle: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                (*handle.cast::<CopyRelationSession<A>>()).complete();
                Ok(())
            })
        }
    }

    unsafe extern "C-unwind" fn release(
        handle: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        unsafe {
            (*error).capture(|| {
                drop(Box::from_raw(handle.cast::<CopyRelationSession<A>>()));
                Ok(())
            })
        }
    }
}
