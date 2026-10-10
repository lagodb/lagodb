//! C executor boundary. Provider callbacks return errors as data; report only
//! after Rust borrows have ended, before control returns to PostgreSQL.

use std::cell::RefCell;
use std::ffi::c_void;

use pgrx::{pg_guard, pg_sys};

use crate::diag::ReportableError;
use crate::runtime_api::CallbackErrorReport;

use super::execution::{CopyBinding, CopyExecution, CopyOwner};
use super::io;

#[repr(C)]
pub(crate) struct CopyWriteBridge {
    context: *mut c_void,
    bind: unsafe extern "C-unwind" fn(
        *mut c_void,
        pg_sys::Relation,
        pg_sys::CommandId,
    ) -> *mut c_void,
    insert:
        unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::TupleTableSlot, i32),
    insert_batch: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut *mut pg_sys::TupleTableSlot,
        i32,
        i32,
    ),
    finish_bulk: unsafe extern "C-unwind" fn(*mut c_void, i32),
    publish: unsafe extern "C-unwind" fn(*mut c_void),
}

impl CopyWriteBridge {
    pub(super) fn new(owner: &CopyOwner) -> Self {
        Self {
            context: owner.context(),
            bind: Self::bind,
            insert: Self::insert,
            insert_batch: Self::insert_batch,
            finish_bulk: Self::finish_bulk,
            publish: Self::publish,
        }
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bind(
        context: *mut c_void,
        relation: pg_sys::Relation,
        cid: pg_sys::CommandId,
    ) -> *mut c_void {
        let result = unsafe {
            (*context.cast::<RefCell<CopyExecution>>())
                .borrow_mut()
                .bind(relation, cid)
        };
        match result {
            Ok(binding) => binding,
            Err(error) => io::report(error),
        }
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn insert(
        pointer: *mut c_void,
        slot: *mut pg_sys::TupleTableSlot,
        options: i32,
    ) {
        let binding = unsafe { &*pointer.cast::<CopyBinding>() };
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (binding.callbacks.insert)(binding.handle, slot, options, &mut error)
        };
        unsafe { error.into_result(status, "COPY insert") }.report_unwrap();
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn insert_batch(
        pointer: *mut c_void,
        slots: *mut *mut pg_sys::TupleTableSlot,
        count: i32,
        options: i32,
    ) {
        let binding = unsafe { &*pointer.cast::<CopyBinding>() };
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (binding.callbacks.insert_batch)(
                binding.handle,
                slots,
                count,
                options,
                &mut error,
            )
        };
        unsafe { error.into_result(status, "COPY batch insert") }.report_unwrap();
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn finish_bulk(pointer: *mut c_void, options: i32) {
        let binding = unsafe { &*pointer.cast::<CopyBinding>() };
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (binding.callbacks.finish_bulk)(binding.handle, options, &mut error)
        };
        unsafe { error.into_result(status, "COPY bulk finish") }.report_unwrap();
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn publish(context: *mut c_void) {
        let result = unsafe {
            (*context.cast::<RefCell<CopyExecution>>())
                .borrow_mut()
                .publish()
        };
        result.report_unwrap();
    }
}
