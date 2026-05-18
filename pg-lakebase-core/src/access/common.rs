//! Shared FFI container and session support for access callbacks.
//!
//! `FfiContainer` models the common PostgreSQL-owned wrapper used by scan and
//! index-fetch callbacks: a C base struct must be the first field, followed by
//! the lifecycle context and Rust session pointer.  The Rust session is still
//! dropped by the lifecycle memory-context callback, so normal end callbacks
//! and PostgreSQL ERROR unwinds share the same ownership boundary.

use std::ffi::CStr;

use crate::tuple::{Row, TupleSlotWriter};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;

use super::lifecycle;

#[repr(C)]
pub(crate) struct FfiContainer<B, T> {
    base: B,
    lifecycle_ctx: pg_sys::MemoryContext,
    session: *mut AmFfiSession<T>,
}

pub(crate) struct AmFfiSession<T> {
    pub(crate) am_instance: T,
    pub(crate) row: Row,
    tmp_ctx: pg_sys::MemoryContext,
}

impl<B, T> FfiContainer<B, T> {
    pub(crate) unsafe fn alloc(
        parent: pg_sys::MemoryContext,
        lifecycle_ctx_name: &'static CStr,
    ) -> *mut Self {
        unsafe {
            let lifecycle_ctx =
                lifecycle::create_child_context(parent, lifecycle_ctx_name);
            let container = lifecycle::palloc0_in_context::<Self>(lifecycle_ctx);
            (*container).lifecycle_ctx = lifecycle_ctx;
            container
        }
    }

    pub(crate) unsafe fn from_base_ptr(base: *mut B) -> *mut Self {
        base.cast()
    }

    pub(crate) fn base(&self) -> &B {
        &self.base
    }

    pub(crate) fn base_mut(&mut self) -> &mut B {
        &mut self.base
    }

    pub(crate) unsafe fn init_session(
        &mut self,
        am_instance: T,
        tmp_ctx_name: &'static CStr,
        natts: usize,
    ) {
        unsafe {
            let tmp_ctx =
                lifecycle::create_child_context(self.lifecycle_ctx, tmp_ctx_name);
            let session = AmFfiSession::new(am_instance, tmp_ctx, natts);
            self.session =
                lifecycle::leak_state_in_context(self.lifecycle_ctx, session);
        }
    }

    pub(crate) unsafe fn session_mut(&mut self) -> &mut AmFfiSession<T> {
        debug_assert!(!self.session.is_null());
        unsafe { &mut *self.session }
    }

    pub(crate) unsafe fn session_mut_if_initialized(
        &mut self,
    ) -> Option<&mut AmFfiSession<T>> {
        if self.session.is_null() {
            None
        } else {
            Some(unsafe { &mut *self.session })
        }
    }

    pub(crate) unsafe fn finish_with<R>(
        container: *mut Self,
        finish: impl FnOnce(&mut AmFfiSession<T>) -> R,
    ) -> Option<R> {
        unsafe {
            if (*container).session.is_null() {
                Self::delete_lifecycle_context(container);
                return None;
            }

            let result = {
                let session = &mut *(*container).session;
                finish(session)
            };

            (*container).session = std::ptr::null_mut();
            Self::delete_lifecycle_context(container);
            Some(result)
        }
    }

    unsafe fn delete_lifecycle_context(container: *mut Self) {
        unsafe {
            let lifecycle_ctx = (*container).lifecycle_ctx;
            if !lifecycle_ctx.is_null() {
                pg_sys::MemoryContextDelete(lifecycle_ctx);
            }
        }
    }
}

impl<T> AmFfiSession<T> {
    fn new(am_instance: T, tmp_ctx: pg_sys::MemoryContext, natts: usize) -> Self {
        Self {
            am_instance,
            row: Row::with_capacity(natts),
            tmp_ctx,
        }
    }

    pub(crate) unsafe fn reset_tmp_context(&mut self) {
        unsafe {
            pg_sys::MemoryContextReset(self.tmp_ctx);
        }
    }

    pub(crate) unsafe fn write_row_to_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), ErrorReport> {
        unsafe { TupleSlotWriter::new(slot, self.tmp_ctx).write_row(&mut self.row) }
    }
}
