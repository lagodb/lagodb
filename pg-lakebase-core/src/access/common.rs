//! Shared FFI container and session support for access callbacks.
//!
//! `FfiContainer` models the common PostgreSQL-owned wrapper used by scan and
//! index-fetch callbacks: a C base struct must be the first field, followed by
//! the lifecycle context and Rust session pointer.  The Rust session is still
//! dropped by the lifecycle memory-context callback, so normal end callbacks
//! and PostgreSQL ERROR unwinds share the same ownership boundary.

use std::ffi::CStr;

use crate::diag::PgReportError;
use crate::handles::OwnedScanKeys;
use crate::tuple::{Row, SlotColumns};
use pgrx::pg_sys;

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
    /// Dispatcher-owned scan keys.
    ///
    /// Populated for table-scan sessions (built once in `scan_begin` and
    /// rewritten in `scan_rescan` when PostgreSQL passes a non-null key
    /// pointer). Always empty for index-fetch sessions, which never receive
    /// scan keys; the field is shared rather than gated by a generic so the
    /// FFI container layout stays uniform across access-method facets.
    ///
    /// TODO: this is a small amount of scan-specific state living on the
    /// shared FFI session container. The cost is one empty `Vec` per
    /// index-fetch session, which is fine, but if more facet-specific
    /// fields appear we should split the session struct (e.g. a generic
    /// `AmFfiSession<T, Extra>` or two separate session types) rather than
    /// keep accumulating fields here.
    pub(crate) scan_keys: OwnedScanKeys,
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
        scan_keys: OwnedScanKeys,
        tmp_ctx_name: &'static CStr,
        natts: usize,
    ) {
        unsafe {
            let tmp_ctx =
                lifecycle::create_child_context(self.lifecycle_ctx, tmp_ctx_name);
            let session = AmFfiSession::new(am_instance, scan_keys, tmp_ctx, natts);
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
    fn new(
        am_instance: T,
        scan_keys: OwnedScanKeys,
        tmp_ctx: pg_sys::MemoryContext,
        natts: usize,
    ) -> Self {
        Self {
            am_instance,
            row: Row::with_capacity(natts),
            scan_keys,
            tmp_ctx,
        }
    }

    pub(crate) unsafe fn reset_tmp_context(&mut self) {
        unsafe {
            pg_sys::MemoryContextReset(self.tmp_ctx);
        }
    }

    /// Per-row context the slot-first path palloc's varlena datums into, reset
    /// once per fetch so slot datum lifetime is bounded to a single row.
    pub(crate) fn tmp_ctx(&self) -> pg_sys::MemoryContext {
        self.tmp_ctx
    }

    /// Materialize the buffered row into the slot through [`SlotColumns`], the
    /// single substrate that owns the unsafe slot writes (shared with the
    /// column path). It does **not** call `ExecStoreVirtualTuple`; the caller
    /// marks the slot non-empty on a produced row, matching `next_into_slot`.
    pub(crate) unsafe fn write_row_to_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), PgReportError> {
        let mut cols = unsafe { SlotColumns::new(slot, self.tmp_ctx) };
        cols.fill_from_row(&mut self.row)
    }
}
