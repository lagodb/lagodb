//! Shared FFI container and session support for access callbacks.
//!
//! `FfiContainer` models the common PostgreSQL-owned wrapper used by scan and
//! index-fetch callbacks: a C base struct must be the first field, followed by
//! the [`SessionContext`] that owns the lifecycle context and Rust session.
//! The container itself lives in that lifecycle context, so normal end
//! callbacks and PostgreSQL ERROR unwinds share the same ownership boundary.
//! It is allocated only after the session is installed, so every published
//! container has one.

use std::ptr::addr_of_mut;

use crate::am_lifecycle::SessionContext;

#[repr(C)]
pub(crate) struct FfiContainer<B, T: 'static> {
    base: B,
    session: SessionContext<T>,
}

impl<B, T: 'static> FfiContainer<B, T> {
    /// Allocate a container with a zeroed base inside `session`'s lifecycle
    /// context. The caller fills the base before publishing it.
    ///
    /// # Safety
    ///
    /// The lifecycle context must not have been deleted, and all-zero bytes
    /// must be a valid `B`.
    pub(crate) unsafe fn alloc(session: SessionContext<T>) -> *mut Self {
        unsafe {
            let container = session.context().alloc_zeroed::<Self>();
            addr_of_mut!((*container).session).write(session);
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

    #[inline]
    pub(crate) unsafe fn session_mut(&mut self) -> &mut T {
        unsafe { self.session.session_mut() }
    }

    /// Run `finish` on the session and delete the container's lifecycle
    /// context, which also frees the container.
    ///
    /// # Safety
    ///
    /// Call only from the end callback, with the container that PostgreSQL
    /// passed in, as [`SessionContext::finish_with`] requires.
    pub(crate) unsafe fn finish_with<R>(
        container: *mut Self,
        finish: impl FnOnce(&mut T) -> R,
    ) -> R {
        unsafe {
            SessionContext::finish_with(addr_of_mut!((*container).session), finish)
        }
    }
}
