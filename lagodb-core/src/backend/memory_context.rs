//! PostgreSQL memory contexts used from Rust: a non-owning handle and a
//! context owned by a Rust value.

use std::ffi::CStr;
use std::mem::size_of;
use std::ptr::NonNull;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

/// Handle to a memory context. It does not own the context: whoever
/// deletes it (PostgreSQL with an ancestor, or an owner such as
/// [`OwnedBackendContext`]) decides its lifetime.
#[derive(Clone, Copy)]
pub(crate) struct MemoryContextHandle(pg_sys::MemoryContext);

impl MemoryContextHandle {
    /// Create an AllocSet child of `parent` with PostgreSQL's default sizes.
    ///
    /// # Safety
    ///
    /// `parent` must be a live memory context.
    pub(crate) unsafe fn create(
        parent: pg_sys::MemoryContext,
        name: &'static CStr,
    ) -> Self {
        // SAFETY: the caller guarantees `parent`; PostgreSQL keeps the
        // 'static name pointer.
        Self(unsafe {
            pg_sys::AllocSetContextCreateExtended(
                parent,
                name.as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        })
    }

    #[inline]
    pub(crate) fn as_ptr(self) -> pg_sys::MemoryContext {
        self.0
    }

    /// Create an AllocSet child that is deleted with this context.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted.
    pub(crate) unsafe fn create_child(self, name: &'static CStr) -> Self {
        unsafe { Self::create(self.0, name) }
    }

    /// Allocate a zeroed `U` that lives as long as the context.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted, and all-zero bytes must be a
    /// valid `U` until the caller initializes it.
    pub(crate) unsafe fn alloc_zeroed<U>(self) -> *mut U {
        unsafe { pg_sys::MemoryContextAllocZero(self.0, size_of::<U>()).cast() }
    }

    /// Allocate `len` zeroed `U`s that live as long as the context.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted, and all-zero bytes must be a
    /// valid `U`.
    pub(crate) unsafe fn alloc_zeroed_array<U>(self, len: usize) -> *mut U {
        unsafe { pg_sys::MemoryContextAllocZero(self.0, size_of::<U>() * len).cast() }
    }

    /// Hand `value` to the context: deleting the context drops it.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted.
    pub(crate) unsafe fn install<T: 'static>(self, value: T) -> NonNull<T> {
        let leaked = PgMemoryContexts::For(self.0).leak_and_drop_on_delete(value);
        // SAFETY: `Box::leak` never yields null.
        unsafe { NonNull::new_unchecked(leaked) }
    }

    /// Free everything allocated in the context and delete its children.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted, none of its descendants may be
    /// `CurrentMemoryContext` (`MemoryContextDeleteChildren` deletes them),
    /// and nothing allocated in it may be used afterwards.
    #[inline]
    pub(crate) unsafe fn reset(self) {
        unsafe { pg_sys::MemoryContextReset(self.0) };
    }

    /// Delete the context and its descendants, dropping what was installed
    /// in them.
    ///
    /// # Safety
    ///
    /// The context must not have been deleted, neither it nor any descendant
    /// may be `CurrentMemoryContext` (`MemoryContextDeleteOnly`), and nothing
    /// allocated in them may be used afterwards.
    pub(crate) unsafe fn delete(self) {
        unsafe { pg_sys::MemoryContextDelete(self.0) };
    }
}

/// An AllocSet context under `TopMemoryContext`, deleted when this value is
/// dropped.
///
/// Rust state that PostgreSQL drops through a memory context reset callback,
/// such as an access-method session, cannot own an ordinary child context:
/// when PostgreSQL deletes a common ancestor, it may delete that child
/// before it runs the callback (`MemoryContextDelete` deletes descendants
/// first), and dropping the owner would delete it again. Nothing but this
/// owner deletes a context under `TopMemoryContext`, so it is deleted
/// exactly once, when the owner drops, on success and after ERROR alike.
pub struct OwnedBackendContext(MemoryContextHandle);

impl OwnedBackendContext {
    pub fn new(name: &'static CStr) -> Self {
        // SAFETY: TopMemoryContext lives for the backend.
        Self(unsafe { MemoryContextHandle::create(pg_sys::TopMemoryContext, name) })
    }

    /// The context, for PostgreSQL functions that take one explicitly
    /// (`fmgr_info_cxt`). Valid while `self` is alive.
    pub fn as_ptr(&self) -> pg_sys::MemoryContext {
        self.0.as_ptr()
    }

    /// Run `allocate` with this context current.
    ///
    /// The previous context is restored by a guard, so it is restored also
    /// while a PostgreSQL ERROR unwinds through this frame, before an owner
    /// further up can drop this context.
    ///
    /// # Safety
    ///
    /// PostgreSQL memory allocated while `allocate` runs belongs to this
    /// context. Nothing that refers to it, including values returned from
    /// `allocate` such as a `StringInfo`, may be used after the context is
    /// reset or dropped. The borrow checker cannot see this, as for pgrx's
    /// `PgMemoryContexts::switch_to`.
    pub unsafe fn with<R>(&self, allocate: impl FnOnce() -> R) -> R {
        // SAFETY: CurrentMemoryContext is backend-local and this context
        // outlives the call.
        let _restore = RestoreContext(unsafe { pg_sys::CurrentMemoryContext });
        unsafe { pg_sys::CurrentMemoryContext = self.0.as_ptr() };
        allocate()
    }

    /// Free everything allocated in the context.
    ///
    /// This is safe for the same reason dropping the context is: the
    /// obligation not to use freed allocations lies with [`Self::with`],
    /// which produces them, and with raw users of [`Self::as_ptr`].
    pub fn reset(&mut self) {
        // SAFETY: the context is alive. `with` is the only switch into it
        // and its guard restores the previous context, also on unwind, so
        // `&mut self` rules out the context or a descendant being current.
        unsafe { self.0.reset() };
    }
}

impl Drop for OwnedBackendContext {
    fn drop(&mut self) {
        // SAFETY: only this owner deletes the context, and nothing else
        // parents it. No `with` can be running, as for `reset`, so neither
        // the context nor a descendant is current.
        unsafe { self.0.delete() };
    }
}

struct RestoreContext(pg_sys::MemoryContext);

impl Drop for RestoreContext {
    fn drop(&mut self) {
        // SAFETY: restores the context that was current on entry.
        unsafe { pg_sys::CurrentMemoryContext = self.0 };
    }
}
