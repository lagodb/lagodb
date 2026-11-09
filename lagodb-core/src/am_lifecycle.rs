//! Memory-context owned session state for access-method callbacks.
//!
//! PostgreSQL error cleanup skips the normal end callbacks (`table_endscan`,
//! `index_fetch_end`, `amendscan`, `aminsertcleanup`) and only deletes memory
//! contexts. Rust session state is therefore owned by a PostgreSQL memory
//! context through a reset callback, so the normal end callback and an ERROR
//! unwind share one ownership boundary. pgrx's guarded unwind still runs
//! before control returns to PostgreSQL; raw longjmp must not cross a
//! non-trivial Rust frame.
//!
//! PostgreSQL deletes a context's descendants before it runs the context's
//! reset callbacks, and deleting an ancestor deletes the lifecycle context's
//! siblings in list order (`MemoryContextDelete`). A context a session owns
//! may therefore be gone when the session drops, unless nothing but the
//! session deletes it: PostgreSQL contexts a session owns are
//! [`OwnedBackendContext`](crate::backend::OwnedBackendContext)s. Children
//! of the lifecycle context are held only by the framework.

use std::ptr::NonNull;

use crate::backend::MemoryContextHandle;

/// A lifecycle context and the Rust session installed in it.
///
/// The only constructor installs the session, so an owner reachable through
/// a PostgreSQL-visible pointer (`ii_AmCache`, a scan descriptor) always has
/// one. Deleting the context, either through [`Self::finish_with`] or
/// through PostgreSQL's deletion of an ancestor, drops the session. Dropping
/// the session only destructs it; success-path work belongs to the end
/// callback passed to [`Self::finish_with`].
pub(crate) struct SessionContext<T: 'static> {
    context: MemoryContextHandle,
    session: NonNull<T>,
}

impl<T: 'static> SessionContext<T> {
    /// Install `session` in `context`.
    ///
    /// # Safety
    ///
    /// `context` must not have been deleted, and no other owner may finish
    /// it.
    pub(crate) unsafe fn install(context: MemoryContextHandle, session: T) -> Self {
        Self {
            context,
            session: unsafe { context.install(session) },
        }
    }

    pub(crate) fn context(&self) -> MemoryContextHandle {
        self.context
    }

    /// # Safety
    ///
    /// The lifecycle context must not have been deleted.
    #[inline]
    pub(crate) unsafe fn session_mut(&mut self) -> &mut T {
        // SAFETY: the session lives until the context is deleted.
        unsafe { self.session.as_mut() }
    }

    /// Move this owner into its own lifecycle context, for callers whose
    /// PostgreSQL-visible pointer (`ii_AmCache`) refers to the owner itself.
    ///
    /// # Safety
    ///
    /// The lifecycle context must not have been deleted.
    pub(crate) unsafe fn into_own_context(self) -> *mut Self {
        unsafe {
            let owner = self.context.alloc_zeroed::<Self>();
            owner.write(self);
            owner
        }
    }

    /// Run `finish` on the session, then delete the lifecycle context, which
    /// drops the session.
    ///
    /// The owner may live inside the context it deletes, so this takes a raw
    /// pointer and never touches `this` after the deletion. The caller must
    /// already have cleared every PostgreSQL-visible pointer to the owner, as
    /// `brin.c` clears `ii_AmCache` before freeing its insert state.
    ///
    /// # Safety
    ///
    /// `this` must point to a live owner that no PostgreSQL-visible pointer
    /// will reach again. Neither the lifecycle context nor a descendant may
    /// be `CurrentMemoryContext`, as in an end callback, which PostgreSQL
    /// calls with its own context current.
    pub(crate) unsafe fn finish_with<R>(
        this: *mut Self,
        finish: impl FnOnce(&mut T) -> R,
    ) -> R {
        unsafe {
            let Self {
                context,
                mut session,
            } = this.read();
            let result = finish(session.as_mut());
            // SAFETY: `finish` returned, so any switch it made is undone;
            // the caller guarantees the rest.
            context.delete();
            result
        }
    }
}
