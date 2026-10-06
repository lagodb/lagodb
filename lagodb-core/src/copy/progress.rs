//! Statement-scoped physical output progress, published at file boundaries.

use std::cell::Cell;
use std::rc::Rc;

use pgrx::pg_sys;

use super::pg::CopyBridge;

/// A backend-local capability for byte COPY destinations to publish encoded
/// bytes after completing a file, before publishing or uploading it.
///
/// The driver revokes this capability before releasing its PostgreSQL state.
/// Retaining a clone therefore cannot access an ended COPY state. No progress
/// operation is needed for individual rows or encoder writes.
#[derive(Clone)]
pub struct CopyOutputProgress {
    state: Rc<Cell<Option<pg_sys::CopyToState>>>,
}

impl CopyOutputProgress {
    /// Publish cumulative physical bytes for completed encoders. Include
    /// framing and compression trailers, and call before file publication.
    pub fn publish(&self, bytes_produced: u64) {
        if let Some(state) = self.state.get() {
            // SAFETY: only the owning driver can create/revoke this capability,
            // and Rc makes it backend-thread-affine. The C updater only writes
            // the COPY counter and PostgreSQL's progress entry; it cannot ERROR.
            unsafe { CopyBridge::update_routed_to_progress(state, bytes_produced) };
        }
    }
}

/// Sole owner of the capability's lifetime. Drop also covers initialization
/// failures and Rust unwinding before a driver has been fully constructed.
pub(super) struct CopyOutputProgressGuard {
    progress: CopyOutputProgress,
}

impl CopyOutputProgressGuard {
    /// # Safety
    ///
    /// The owning driver must revoke the capability before ending `state`.
    pub(super) unsafe fn new(state: pg_sys::CopyToState) -> Self {
        Self {
            progress: CopyOutputProgress {
                state: Rc::new(Cell::new(Some(state))),
            },
        }
    }

    pub(super) fn progress(&self) -> &CopyOutputProgress {
        &self.progress
    }

    pub(super) fn revoke(&self) {
        self.progress.state.set(None);
    }
}

impl Drop for CopyOutputProgressGuard {
    fn drop(&mut self) {
        self.revoke();
    }
}
