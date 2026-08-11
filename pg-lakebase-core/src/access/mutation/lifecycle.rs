//! COPY FROM utility lifecycle.
//!
//! ModifyTable execution owns its mutation state inside the outer CustomScan. COPY
//! FROM bypasses ModifyTable, so it alone needs utility-hook boundaries.

use crate::diag::PgReportError;

use super::session;

/// Utility-scoped relation COPY FROM lifecycle.
///
/// The guard is created only after a COPY consumer has been selected. This is
/// important because an external COPY consumer may own a format writer without
/// invoking PostgreSQL's relation COPY executor and therefore must not create
/// a Table-AM session frame accidentally.
pub struct CopyFromLifecycleGuard {
    id: u64,
    finished: bool,
}

impl CopyFromLifecycleGuard {
    pub(crate) fn begin() -> Self {
        let id = session::begin_copy_from_frame();
        Self {
            id,
            finished: false,
        }
    }

    /// Finish every relation-local Table-AM COPY session in this utility
    /// frame. Dropping an unfinished guard aborts the frame immediately;
    /// ResourceOwner cleanup remains as a backend-error safety net.
    pub fn finish(mut self) -> Result<(), PgReportError> {
        let result = session::finish_copy_frame(self.id);
        if result.is_err() {
            // A frame can only be finished while it is the stack top. If that
            // invariant is violated, remove the frame explicitly so its
            // sessions still run their abort cleanup when the frame drops.
            session::abort_copy_frame(self.id);
        }
        self.finished = true;
        result
    }
}

impl Drop for CopyFromLifecycleGuard {
    fn drop(&mut self) {
        if !self.finished {
            session::abort_copy_frame(self.id);
            self.finished = true;
        }
    }
}

pub fn begin_copy_from_lifecycle() -> CopyFromLifecycleGuard {
    CopyFromLifecycleGuard::begin()
}
