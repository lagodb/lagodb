//! Shared physical-read accounting for typed COPY progress.

use std::cell::Cell;
use std::rc::Rc;

/// A backend-local counter shared with a format reader after that reader has
/// taken ownership of its `StorageFile` adapter.
///
/// The readers are synchronous and backend-thread-affine, so `Rc<Cell<_>>`
/// expresses the actual lifecycle without atomic traffic on every object read.
#[derive(Clone, Default)]
pub(crate) struct ReadProgress(Rc<Cell<u64>>);

impl ReadProgress {
    #[inline]
    pub(crate) fn record(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.0.set(self.0.get().saturating_add(bytes));
    }

    #[inline]
    pub(crate) fn bytes(&self) -> u64 {
        self.0.get()
    }
}
