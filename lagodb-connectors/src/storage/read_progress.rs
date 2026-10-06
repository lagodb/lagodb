//! Shared physical-read accounting for typed COPY progress.

use std::cell::Cell;
use std::io::{self, Read};
use std::rc::Rc;

/// A backend-local counter shared with a format reader after that reader has
/// taken ownership of its `InputFile` adapter.
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

/// Count physical reads below format buffering and decompression.
pub(crate) struct ProgressReader<R> {
    reader: R,
    progress: Option<ReadProgress>,
}

impl<R> ProgressReader<R> {
    pub(crate) fn new(reader: R, progress: Option<ReadProgress>) -> Self {
        Self { reader, progress }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.reader.read(output)?;
        if let Some(progress) = &self.progress {
            progress.record(read);
        }
        Ok(read)
    }
}
