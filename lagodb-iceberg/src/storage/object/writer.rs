//! Staged local writer for object-storage uploads.

use std::io::{self, Write};
use std::marker::PhantomData;
use std::rc::Rc;

use iceberg_lite::Result;
use iceberg_lite::io::{FileMetadata, FileWrite};
use lagodb_storage::StagingFile;

use super::wait_event::ObjectWaitEvent;

pub(super) struct ObjectWriter {
    staging: Option<StagingFile>,
    bytes_written: u64,
    backend_thread: PhantomData<Rc<()>>,
}

// SAFETY: this private adapter implements iceberg-lite `FileWrite`, which
// requires `Send + Sync`. Its PostgreSQL wait-event reporting and external-FD
// lease remain inside the owning backend's serial mutation lifecycle.
// `backend_thread` intentionally opts out of compiler-derived `Send + Sync`
// from `StagingFile` so this host-thread boundary stays explicit.
unsafe impl Send for ObjectWriter {}
// SAFETY: the writer is uniquely driven until close and is never concurrently
// accessed by the extension executor.
unsafe impl Sync for ObjectWriter {}

impl ObjectWriter {
    pub(super) fn new(staging: StagingFile) -> Self {
        Self {
            staging: Some(staging),
            bytes_written: 0,
            backend_thread: PhantomData,
        }
    }
}

impl Write for ObjectWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let staging = self
            .staging
            .as_mut()
            .ok_or_else(|| io::Error::other("writer already closed"))?;
        let _wait = ObjectWaitEvent::StagingFileWrite.enter();
        staging.write(buf).map_err(io::Error::other)?;
        self.bytes_written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(staging) = &self.staging {
            let _wait = ObjectWaitEvent::StagingFileSync.enter();
            staging.sync().map_err(io::Error::other)?;
        }
        Ok(())
    }
}

impl FileWrite for ObjectWriter {
    fn close(&mut self) -> Result<FileMetadata> {
        // Intentionally do not call `flush()` here. Object writes stage bytes in
        // a local file that `Storage::finalize_write` uploads in the same
        // statement path; a crash before upload aborts the transaction and leaves
        // only cleanup work. Callers that need durable staged bytes before upload
        // can call `flush()` explicitly, but making close fsync every object file
        // would add latency without improving the committed object-store state.
        self.staging.take();
        Ok(FileMetadata {
            size: self.bytes_written,
        })
    }
}
