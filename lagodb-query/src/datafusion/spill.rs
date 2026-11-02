//! DataFusion spill files stored in PostgreSQL temporary files.
//!
//! Every operation runs on the backend thread: DataFusion writes from the
//! operator that overflowed, and reads from a task on the current-thread
//! runtime. PostgreSQL I/O is synchronous here, as it is for the executor's own
//! tuplestores, so these streams are always ready and never wait on a waker.
//! Interrupts are checked once per chunk because a merge phase can run for a
//! long time without polling a table scan.

use std::io::{self, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::{SpillFile, SpillWriter, TempFileFactory};
use futures::Stream;
use lagodb_core::backend_process::BackendProcess;
use lagodb_core::diag::PgReportError;
use lagodb_core::temp_file::PgTempFile;
use pgrx::pg_sys;

use super::error::QueryExecutionError;

/// Matches PostgreSQL's 32-block merge read-ahead (`MERGE_BUFFER_SIZE`).
const SPILL_CHUNK_BYTES: usize = 32 * pg_sys::BLCKSZ as usize;

/// Creates DataFusion spill files through PostgreSQL's temporary-file layer.
pub(super) struct PgTempFileFactory;

impl PgTempFileFactory {
    /// Disk manager for one execution environment. `temp_file_limit` owns the
    /// size quota. Fan-in follows PostgreSQL's own sort, because an external
    /// merge holds one batch per input and an unbounded merge can outgrow a
    /// small `work_mem`.
    pub(super) fn disk_manager(work_mem_bytes: usize) -> DiskManagerBuilder {
        // SAFETY: a pure calculation over its argument.
        let fan_in = unsafe { pg_sys::tuplesort_merge_order(work_mem_bytes as i64) };
        DiskManagerBuilder::default()
            .with_temp_file_factory(Arc::new(Self))
            .with_max_spill_merge_fan_in(fan_in as usize)
    }
}

impl TempFileFactory for PgTempFileFactory {
    fn create_temp_file(&self, _description: &str) -> Result<Arc<dyn SpillFile>> {
        BackendTempFile::check_interrupts()?;
        let file = PgTempFile::create().map_err(BackendTempFile::external)?;
        Ok(Arc::new(PgSpillFile(Arc::new(BackendTempFile {
            file,
            written: AtomicU64::new(0),
        }))))
    }
}

struct BackendTempFile {
    file: PgTempFile,
    written: AtomicU64,
}

// SAFETY: DataFusion's spill traits require Send + Sync, but LagoDB creates,
// polls and drops these values only on the backend's current-thread runtime.
// Tasks DataFusion spawns stay on that thread, and this path starts no blocking
// pool, so no PostgreSQL state is reached from another OS thread.
unsafe impl Send for BackendTempFile {}
// SAFETY: as above; shared references are only used on the backend thread.
unsafe impl Sync for BackendTempFile {}

impl BackendTempFile {
    /// Append at the end of the file. A spill file has one writer at a time.
    fn append(&self, bytes: &[u8]) -> Result<()> {
        Self::check_interrupts()?;
        let offset = self.written.load(Ordering::Relaxed);
        self.file.write_at(bytes, offset).map_err(Self::external)?;
        self.written
            .store(offset + bytes.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Read the next chunk at `offset`, or `None` at the written end.
    fn read_chunk(&self, offset: u64) -> Result<Option<Bytes>> {
        Self::check_interrupts()?;
        let mut chunk = vec![0; SPILL_CHUNK_BYTES];
        let read = self
            .file
            .read_at(&mut chunk, offset)
            .map_err(Self::external)?;
        if read == 0 {
            return Ok(None);
        }
        chunk.truncate(read);
        Ok(Some(Bytes::from(chunk)))
    }

    /// Spill I/O can run on a spawned task, which must not unwind. A held
    /// interrupt cannot be serviced here at all, so it ends the stream instead,
    /// and the holder services it when its guard drops.
    fn check_interrupts() -> Result<()> {
        if !BackendProcess::interrupts_held() {
            return BackendProcess::check_interrupts().map_err(Self::external);
        }
        if BackendProcess::query_interrupt_pending() {
            return Err(QueryExecutionError::interrupted());
        }
        Ok(())
    }

    fn external(error: PgReportError) -> DataFusionError {
        DataFusionError::External(Box::new(error))
    }
}

struct PgSpillFile(Arc<BackendTempFile>);

impl SpillFile for PgSpillFile {
    fn size(&self) -> Option<u64> {
        Some(self.0.written.load(Ordering::Relaxed))
    }

    fn read_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        Ok(Box::pin(PgSpillReader {
            file: Arc::clone(&self.0),
            offset: 0,
        }))
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        Ok(Box::new(PgSpillWriter {
            file: Arc::clone(&self.0),
            pending: Vec::with_capacity(SPILL_CHUNK_BYTES),
        }))
    }
}

/// One reader's position. Nested-loop replay reads a file more than once, and
/// a repartition reader follows its writer, so readers share nothing else.
struct PgSpillReader {
    file: Arc<BackendTempFile>,
    offset: u64,
}

impl Stream for PgSpillReader {
    type Item = Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let chunk = this.file.read_chunk(this.offset).transpose();
        if let Some(Ok(bytes)) = &chunk {
            this.offset += bytes.len() as u64;
        }
        Poll::Ready(chunk)
    }
}

/// Arrow writes each IPC message as many small slices, so writes are gathered
/// here. `flush` must publish everything: a repartition reader starts on a batch
/// as soon as the writer has flushed it.
struct PgSpillWriter {
    file: Arc<BackendTempFile>,
    pending: Vec<u8>,
}

impl Write for PgSpillWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.pending.len() + bytes.len() > SPILL_CHUNK_BYTES {
            self.flush()?;
        }
        if bytes.len() >= SPILL_CHUNK_BYTES {
            self.file.append(bytes).map_err(io::Error::other)?;
        } else {
            self.pending.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            self.file.append(&self.pending).map_err(io::Error::other)?;
            self.pending.clear();
        }
        Ok(())
    }
}

impl SpillWriter for PgSpillWriter {
    fn finish(&mut self) -> Result<()> {
        self.flush().map_err(DataFusionError::IoError)
    }
}
