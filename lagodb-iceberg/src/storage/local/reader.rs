//! PostgreSQL VFD-backed file reader.

use std::ffi::CString;
use std::io;
use std::ops::Range;

use bytes::Bytes;
use iceberg_lite::Result;
use iceberg_lite::io::FileRead;
use lagodb_core::backend::OwnedVfd;
use pgrx::pg_sys;

/// PostgreSQL VFD-backed file reader.
///
/// Wraps a PostgreSQL virtual file descriptor for range reads; the VFD is
/// closed when the reader drops. Consumers of one opened file share this
/// reader rather than cloning it. The surrounding executor state is
/// responsible for dropping the reader during normal completion and
/// PostgreSQL error unwinding.
#[derive(Debug)]
pub(super) struct LocalFileReader {
    vfd: OwnedVfd,
    path: Box<str>,
    /// Total file size in bytes
    size: u64,
}

// SAFETY: this private adapter implements iceberg-lite `FileRead`, which
// requires `Send + Sync`. The inner `OwnedVfd` remains intentionally
// `!Send + !Sync` so `Send + Sync` is never an accidental compiler-derived
// property; every open, read, and `Drop` runs inside a PostgreSQL-owned serial
// scan on the owning backend thread.
unsafe impl Send for LocalFileReader {}
// SAFETY: the serial iterator never invokes one reader concurrently; shared
// access exists only for the upstream `FileRead` trait surface.
unsafe impl Sync for LocalFileReader {}

impl LocalFileReader {
    /// Open a file for reading using PostgreSQL's VFD system.
    pub(super) fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let Some(vfd) = OwnedVfd::open(&c_path, libc::O_RDONLY) else {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}': {}", path, err),
            ));
        };

        let size = vfd.size();
        if size < 0 {
            let err = io::Error::last_os_error();
            drop(vfd);
            return Err(io::Error::new(
                err.kind(),
                format!("failed to get size of file '{}': {}", path, err),
            ));
        }

        Ok(Self {
            vfd,
            path: path.into(),
            size: size as u64,
        })
    }

    #[inline]
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Read exactly `len` bytes at `offset`. `FileRead` may return fewer bytes
    /// than requested, so repeat until the range is complete; end of file
    /// before that means the file is shorter than the size recorded at open.
    /// The bytes land directly in the vector's spare capacity, which is never
    /// zeroed first.
    fn read_exact_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(len);
        while buffer.len() < len {
            let filled = buffer.len();
            let position = offset + filled as u64;
            let read = self.vfd.read_at_uninit(
                &mut buffer.spare_capacity_mut()[..len - filled],
                position as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_READ,
            );
            if read < 0 {
                let err = io::Error::last_os_error();
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "failed to read from file '{}' at offset {}: {}",
                        self.path, position, err
                    ),
                ));
            }
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "file '{}' ended at offset {} before range {}..{} was read",
                        self.path,
                        position,
                        offset,
                        offset + len as u64
                    ),
                ));
            }
            // SAFETY: the read initialized the `read` bytes that follow the
            // `filled` bytes already in the vector, within its capacity.
            unsafe { buffer.set_len(filled + read as usize) };
        }
        Ok(buffer)
    }
}

impl FileRead for LocalFileReader {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid range: start {} > end {}", range.start, range.end),
            )
            .into());
        }
        if range.end > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "range end {} exceeds file size {} for '{}'",
                    range.end, self.size, self.path
                ),
            )
            .into());
        }

        let len = (range.end - range.start) as usize;
        let buffer = self.read_exact_at(range.start, len)?;
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range(0..self.size)
    }
}
