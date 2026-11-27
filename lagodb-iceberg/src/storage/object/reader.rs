//! Chunked object-storage reader backed by `BackendStorageService`.

use std::cmp;
use std::ops::Range;

use bytes::Bytes;
use iceberg_lite::io::FileRead;
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_storage::{DEFAULT_MAX_READ_SIZE, StorageFile};

use super::injection_points::ObjectInjectionPoints;
use super::wait_event::ObjectWaitEvent;
use crate::error::storage_error_to_iceberg_lite;

// The storage wire protocol accepts a u32 read length per request, and the
// service clamps each response to its configured max_read_size. Keep the
// adapter chunk size aligned with the default service clamp so large Iceberg
// `read_range` calls do not turn into multi-GB direct-I/O allocations.
const OBJECT_READ_CHUNK_LEN: u32 = DEFAULT_MAX_READ_SIZE;

pub(super) struct ObjectReader {
    /// Object key, for diagnostics.
    key: Box<str>,
    file: StorageFile,
}

// SAFETY: `ObjectReader` is the private adapter for
// `iceberg_lite::io::FileRead`. PostgreSQL constructs, uses, and drops every
// reader on one backend thread; the serial iterator never transfers or
// concurrently invokes it. The contained `StorageFile` remains naturally
// `!Send + !Sync`.
unsafe impl Send for ObjectReader {}
unsafe impl Sync for ObjectReader {}

impl ObjectReader {
    pub(super) fn new(key: &str, file: StorageFile) -> Self {
        Self {
            key: key.into(),
            file,
        }
    }

    fn validate_read_range(&self, range: &Range<u64>) -> Result<usize> {
        if range.start > range.end {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "invalid read range: start {} > end {}",
                    range.start, range.end
                ),
            ));
        }

        let size = self.file.size();
        if range.end > size {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "read range end {} exceeds object size {} for '{}'",
                    range.end, size, &*self.key
                ),
            ));
        }

        usize::try_from(range.end - range.start).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "read range length {} exceeds addressable memory on this platform",
                    range.end - range.start
                ),
            )
        })
    }

    fn read_range_chunked(&self, range: Range<u64>) -> Result<Bytes> {
        let len = self.validate_read_range(&range)?;
        if len == 0 {
            return Ok(Bytes::new());
        }

        // `FileRead` returns one Bytes value, so the full logical range still
        // must fit in backend memory. Chunking keeps the storage protocol's
        // u32 request length as a per-read detail instead of a file/range cap.
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "failed to reserve {} bytes for object read '{}': {}",
                    len, &*self.key, err
                ),
            )
        })?;
        // Keep the buffer initialized: `read_at_into` accepts `&mut [u8]`,
        // not a MaybeUninit-backed spare capacity slice.
        data.resize(len, 0);

        let mut offset = range.start;
        let mut remaining = len;
        let mut written = 0;

        let _wait = ObjectWaitEvent::ObjectRead.enter();
        while remaining > 0 {
            let request_len = cmp::min(remaining, OBJECT_READ_CHUNK_LEN as usize);
            let bytes_read = self
                .file
                .read_at_into(offset, &mut data[written..written + request_len])
                .map_err(storage_error_to_iceberg_lite)?;

            if bytes_read == 0 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "object read for '{}' returned 0 bytes before range {}..{} completed",
                        &*self.key, range.start, range.end
                    ),
                ));
            }
            if bytes_read > request_len {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "object read for '{}' returned {} bytes into a {} byte buffer",
                        &*self.key, bytes_read, request_len
                    ),
                ));
            }

            offset += bytes_read as u64;
            written += bytes_read;
            remaining -= bytes_read;
        }

        Ok(Bytes::from(data))
    }
}

impl Drop for ObjectReader {
    fn drop(&mut self) {
        ObjectInjectionPoints::OBJECT_READER_BEFORE_DROP.run();
    }
}

impl FileRead for ObjectReader {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.read_range_chunked(range)
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range_chunked(0..self.file.size())
    }
}
