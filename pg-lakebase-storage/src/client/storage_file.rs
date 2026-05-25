//! Open object read handle with cursor, seek, and direct I/O support.

use std::os::unix::fs::FileExt;

use crate::error::StorageResult;
use crate::handle::FileHandle;
use crate::protocol::{WireRequestPayload, WireResponsePayload};

use super::{StorageClient, unexpected_response};

/// Seek position for [`StorageFile::seek`], mirroring [`std::io::SeekFrom`] without requiring
/// `std::io` trait implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeekFrom {
    /// Seek to an absolute byte offset from the start of the object.
    Start(u64),
    /// Seek relative to the current cursor position (may be negative).
    Current(i64),
    /// Seek relative to the end of the object (offset is typically negative or zero).
    End(i64),
}

/// Open object read handle with a cursor, returned by [`StorageClient::open`].
///
/// Provides two read surfaces:
/// - [`Self::read`] — allocates and returns a `Vec<u8>` (convenient for small reads).
/// - [`Self::read_into`] — fills a caller-provided buffer (zero-copy, avoids per-call allocation).
///
/// The cursor advances by the number of bytes actually read. Use [`Self::seek`] to reposition.
pub struct StorageFile {
    client: StorageClient,
    handle: FileHandle,
    cursor: u64,
    size: u64,
    read_path: ReadPath,
    closed: bool,
}

impl StorageFile {
    pub(super) fn new(
        client: StorageClient,
        handle: FileHandle,
        size: u64,
        read_path: ReadPath,
    ) -> Self {
        Self {
            client,
            handle,
            cursor: 0,
            size,
            read_path,
            closed: false,
        }
    }

    /// Total size of the object in bytes (as reported by the server at open time).
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` when the server promoted this handle to direct file I/O (the client
    /// reads from a local cache file via `pread` rather than issuing wire READ RPCs).
    pub fn is_direct_io(&self) -> bool {
        matches!(self.read_path, ReadPath::Direct(_))
    }

    /// Current cursor position (byte offset from start).
    pub fn position(&self) -> u64 {
        self.cursor
    }

    /// Repositions the read cursor. Returns the new absolute position.
    ///
    /// Seeking past the end of the object is allowed (subsequent reads will return empty).
    /// Seeking before the start saturates at 0.
    pub fn seek(&mut self, pos: SeekFrom) -> u64 {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.cursor.saturating_add(offset as u64)
                } else {
                    self.cursor.saturating_sub(offset.unsigned_abs())
                }
            }
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.size.saturating_add(offset as u64)
                } else {
                    self.size.saturating_sub(offset.unsigned_abs())
                }
            }
        };
        self.cursor = new_pos;
        new_pos
    }

    /// Reads up to `len` bytes from the given absolute offset, without modifying the cursor.
    ///
    /// Returns an empty `Vec` when `offset` is at or past EOF.
    pub fn read_at(&self, offset: u64, len: u32) -> StorageResult<Vec<u8>> {
        if let ReadPath::Direct(reader) = &self.read_path {
            let clamped =
                std::cmp::min(len as u64, self.size.saturating_sub(offset)) as usize;
            if clamped == 0 {
                return Ok(Vec::new());
            }
            return reader.read_at_exact(offset, clamped);
        }
        self.client.read_alloc(self.handle, offset, len)
    }

    /// Reads into a caller-provided buffer from the given absolute offset, without modifying the
    /// cursor.
    ///
    /// Returns the number of bytes written to `buf`. Returns `0` when `offset` is at or past EOF.
    pub fn read_at_into(&self, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = std::cmp::min(buf.len(), u32::MAX as usize) as u32;
        if let ReadPath::Direct(reader) = &self.read_path {
            let clamped =
                std::cmp::min(len as u64, self.size.saturating_sub(offset)) as usize;
            if clamped == 0 {
                return Ok(0);
            }
            return reader.read_at_into(offset, &mut buf[..clamped]);
        }
        let result = self.client.read_into(
            self.handle,
            offset,
            len,
            &mut buf[..len as usize],
        )?;
        Ok(result.bytes_read)
    }

    /// Reads up to `len` bytes starting at the current cursor, returning them as a new `Vec`.
    ///
    /// The cursor advances by the number of bytes returned. Returns an empty `Vec` when the
    /// cursor is at or past EOF.
    pub fn read(&mut self, len: u32) -> StorageResult<Vec<u8>> {
        if let ReadPath::Direct(reader) = &self.read_path {
            let offset = self.cursor;
            let len =
                std::cmp::min(len as u64, self.size.saturating_sub(offset)) as usize;
            if len == 0 {
                return Ok(Vec::new());
            }
            let data = reader.read_at_exact(offset, len)?;
            self.cursor += data.len() as u64;
            return Ok(data);
        }
        let data = self.client.read_alloc(self.handle, self.cursor, len)?;
        self.cursor += data.len() as u64;
        Ok(data)
    }

    /// Reads into a caller-provided buffer, returning the number of bytes written to `buf`.
    ///
    /// At most `buf.len()` bytes are read (clamped to `u32::MAX` for the wire protocol).
    /// The cursor advances by the number of bytes returned. Returns `0` at EOF.
    pub fn read_into(&mut self, buf: &mut [u8]) -> StorageResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = std::cmp::min(buf.len(), u32::MAX as usize) as u32;
        if let ReadPath::Direct(reader) = &self.read_path {
            let offset = self.cursor;
            let clamped =
                std::cmp::min(len as u64, self.size.saturating_sub(offset)) as usize;
            if clamped == 0 {
                return Ok(0);
            }
            let n = reader.read_at_into(offset, &mut buf[..clamped])?;
            self.cursor += n as u64;
            return Ok(n);
        }
        let result = self.client.read_into(
            self.handle,
            self.cursor,
            len,
            &mut buf[..len as usize],
        )?;
        self.cursor += result.bytes_read as u64;
        Ok(result.bytes_read)
    }

    /// Closes the server-side handle, releasing cache activity leases and fill sessions.
    ///
    /// Calling `close` on an already-closed file is a no-op. Dropping a `StorageFile` without
    /// calling `close` will attempt to close it automatically (errors are silently ignored in
    /// `Drop`).
    pub fn close(&mut self) -> StorageResult<()> {
        if self.closed {
            return Ok(());
        }
        let response = self.client.request(WireRequestPayload::Close {
            handle: self.handle,
        })?;
        match response.0 {
            WireResponsePayload::Close => {
                self.closed = true;
                Ok(())
            }
            other => Err(unexpected_response("close", &other)),
        }
    }
}

impl Drop for StorageFile {
    fn drop(&mut self) {
        // Match ordinary file-handle semantics for callers that forget to call
        // `close()`. Errors are intentionally ignored: drop cannot report them, and
        // connection teardown will release any remaining server-side handles.
        let _ = self.close();
    }
}

pub(super) enum ReadPath {
    Direct(DirectReader),
    Mediated,
}

pub(super) struct DirectReader {
    file: std::fs::File,
}

impl DirectReader {
    pub(super) fn new(file: std::fs::File) -> Self {
        Self { file }
    }

    fn read_at_exact(&self, offset: u64, len: usize) -> StorageResult<Vec<u8>> {
        let mut data = vec![0_u8; len];
        let n = self.read_at_into(offset, &mut data)?;
        data.truncate(n);
        Ok(data)
    }

    fn read_at_into(&self, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let mut read = 0;
        while read < buf.len() {
            let n = self.file.read_at(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                break;
            }
            read += n;
        }
        Ok(read)
    }
}
