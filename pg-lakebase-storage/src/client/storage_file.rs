//! Open object read handle with cursor, seek, and direct I/O support.

use std::mem;
use std::os::unix::fs::FileExt;
use std::thread;

use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::protocol::{WireRequestPayload, WireResponsePayload};

use super::{ExternalFdLease, StorageClient};

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
    handle: FileHandle,
    cursor: u64,
    size: u64,
    direct_io: bool,
    state: StorageFileState,
}

enum StorageFileState {
    Open {
        client: StorageClient,
        read_path: ReadPath,
    },
    Closed,
}

impl StorageFile {
    pub(super) fn new(
        client: StorageClient,
        handle: FileHandle,
        size: u64,
        read_path: ReadPath,
    ) -> Self {
        let direct_io = matches!(read_path, ReadPath::Direct(_));
        Self {
            handle,
            cursor: 0,
            size,
            direct_io,
            state: StorageFileState::Open { client, read_path },
        }
    }

    /// Total size of the object in bytes (as reported by the server at open time).
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` when the server promoted this handle to direct file I/O (the client
    /// reads from a local cache file via `pread` rather than issuing wire READ RPCs).
    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    /// Returns whether the connection that owns this server-side handle is
    /// still usable locally.
    ///
    /// A false result means the next operation must reopen the object through
    /// a fresh client generation. Direct-I/O reads do not need the connection,
    /// but callers may still use this method to decide whether cleanup RPCs
    /// can be attempted.
    pub fn is_connection_usable(&self) -> bool {
        match &self.state {
            StorageFileState::Open { client, .. } => client.is_usable(),
            StorageFileState::Closed => false,
        }
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
        let (client, read_path) = self.open_resources()?;
        match read_path {
            ReadPath::Direct(reader) => {
                let clamped =
                    std::cmp::min(len as u64, self.size.saturating_sub(offset))
                        as usize;
                if clamped == 0 {
                    return Ok(Vec::new());
                }
                reader.read_at_exact(offset, clamped)
            }
            ReadPath::Mediated => client.read_alloc(self.handle, offset, len),
        }
    }

    /// Reads into a caller-provided buffer from the given absolute offset, without modifying the
    /// cursor.
    ///
    /// Returns the number of bytes written to `buf`. Returns `0` when `offset` is at or past EOF.
    pub fn read_at_into(&self, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let (client, read_path) = self.open_resources()?;
        if buf.is_empty() {
            return Ok(0);
        }
        let len = std::cmp::min(buf.len(), u32::MAX as usize) as u32;
        match read_path {
            ReadPath::Direct(reader) => {
                let clamped =
                    std::cmp::min(len as u64, self.size.saturating_sub(offset))
                        as usize;
                if clamped == 0 {
                    return Ok(0);
                }
                reader.read_at_into(offset, &mut buf[..clamped])
            }
            ReadPath::Mediated => {
                client.read_into(self.handle, offset, len, &mut buf[..len as usize])
            }
        }
    }

    /// Reads up to `len` bytes starting at the current cursor, returning them as a new `Vec`.
    ///
    /// The cursor advances by the number of bytes returned. Returns an empty `Vec` when the
    /// cursor is at or past EOF.
    pub fn read(&mut self, len: u32) -> StorageResult<Vec<u8>> {
        let offset = self.cursor;
        let data = {
            let (client, read_path) = self.open_resources()?;
            match read_path {
                ReadPath::Direct(reader) => {
                    let len =
                        std::cmp::min(len as u64, self.size.saturating_sub(offset))
                            as usize;
                    if len == 0 {
                        Vec::new()
                    } else {
                        reader.read_at_exact(offset, len)?
                    }
                }
                ReadPath::Mediated => client.read_alloc(self.handle, offset, len)?,
            }
        };
        self.cursor += data.len() as u64;
        Ok(data)
    }

    /// Reads into a caller-provided buffer, returning the number of bytes written to `buf`.
    ///
    /// At most `buf.len()` bytes are read (clamped to `u32::MAX` for the wire protocol).
    /// The cursor advances by the number of bytes returned. Returns `0` at EOF.
    pub fn read_into(&mut self, buf: &mut [u8]) -> StorageResult<usize> {
        let (client, read_path) = self.open_resources()?;
        if buf.is_empty() {
            return Ok(0);
        }
        let len = std::cmp::min(buf.len(), u32::MAX as usize) as u32;
        let offset = self.cursor;
        let bytes_read = match read_path {
            ReadPath::Direct(reader) => {
                let clamped =
                    std::cmp::min(len as u64, self.size.saturating_sub(offset))
                        as usize;
                if clamped == 0 {
                    0
                } else {
                    reader.read_at_into(offset, &mut buf[..clamped])?
                }
            }
            ReadPath::Mediated => client.read_into(
                self.handle,
                offset,
                len,
                &mut buf[..len as usize],
            )?,
        };
        self.cursor += bytes_read as u64;
        Ok(bytes_read)
    }

    fn open_resources(&self) -> StorageResult<(&StorageClient, &ReadPath)> {
        match &self.state {
            StorageFileState::Open { client, read_path } => Ok((client, read_path)),
            StorageFileState::Closed => {
                Err(StorageError::closed_handle(self.handle.0))
            }
        }
    }

    /// Closes the server-side handle, releasing cache activity leases and fill sessions.
    ///
    /// Calling `close` on an already-closed file is a no-op. Dropping a `StorageFile` without
    /// calling `close` makes one bounded close attempt. Cleanup errors cannot
    /// be returned from `Drop`, so they invalidate the connection and let
    /// server-side connection teardown release the handle.
    pub fn close(&mut self) -> StorageResult<()> {
        let response = match &self.state {
            StorageFileState::Open { client, .. } => {
                client.request(WireRequestPayload::Close {
                    handle: self.handle,
                })?
            }
            StorageFileState::Closed => return Ok(()),
        };
        match response.0 {
            WireResponsePayload::Close => {
                self.state = StorageFileState::Closed;
                Ok(())
            }
            other => match &self.state {
                StorageFileState::Open { client, .. } => {
                    client.reject_unexpected("close", &other)
                }
                StorageFileState::Closed => {
                    unreachable!("file cannot close while processing its response")
                }
            },
        }
    }
}

impl Drop for StorageFile {
    fn drop(&mut self) {
        let (client, read_path) =
            match mem::replace(&mut self.state, StorageFileState::Closed) {
                StorageFileState::Open { client, read_path } => (client, read_path),
                StorageFileState::Closed => return,
            };
        // The local direct-I/O descriptor is no longer usable once Drop
        // begins. Release it and its PostgreSQL FD reservation before bounded
        // cleanup I/O that may time out and invalidate the connection.
        drop(read_path);

        // Never start protocol I/O while unwinding or on a connection already
        // poisoned by an interrupted/incomplete operation. Closing the socket
        // makes the server release every handle owned by this connection.
        if thread::panicking() || !client.is_usable() {
            let _ = client.invalidate();
            return;
        }

        let response = client.request_cleanup(WireRequestPayload::Close {
            handle: self.handle,
        });
        match response {
            Ok((WireResponsePayload::Close, _)) => {}
            Ok((other, _)) => {
                let _ = client.reject_unexpected::<()>("close", &other);
            }
            Err(_) => {
                // A framed server error leaves the protocol synchronized, but
                // Drop cannot report or retry it. Invalidate so the server's
                // connection teardown releases the unclosed handle.
                let _ = client.invalidate();
            }
        }
    }
}

pub(super) enum ReadPath {
    Direct(DirectReader),
    Mediated,
}

pub(super) struct DirectReader {
    // Drop the OS descriptor before releasing its accounting lease.
    file: std::fs::File,
    _fd_lease: Option<Box<dyn ExternalFdLease>>,
}

impl DirectReader {
    pub(super) fn new(
        file: std::fs::File,
        fd_lease: Option<Box<dyn ExternalFdLease>>,
    ) -> Self {
        Self {
            file,
            _fd_lease: fd_lease,
        }
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
