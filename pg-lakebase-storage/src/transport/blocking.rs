//! Blocking mirror of the async transport, used by the synchronous test/tooling client.
//!
//! Kept separate from [`super::frame`]: sharing a trait between sync and async variants of I/O
//! tends to force awkward generics on callers, and the two paths have no real logic overlap beyond
//! the length-prefix format itself.

use std::io::{self, IoSlice, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream as StdUnixStream;

use crate::error::{StorageError, StorageResult};
use crate::protocol::MAX_FRAME_BYTES;

use super::fd_channel::recv_blocking;

/// Cursor over one length-prefixed blocking frame.
///
/// The length prefix is consumed up front, then callers can read fixed-size header fields and
/// stream the payload directly into their destination buffer without materializing the whole frame.
pub(crate) struct BlockingFrameCursor<'a, R: Read> {
    reader: &'a mut R,
    len: usize,
    remaining: usize,
}

impl<'a, R: Read> BlockingFrameCursor<'a, R> {
    pub(crate) fn read_from(reader: &'a mut R) -> StorageResult<Option<Self>> {
        let mut len_buf = [0_u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(StorageError::protocol(format!("frame too large: {len}")));
        }
        Ok(Some(Self {
            reader,
            len,
            remaining: len,
        }))
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> StorageResult<()> {
        if buf.len() > self.remaining {
            return Err(StorageError::protocol(format!(
                "short frame: need {} bytes, have {}",
                buf.len(),
                self.remaining
            )));
        }
        self.reader.read_exact(buf)?;
        self.remaining -= buf.len();
        Ok(())
    }

    pub(crate) fn read_remaining_after(&mut self, prefix: &[u8]) -> StorageResult<Vec<u8>> {
        let mut frame = Vec::with_capacity(prefix.len() + self.remaining);
        frame.extend_from_slice(prefix);
        let prefix_len = frame.len();
        frame.resize(prefix_len + self.remaining, 0);
        self.read_exact(&mut frame[prefix_len..])?;
        Ok(frame)
    }

    pub(crate) fn discard_remaining(&mut self) -> StorageResult<()> {
        let mut scratch = [0_u8; 8192];
        while self.remaining > 0 {
            let len = self.remaining.min(scratch.len());
            self.read_exact(&mut scratch[..len])?;
        }
        Ok(())
    }
}

pub fn read_frame_blocking<R: Read>(reader: &mut R) -> StorageResult<Option<Vec<u8>>> {
    let Some(mut cursor) = BlockingFrameCursor::read_from(reader)? else {
        return Ok(None);
    };
    let mut frame = vec![0; cursor.len()];
    cursor.read_exact(&mut frame)?;
    Ok(Some(frame))
}

pub fn write_frame_blocking<W: Write>(writer: &mut W, frame: &[u8]) -> StorageResult<()> {
    let len = frame.len();
    if len > MAX_FRAME_BYTES {
        return Err(StorageError::protocol(format!("frame too large: {len}")));
    }
    let len_buf = (len as u32).to_be_bytes();
    write_all_two_vectored(writer, &len_buf, frame)?;
    writer.flush()?;
    Ok(())
}

fn write_all_two_vectored<W: Write>(writer: &mut W, mut first: &[u8], mut second: &[u8]) -> std::io::Result<()> {
    while !first.is_empty() || !second.is_empty() {
        let written = if first.is_empty() {
            writer.write(second)?
        } else if second.is_empty() {
            writer.write(first)?
        } else {
            writer.write_vectored(&[IoSlice::new(first), IoSlice::new(second)])?
        };
        if written == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole frame"));
        }
        if written < first.len() {
            first = &first[written..];
        } else {
            let second_written = written - first.len();
            first = &[];
            second = &second[second_written..];
        }
    }
    Ok(())
}

pub fn read_fd_blocking(stream: &mut StdUnixStream) -> StorageResult<OwnedFd> {
    recv_blocking(stream)
}
