//! Async length-prefixed framing.
//!
//! [`FrameReader`] provides [`read_frame_buf`](FrameReader::read_frame_buf) which reuses an
//! internal buffer across reads, avoiding per-frame heap allocation. [`FrameWriter`] serializes a
//! frame length plus body in one flush. Free functions ([`read_frame`] / [`write_frame`]) remain
//! available for callers that hold an `AsyncRead` / `AsyncWrite` directly.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{StorageError, StorageResult};
use crate::protocol::MAX_FRAME_BYTES;

/// Reads length-prefixed frames from an `AsyncRead` source.
///
/// Prefer [`Self::read_frame_buf`] on hot paths: it reuses an internal buffer across calls,
/// returning a borrowed slice that avoids per-frame heap allocation.
pub struct FrameReader<R> {
    reader: R,
    buf: Vec<u8>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader, buf: Vec::new() }
    }

    /// Returns `Ok(None)` on clean EOF between frames; any other read error surfaces as `Err`.
    pub async fn read_frame(&mut self) -> StorageResult<Option<Vec<u8>>> {
        read_frame(&mut self.reader).await
    }

    /// Reads one frame, reusing the internal buffer across calls.
    ///
    /// The returned slice borrows the reader's buffer and is valid until the next call to this
    /// method. This avoids the per-frame `Vec` allocation that [`Self::read_frame`] incurs.
    pub async fn read_frame_buf(&mut self) -> StorageResult<Option<&[u8]>> {
        let mut len_buf = [0_u8; 4];
        match self.reader.read_exact(&mut len_buf).await {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(StorageError::protocol(format!("frame too large: {len}")));
        }
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf).await?;
        Ok(Some(&self.buf))
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

/// Writes length-prefixed frames to an `AsyncWrite` sink.
pub struct FrameWriter<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub async fn write_frame(&mut self, frame: &[u8]) -> StorageResult<()> {
        write_frame(&mut self.writer, frame).await
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Reads one length-prefixed frame; yields `Ok(None)` at clean EOF.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> StorageResult<Option<Vec<u8>>> {
    let mut len_buf = [0_u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {},
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(StorageError::protocol(format!("frame too large: {len}")));
    }
    let mut frame = vec![0; len];
    reader.read_exact(&mut frame).await?;
    Ok(Some(frame))
}

/// Writes one length-prefixed frame and flushes.
///
/// Small frames (≤ 8 KiB) are coalesced with their 4-byte length prefix into a single
/// `write_all` to save a syscall on unbuffered writers. Larger frames use two separate
/// writes to avoid allocating and copying up to [`MAX_FRAME_BYTES`] (64 MiB) of data.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &[u8]) -> StorageResult<()> {
    let len = frame.len();
    if len > MAX_FRAME_BYTES {
        return Err(StorageError::protocol(format!("frame too large: {len}")));
    }
    let len_buf = (len as u32).to_be_bytes();
    if len <= 8192 {
        let mut buf = Vec::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(frame);
        writer.write_all(&buf).await?;
    } else {
        writer.write_all(&len_buf).await?;
        writer.write_all(frame).await?;
    }
    writer.flush().await?;
    Ok(())
}
