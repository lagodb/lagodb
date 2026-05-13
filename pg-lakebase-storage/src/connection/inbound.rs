//! Inbound request reader: owns the read half of the socket and produces decoded wire requests.
//!
//! The reader reuses an internal buffer across frames so consecutive `next_event` calls do not
//! each allocate a fresh `Vec`.

use std::io;

use tokio::io::AsyncReadExt;
use tokio::net::unix::OwnedReadHalf;

use crate::error::{StorageError, StorageResult};
use crate::protocol::{decode_request, WireRequest, MAX_FRAME_BYTES};

/// Event produced by the inbound reader each loop iteration.
pub(super) enum InboundEvent {
    /// A full request frame was decoded. Boxed to keep the enum compact.
    ///
    /// READ requests are small and hot, so this does add one allocation on that path. Keep this
    /// shape until profiling shows small-READ QPS is allocator-bound; then consider a dedicated
    /// stack-carried READ event while keeping larger request payloads boxed.
    Request(Box<WireRequest>),
    /// The peer closed the read half cleanly.
    Closed,
}

pub(super) struct InboundReader {
    reader: OwnedReadHalf,
    buf: Vec<u8>,
}

impl InboundReader {
    pub(super) fn new(reader: OwnedReadHalf) -> Self {
        Self { reader, buf: Vec::new() }
    }

    pub(super) async fn next_event(&mut self) -> StorageResult<InboundEvent> {
        let mut len_buf = [0_u8; 4];
        match self.reader.read_exact(&mut len_buf).await {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(InboundEvent::Closed),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(StorageError::protocol(format!("frame too large: {len}")));
        }
        self.buf.resize(len, 0);
        self.reader.read_exact(&mut self.buf).await?;
        Ok(InboundEvent::Request(Box::new(decode_request(&self.buf)?)))
    }
}
