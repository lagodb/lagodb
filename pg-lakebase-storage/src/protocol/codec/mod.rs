//! Binary codec for wire requests and responses.
//!
//! The `WireEncode` / `WireDecode` traits live in [`traits`], with trait impls for every wire type
//! colocated with that type's on-the-wire layout ([`types`] for shared types, [`message`] for the
//! request/response envelopes). The free functions in this module are thin entry points that
//! delegate to those trait impls.

mod header;
mod message;
mod traits;
mod types;

use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;

use super::limits::{MAX_READ_RESPONSE_DATA_BYTES, READ_REQUEST_BYTES, READ_RESPONSE_PREFIX_BYTES};
use super::model::{WireRequest, WireResponse};
use super::op::WireOp;

use self::header::{FrameHeader, FrameKind};
use self::traits::{ensure_eof, get_u16, get_u32, WireDecode, WireEncode};

pub fn encode_request(request: &WireRequest) -> StorageResult<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    request.encode(&mut out)?;
    Ok(out)
}

pub fn decode_request(input: &[u8]) -> StorageResult<WireRequest> {
    let mut cursor = input;
    WireRequest::decode(&mut cursor)
}

pub fn encode_response(response: &WireResponse) -> StorageResult<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    response.encode(&mut out)?;
    Ok(out)
}

pub fn decode_response(input: &[u8]) -> StorageResult<WireResponse> {
    let mut cursor = input;
    WireResponse::decode(&mut cursor)
}

/// Builds the fixed-size frame for a READ request.
///
/// The blocking client uses this in its READ hot path to avoid allocating a `Vec` for a request
/// whose wire shape is entirely fixed-size.
pub(crate) fn encode_read_request(
    request_id: u64,
    handle: FileHandle,
    offset: u64,
    len: u32,
) -> [u8; READ_REQUEST_BYTES] {
    let mut buf = [0u8; READ_REQUEST_BYTES];
    let header = FrameHeader::new(FrameKind::Request, request_id).encode_fixed();
    buf[..FrameHeader::ENCODED_LEN].copy_from_slice(&header);
    let mut pos = FrameHeader::ENCODED_LEN;

    // Op code(2) + handle(8) + offset(8) + requested length(4)
    buf[pos..pos + 2].copy_from_slice(&WireOp::Read.code().to_be_bytes());
    pos += 2;
    buf[pos..pos + 8].copy_from_slice(&handle.0.to_be_bytes());
    pos += 8;
    buf[pos..pos + 8].copy_from_slice(&offset.to_be_bytes());
    pos += 8;
    buf[pos..pos + 4].copy_from_slice(&len.to_be_bytes());
    pos += 4;
    debug_assert_eq!(pos, READ_REQUEST_BYTES);

    buf
}

/// The fixed response envelope that identifies the response request id and operation.
///
/// READ streaming clients decode this prefix first. If the operation is READ they can continue
/// with [`ReadResponsePrefix`] and stream the body directly into a caller buffer; otherwise they
/// materialize the remaining frame and fall back to ordinary [`decode_response`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseFrameHeader {
    pub(crate) request_id: u64,
    kind: ResponseFrameKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseFrameKind {
    Read,
    Other,
}

impl ResponseFrameHeader {
    pub(crate) const ENCODED_LEN: usize = FrameHeader::ENCODED_LEN + 2;

    pub(crate) fn decode(input: &[u8]) -> StorageResult<Self> {
        let mut cursor = input;
        let request_id = FrameHeader::decode_expecting(&mut cursor, FrameKind::Response)?;
        let op = WireOp::from_response_code(get_u16(&mut cursor)?)?;
        ensure_eof(&cursor)?;
        let kind = match op {
            WireOp::Read => ResponseFrameKind::Read,
            _ => ResponseFrameKind::Other,
        };
        Ok(Self { request_id, kind })
    }

    pub(crate) fn is_read(self) -> bool {
        self.kind == ResponseFrameKind::Read
    }
}

/// Decoded non-body portion of a READ response frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadResponsePrefix {
    pub(crate) eof: bool,
    pub(crate) data_len: usize,
}

impl ReadResponsePrefix {
    pub(crate) const TAIL_LEN: usize = READ_RESPONSE_PREFIX_BYTES - ResponseFrameHeader::ENCODED_LEN;

    pub(crate) fn decode_tail(header: ResponseFrameHeader, input: &[u8]) -> StorageResult<Self> {
        if !header.is_read() {
            return Err(StorageError::protocol("response frame is not a read response"));
        }
        let mut cursor = input;
        let eof = bool::decode(&mut cursor)?;
        let data_len = get_u32(&mut cursor)? as usize;
        ensure_eof(&cursor)?;
        if data_len > MAX_READ_RESPONSE_DATA_BYTES {
            return Err(StorageError::protocol(format!(
                "read response data length {data_len} exceeds maximum ({MAX_READ_RESPONSE_DATA_BYTES} bytes)"
            )));
        }
        Ok(Self { eof, data_len })
    }
}

/// Builds the fixed-size prefix that precedes an in-band READ response body.
///
/// The body is streamed separately by the connection writer, so the prefix (header + op + eof + data
/// length) is encoded without copying the body through the codec. Data length is bounded by
/// [`MAX_READ_RESPONSE_DATA_BYTES`].
///
/// Encodes directly into a stack array — no heap allocation.
pub(crate) fn encode_read_response_prefix(
    request_id: u64,
    eof: bool,
    data_len: usize,
) -> StorageResult<[u8; READ_RESPONSE_PREFIX_BYTES]> {
    if data_len > MAX_READ_RESPONSE_DATA_BYTES {
        return Err(StorageError::protocol(format!(
            "read response data length {data_len} exceeds maximum ({MAX_READ_RESPONSE_DATA_BYTES} bytes)"
        )));
    }
    let data_len = u32::try_from(data_len).map_err(|_| StorageError::protocol("read response data too large"))?;
    let mut buf = [0u8; READ_RESPONSE_PREFIX_BYTES];
    let header = FrameHeader::new(FrameKind::Response, request_id).encode_fixed();
    buf[..FrameHeader::ENCODED_LEN].copy_from_slice(&header);
    let mut pos = FrameHeader::ENCODED_LEN;

    // Op code(2) + eof(1) + data_len(4)
    buf[pos..pos + 2].copy_from_slice(&WireOp::Read.code().to_be_bytes());
    pos += 2;
    buf[pos] = u8::from(eof);
    pos += 1;
    buf[pos..pos + 4].copy_from_slice(&data_len.to_be_bytes());
    pos += 4;
    debug_assert_eq!(pos, READ_RESPONSE_PREFIX_BYTES);

    Ok(buf)
}

#[cfg(test)]
mod tests;
