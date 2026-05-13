//! Fixed frame header: magic / version / kind / request id.

use bytes::{Buf, BufMut};

use crate::error::{StorageError, StorageResult};

use super::super::limits::FRAME_HEADER_BYTES;
use super::super::op::{KIND_REQUEST, KIND_RESPONSE, MAGIC, VERSION};
use super::traits::{get_u16, get_u32, get_u64};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameKind {
    Request,
    Response,
}

impl FrameKind {
    const fn code(self) -> u8 {
        match self {
            Self::Request => KIND_REQUEST,
            Self::Response => KIND_RESPONSE,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
    pub(crate) kind: FrameKind,
    pub(crate) request_id: u64,
}

impl FrameHeader {
    pub(crate) const ENCODED_LEN: usize = FRAME_HEADER_BYTES;

    pub(crate) fn new(kind: FrameKind, request_id: u64) -> Self {
        Self { kind, request_id }
    }

    pub(crate) fn encode(self, out: &mut impl BufMut) {
        out.put_slice(&self.encode_fixed());
    }

    pub(crate) fn encode_fixed(self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = [0u8; Self::ENCODED_LEN];
        let mut pos = 0;

        buf[pos..pos + 4].copy_from_slice(&MAGIC.to_be_bytes());
        pos += 4;
        buf[pos..pos + 2].copy_from_slice(&VERSION.to_be_bytes());
        pos += 2;
        buf[pos] = self.kind.code();
        pos += 1;
        buf[pos..pos + 8].copy_from_slice(&self.request_id.to_be_bytes());
        pos += 8;
        debug_assert_eq!(pos, Self::ENCODED_LEN);

        buf
    }

    pub(crate) fn decode_expecting(input: &mut impl Buf, expected: FrameKind) -> StorageResult<u64> {
        let magic = get_u32(input)?;
        if magic != MAGIC {
            return Err(StorageError::protocol(format!("bad magic 0x{magic:08x}")));
        }
        let version = get_u16(input)?;
        if version != VERSION {
            return Err(StorageError::protocol(format!("unsupported protocol version {version}")));
        }
        let kind = super::traits::get_u8(input)?;
        if kind != expected.code() {
            return Err(StorageError::protocol(format!("unexpected frame kind {kind}")));
        }
        get_u64(input)
    }
}
