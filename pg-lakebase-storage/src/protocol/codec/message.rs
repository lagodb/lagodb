//! `WireEncode` / `WireDecode` for the top-level request and response envelopes.
//!
//! Each `{payload}.encode()` writes the op tag and variant body; `{payload}::decode()` reads the tag
//! and dispatches to the matching variant. This keeps the binary layout next to each variant's data.

use bytes::{Buf, BufMut};

use crate::error::StorageResult;

use super::super::model::{
    WireRequest, WireRequestPayload, WireResponse, WireResponsePayload,
};
use super::super::op::WireOp;
use super::header::{FrameHeader, FrameKind};
use super::traits::{WireDecode, WireEncode, ensure_eof, get_u16};

// ---- request ------------------------------------------------------------------------------------

impl WireEncode for WireRequestPayload {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::Open {
                store_id,
                bucket,
                key,
                flags,
            } => {
                out.put_u16(WireOp::Open.code());
                flags.encode(out)?;
                store_id.encode(out)?;
                bucket.encode(out)?;
                key.encode(out)
            }
            Self::Head {
                store_id,
                bucket,
                key,
            } => {
                out.put_u16(WireOp::Head.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                key.encode(out)
            }
            Self::Read {
                handle,
                offset,
                len,
            } => {
                out.put_u16(WireOp::Read.code());
                handle.encode(out)?;
                offset.encode(out)?;
                len.encode(out)
            }
            Self::Close { handle } => {
                out.put_u16(WireOp::Close.code());
                handle.encode(out)
            }
            Self::Upload {
                store_id,
                bucket,
                key,
            } => {
                out.put_u16(WireOp::Upload.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                key.encode(out)
            }
            Self::RegisterStore { store_id, config } => {
                out.put_u16(WireOp::RegisterStore.code());
                store_id.encode(out)?;
                config.encode(out)
            }
            Self::UnregisterStore { store_id } => {
                out.put_u16(WireOp::UnregisterStore.code());
                store_id.encode(out)
            }
            Self::PurgeStoreCache { store_id } => {
                out.put_u16(WireOp::PurgeStoreCache.code());
                store_id.encode(out)
            }
            Self::ProbeStore {
                store_id,
                bucket,
                root_prefix,
            } => {
                out.put_u16(WireOp::ProbeStore.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                root_prefix.encode(out)
            }
            Self::InvalidateObjectCache {
                store_id,
                bucket,
                key,
            } => {
                out.put_u16(WireOp::InvalidateObjectCache.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                key.encode(out)
            }
            Self::Delete {
                store_id,
                bucket,
                key,
            } => {
                out.put_u16(WireOp::Delete.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                key.encode(out)
            }
            Self::DeletePrefix {
                store_id,
                bucket,
                prefix,
            } => {
                out.put_u16(WireOp::DeletePrefix.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                prefix.encode(out)
            }
            Self::DeleteObjects {
                store_id,
                bucket,
                keys,
            } => {
                out.put_u16(WireOp::DeleteObjects.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                keys.encode(out)
            }
            Self::List {
                store_id,
                bucket,
                prefix,
                page_size,
                cursor,
            } => {
                out.put_u16(WireOp::List.code());
                store_id.encode(out)?;
                bucket.encode(out)?;
                prefix.encode(out)?;
                page_size.encode(out)?;
                cursor.encode(out)
            }
            Self::CloseList { cursor } => {
                out.put_u16(WireOp::CloseList.code());
                cursor.encode(out)
            }
        }
    }
}

impl WireDecode for WireRequestPayload {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let op = WireOp::from_request_code(get_u16(input)?)?;
        Ok(match op {
            WireOp::Open => Self::Open {
                flags: WireDecode::decode(input)?,
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                key: WireDecode::decode(input)?,
            },
            WireOp::Head => Self::Head {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                key: WireDecode::decode(input)?,
            },
            WireOp::Read => Self::Read {
                handle: WireDecode::decode(input)?,
                offset: WireDecode::decode(input)?,
                len: WireDecode::decode(input)?,
            },
            WireOp::Close => Self::Close {
                handle: WireDecode::decode(input)?,
            },
            WireOp::Upload => Self::Upload {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                key: WireDecode::decode(input)?,
            },
            WireOp::RegisterStore => Self::RegisterStore {
                store_id: WireDecode::decode(input)?,
                config: WireDecode::decode(input)?,
            },
            WireOp::UnregisterStore => Self::UnregisterStore {
                store_id: WireDecode::decode(input)?,
            },
            WireOp::PurgeStoreCache => Self::PurgeStoreCache {
                store_id: WireDecode::decode(input)?,
            },
            WireOp::ProbeStore => Self::ProbeStore {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                root_prefix: WireDecode::decode(input)?,
            },
            WireOp::InvalidateObjectCache => Self::InvalidateObjectCache {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                key: WireDecode::decode(input)?,
            },
            WireOp::Delete => Self::Delete {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                key: WireDecode::decode(input)?,
            },
            WireOp::DeletePrefix => Self::DeletePrefix {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                prefix: WireDecode::decode(input)?,
            },
            WireOp::DeleteObjects => Self::DeleteObjects {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                keys: WireDecode::decode(input)?,
            },
            WireOp::List => Self::List {
                store_id: WireDecode::decode(input)?,
                bucket: WireDecode::decode(input)?,
                prefix: WireDecode::decode(input)?,
                page_size: WireDecode::decode(input)?,
                cursor: WireDecode::decode(input)?,
            },
            WireOp::CloseList => Self::CloseList {
                cursor: WireDecode::decode(input)?,
            },
            WireOp::Error => unreachable!("error op is not valid in requests"),
        })
    }
}

impl WireEncode for WireRequest {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        FrameHeader::new(FrameKind::Request, self.request_id).encode(out);
        self.payload.encode(out)
    }
}

impl WireDecode for WireRequest {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let request_id = FrameHeader::decode_expecting(input, FrameKind::Request)?;
        let payload = WireRequestPayload::decode(input)?;
        ensure_eof(input)?;
        Ok(Self {
            request_id,
            payload,
        })
    }
}

// ---- response -----------------------------------------------------------------------------------

impl WireEncode for WireResponsePayload {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::Open {
                handle,
                size,
                direct_io,
            } => {
                out.put_u16(WireOp::Open.code());
                handle.encode(out)?;
                size.encode(out)?;
                direct_io.encode(out)
            }
            Self::Head { size, etag } => {
                out.put_u16(WireOp::Head.code());
                size.encode(out)?;
                etag.encode(out)
            }
            Self::Read { data, eof } => {
                out.put_u16(WireOp::Read.code());
                eof.encode(out)?;
                data.encode(out)
            }
            Self::Close => {
                out.put_u16(WireOp::Close.code());
                Ok(())
            }
            Self::Upload { size, etag } => {
                out.put_u16(WireOp::Upload.code());
                size.encode(out)?;
                etag.encode(out)
            }
            Self::RegisterStore { replaced } => {
                out.put_u16(WireOp::RegisterStore.code());
                replaced.encode(out)
            }
            Self::UnregisterStore { removed } => {
                out.put_u16(WireOp::UnregisterStore.code());
                removed.encode(out)
            }
            Self::PurgeStoreCache => {
                out.put_u16(WireOp::PurgeStoreCache.code());
                Ok(())
            }
            Self::ProbeStore { result } => {
                out.put_u16(WireOp::ProbeStore.code());
                result.list_succeeded().encode(out)?;
                result.write_succeeded().encode(out)?;
                result.read_succeeded().encode(out)?;
                result.delete_succeeded().encode(out)?;
                result.error().map(str::to_owned).encode(out)
            }
            Self::InvalidateObjectCache { removed } => {
                out.put_u16(WireOp::InvalidateObjectCache.code());
                removed.encode(out)
            }
            Self::Delete => {
                out.put_u16(WireOp::Delete.code());
                Ok(())
            }
            Self::DeletePrefix { deleted } => {
                out.put_u16(WireOp::DeletePrefix.code());
                deleted.encode(out)
            }
            Self::DeleteObjects { deleted } => {
                out.put_u16(WireOp::DeleteObjects.code());
                deleted.encode(out)
            }
            Self::List {
                entries,
                next_cursor,
            } => {
                out.put_u16(WireOp::List.code());
                entries.encode(out)?;
                next_cursor.encode(out)
            }
            Self::CloseList => {
                out.put_u16(WireOp::CloseList.code());
                Ok(())
            }
            Self::Error { kind, message } => {
                out.put_u16(WireOp::Error.code());
                kind.encode(out)?;
                message.encode(out)
            }
        }
    }
}

impl WireDecode for WireResponsePayload {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let op = WireOp::from_response_code(get_u16(input)?)?;
        Ok(match op {
            WireOp::Open => Self::Open {
                handle: WireDecode::decode(input)?,
                size: WireDecode::decode(input)?,
                direct_io: WireDecode::decode(input)?,
            },
            WireOp::Head => Self::Head {
                size: WireDecode::decode(input)?,
                etag: WireDecode::decode(input)?,
            },
            WireOp::Read => Self::Read {
                eof: WireDecode::decode(input)?,
                data: WireDecode::decode(input)?,
            },
            WireOp::Close => Self::Close,
            WireOp::Upload => Self::Upload {
                size: WireDecode::decode(input)?,
                etag: WireDecode::decode(input)?,
            },
            WireOp::RegisterStore => Self::RegisterStore {
                replaced: WireDecode::decode(input)?,
            },
            WireOp::UnregisterStore => Self::UnregisterStore {
                removed: WireDecode::decode(input)?,
            },
            WireOp::PurgeStoreCache => Self::PurgeStoreCache,
            WireOp::ProbeStore => Self::ProbeStore {
                result: crate::backend::StorageProbeResult::from_wire(
                    WireDecode::decode(input)?,
                    WireDecode::decode(input)?,
                    WireDecode::decode(input)?,
                    WireDecode::decode(input)?,
                    WireDecode::decode(input)?,
                ),
            },
            WireOp::InvalidateObjectCache => Self::InvalidateObjectCache {
                removed: WireDecode::decode(input)?,
            },
            WireOp::Delete => Self::Delete,
            WireOp::DeletePrefix => Self::DeletePrefix {
                deleted: WireDecode::decode(input)?,
            },
            WireOp::DeleteObjects => Self::DeleteObjects {
                deleted: WireDecode::decode(input)?,
            },
            WireOp::List => Self::List {
                entries: WireDecode::decode(input)?,
                next_cursor: WireDecode::decode(input)?,
            },
            WireOp::CloseList => Self::CloseList,
            WireOp::Error => Self::Error {
                kind: WireDecode::decode(input)?,
                message: WireDecode::decode(input)?,
            },
        })
    }
}

impl WireEncode for WireResponse {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        FrameHeader::new(FrameKind::Response, self.request_id).encode(out);
        self.payload.encode(out)
    }
}

impl WireDecode for WireResponse {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let request_id = FrameHeader::decode_expecting(input, FrameKind::Response)?;
        let payload = WireResponsePayload::decode(input)?;
        ensure_eof(input)?;
        Ok(Self {
            request_id,
            payload,
        })
    }
}
