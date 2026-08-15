//! Length-delimited wire payloads (`WireRequest` / `WireResponse`) and their codecs.
//!
//! The binary layout for every wire type is owned by that type's `WireEncode` / `WireDecode` impls
//! in [`codec`]; framing (length prefix, FD side channel) lives in [`crate::transport`] and
//! multiplexing in [`crate::connection`].

mod codec;
pub(crate) mod limits;
mod model;
mod op;

pub(crate) use codec::{
    ReadResponsePrefix, ResponseFrameHeader, encode_read_request,
    encode_read_response_prefix,
};
pub(crate) use codec::{
    decode_request, decode_response, encode_request, encode_response,
};
pub use limits::{
    MAX_BULK_DELETE_OBJECT_KEYS, MAX_FRAME_BYTES, MAX_LIST_PAGE_SIZE,
    MAX_READ_RESPONSE_DATA_BYTES,
};
pub(crate) use model::{
    ListCursor, WireListEntry, WireRequest, WireRequestPayload, WireResponse,
    WireResponsePayload,
};
