//! Length-delimited wire payloads (`WireRequest` / `WireResponse`) and their codecs.
//!
//! The binary layout for every wire type is owned by that type's `WireEncode` / `WireDecode` impls
//! in [`codec`]; framing (length prefix, FD side channel) lives in [`crate::transport`] and
//! multiplexing in [`crate::connection`].

mod codec;
pub(crate) mod limits;
mod model;
mod op;

pub use codec::{decode_request, decode_response, encode_request, encode_response};
pub(crate) use codec::{encode_read_request, encode_read_response_prefix, ReadResponsePrefix, ResponseFrameHeader};
pub use limits::{MAX_FRAME_BYTES, MAX_READ_RESPONSE_DATA_BYTES};
pub use model::{ListCursor, WireListEntry, WireRequest, WireRequestPayload, WireResponse, WireResponsePayload};
