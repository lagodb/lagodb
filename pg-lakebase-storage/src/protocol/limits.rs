//! Protocol-level size bounds. The wire layer ([`crate::transport`]) reuses [`MAX_FRAME_BYTES`] for
//! framing, but the limits themselves are a property of the binary protocol and therefore live here.

/// Fixed frame header byte layout (magic `u32` + version `u16` + kind `u8` + request id `u64`).
pub(crate) const FRAME_HEADER_BYTES: usize = 4 + 2 + 1 + 8;

/// Upper bound for a single length-prefixed wire frame.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Defense-in-depth cap on path/error-message fields: hostile length prefixes cannot allocate
/// unbounded buffers even when individually under [`MAX_FRAME_BYTES`].
pub(crate) const MAX_STRING_FIELD_BYTES: usize = 1024 * 1024;

/// Bytes occupied by a READ request frame: frame header + op code + handle + offset + requested length.
pub(crate) const READ_REQUEST_BYTES: usize = FRAME_HEADER_BYTES + 2 + 8 + 8 + 4;

/// Bytes occupied by the non-payload portion of a READ response: frame header + op code + eof flag + data length.
pub const READ_RESPONSE_PREFIX_BYTES: usize = FRAME_HEADER_BYTES + 2 + 1 + 4;

/// Maximum in-band READ response body that still fits inside [`MAX_FRAME_BYTES`].
pub const MAX_READ_RESPONSE_DATA_BYTES: usize = MAX_FRAME_BYTES - READ_RESPONSE_PREFIX_BYTES;
