//! Core [`WireEncode`] / [`WireDecode`] traits plus primitive impls.
//!
//! Each wire type implements these traits on itself, so the binary layout of a value lives next to
//! the type definition rather than in a central switch. Composition (optional/tagged fields) is
//! handled by blanket impls.

use bytes::{Buf, BufMut};

use crate::error::{StorageError, StorageResult};

use super::super::limits::MAX_STRING_FIELD_BYTES;

/// Serializes `self` into the wire format by appending bytes to `out`.
///
/// Returns `Err` only for impossible payloads (e.g. a byte field above [`u32::MAX`]); all framing
/// limits are enforced before reaching the wire.
pub(crate) trait WireEncode {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()>;
}

/// Deserializes `Self` from the wire format, consuming bytes from `input`.
pub(crate) trait WireDecode: Sized {
    fn decode(input: &mut impl Buf) -> StorageResult<Self>;
}

// ---- bounded integer reads ----------------------------------------------------------------------
//
// `Buf::get_*` panics on short input; our decoders must return protocol errors instead. The thin
// wrappers below are the only reason these exist.

pub(crate) fn get_u8(input: &mut impl Buf) -> StorageResult<u8> {
    ensure_remaining(input, 1)?;
    Ok(input.get_u8())
}

pub(crate) fn get_u16(input: &mut impl Buf) -> StorageResult<u16> {
    ensure_remaining(input, 2)?;
    Ok(input.get_u16())
}

pub(crate) fn get_u32(input: &mut impl Buf) -> StorageResult<u32> {
    ensure_remaining(input, 4)?;
    Ok(input.get_u32())
}

pub(crate) fn get_u64(input: &mut impl Buf) -> StorageResult<u64> {
    ensure_remaining(input, 8)?;
    Ok(input.get_u64())
}

fn ensure_remaining(input: &impl Buf, needed: usize) -> StorageResult<()> {
    if input.remaining() < needed {
        return Err(StorageError::protocol(format!(
            "short frame: need {needed} bytes, have {}",
            input.remaining()
        )));
    }
    Ok(())
}

pub(crate) fn ensure_eof(input: &impl Buf) -> StorageResult<()> {
    if input.has_remaining() {
        Err(StorageError::protocol("trailing bytes after frame payload"))
    } else {
        Ok(())
    }
}

// ---- byte / string primitives ------------------------------------------------------------------

/// Writes a length-prefixed byte field.
pub(crate) fn put_bytes(out: &mut impl BufMut, value: &[u8]) -> StorageResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| StorageError::protocol("byte field too large"))?;
    out.put_u32(len);
    out.put_slice(value);
    Ok(())
}

/// Reads a length-prefixed byte field, capping the allocation at `max_field_bytes`.
pub(crate) fn get_bytes(
    input: &mut impl Buf,
    max_field_bytes: usize,
) -> StorageResult<Vec<u8>> {
    let len = get_u32(input)? as usize;
    if len > input.remaining() {
        return Err(StorageError::protocol(format!(
            "byte field length {len} exceeds remaining payload ({} bytes)",
            input.remaining()
        )));
    }
    if len > max_field_bytes {
        return Err(StorageError::protocol(format!(
            "byte field length {len} exceeds maximum ({max_field_bytes} bytes)"
        )));
    }
    let mut data = vec![0_u8; len];
    input.copy_to_slice(&mut data);
    Ok(data)
}

// ---- trait impls for scalar types ---------------------------------------------------------------

impl WireEncode for bool {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u8(u8::from(*self));
        Ok(())
    }
}

impl WireDecode for bool {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(StorageError::protocol(format!(
                "invalid bool value {other}"
            ))),
        }
    }
}

impl WireEncode for String {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        put_bytes(out, self.as_bytes())
    }
}

impl WireDecode for String {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let bytes = get_bytes(input, MAX_STRING_FIELD_BYTES)?;
        String::from_utf8(bytes).map_err(|error| {
            StorageError::protocol_source("invalid utf-8 string field", error)
        })
    }
}

// `Option<T>` uses a 1-byte presence tag followed by the inner encoding if present.
impl<T: WireEncode> WireEncode for Option<T> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Some(value) => {
                out.put_u8(1);
                value.encode(out)
            }
            None => {
                out.put_u8(0);
                Ok(())
            }
        }
    }
}

impl<T: WireDecode> WireDecode for Option<T> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(input)?)),
            other => Err(StorageError::protocol(format!(
                "invalid optional tag {other}"
            ))),
        }
    }
}
