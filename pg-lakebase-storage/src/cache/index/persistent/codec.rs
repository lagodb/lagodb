//! [`crate::cache::CachedObjectMeta`] serialization for persistent cache-index meta rows.

use std::io::{Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::cache::meta::{
    CacheState, CachedObjectMeta, CachedResidency, ObjectIdentity,
};
use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;

pub(super) fn encode_meta(meta: &CachedObjectMeta) -> Vec<u8> {
    let mut out = Vec::new();
    write_string(&mut out, meta.key().store_id().as_str());
    write_string(&mut out, meta.key().bucket());
    write_string(&mut out, meta.key().key());
    out.write_u64::<BigEndian>(meta.size())
        .expect("infallible write to Vec");
    match meta.etag() {
        Some(etag) => {
            out.write_u8(1).expect("infallible write to Vec");
            write_string(&mut out, etag);
        }
        None => {
            out.write_u8(0).expect("infallible write to Vec");
        }
    }
    out.write_u8(meta.cache_state().as_u8())
        .expect("infallible write to Vec");
    out.write_u64::<BigEndian>(meta.last_access_ns)
        .expect("infallible write to Vec");
    out.write_u64::<BigEndian>(meta.generation)
        .expect("infallible write to Vec");
    match meta.residency() {
        CachedResidency::Small { bytes } => out
            .write_u64::<BigEndian>(*bytes)
            .expect("infallible write to Vec"),
        CachedResidency::Complete => {}
    }
    out
}

pub(super) fn decode_meta(bytes: &[u8]) -> StorageResult<CachedObjectMeta> {
    let mut input = Cursor::new(bytes);
    let store_id = read_string(&mut input)?;
    let bucket = read_string(&mut input)?;
    let key = read_string(&mut input)?;
    let size = input.read_u64::<BigEndian>()?;
    let etag = match input.read_u8()? {
        0 => None,
        1 => Some(read_string(&mut input)?),
        other => {
            return Err(StorageError::cache(format!("invalid etag tag {other}")));
        }
    };
    let cache_state = CacheState::from_u8(input.read_u8()?)?;
    let last_access_ns = input.read_u64::<BigEndian>()?;
    let generation = input.read_u64::<BigEndian>()?;
    let residency = match cache_state {
        CacheState::SmallKv => CachedResidency::Small {
            bytes: input.read_u64::<BigEndian>()?,
        },
        CacheState::CompleteFile => CachedResidency::Complete,
    };
    let meta = CachedObjectMeta::from_residency(
        ObjectIdentity {
            key: ObjectLocation::new(store_id, bucket, key)?,
            size,
            etag,
        },
        residency,
        last_access_ns,
        generation,
    );
    if input.position() as usize != bytes.len() {
        return Err(StorageError::cache("trailing bytes in cache metadata"));
    }
    Ok(meta)
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    out.write_u32::<BigEndian>(value.len() as u32)
        .expect("infallible write to Vec");
    out.extend_from_slice(value.as_bytes());
}

fn read_string(input: &mut Cursor<&[u8]>) -> StorageResult<String> {
    let len = input.read_u32::<BigEndian>()? as usize;
    ensure_remaining(input, len, "string field length")?;
    let mut bytes = vec![0; len];
    input.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| {
        StorageError::cache_source("invalid utf-8 string in cache metadata", error)
    })
}

fn ensure_remaining(
    input: &Cursor<&[u8]>,
    wanted: usize,
    field: &'static str,
) -> StorageResult<()> {
    let total = input.get_ref().len();
    let position = usize::try_from(input.position()).map_err(|_| {
        StorageError::cache("cache metadata decode cursor position overflow")
    })?;
    if position > total {
        return Err(StorageError::cache(
            "cache metadata decode cursor past end of payload",
        ));
    }
    let remaining = total - position;
    if wanted > remaining {
        return Err(StorageError::cache(format!(
            "{field} {wanted} exceeds remaining cache metadata payload ({remaining} bytes)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, WriteBytesExt};

    use super::*;

    #[test]
    fn decode_meta_rejects_string_length_claim_beyond_payload() {
        let mut bytes = Vec::new();
        bytes.write_u32::<BigEndian>(u32::MAX).unwrap();

        let error = decode_meta(&bytes).unwrap_err();

        assert!(matches!(error, StorageError::Cache { .. }));
        assert!(error.wire_message().contains("string field length"));
    }

    #[test]
    fn decode_meta_rejects_trailing_complete_payload() {
        let mut bytes = Vec::new();
        write_string(&mut bytes, "store-a");
        write_string(&mut bytes, "bucket");
        write_string(&mut bytes, "file");
        bytes.write_u64::<BigEndian>(8).unwrap();
        bytes.write_u8(0).unwrap();
        bytes.write_u8(CacheState::CompleteFile.as_u8()).unwrap();
        bytes.write_u64::<BigEndian>(0).unwrap();
        bytes.write_u64::<BigEndian>(0).unwrap();
        bytes.write_u8(1).unwrap();

        let error = decode_meta(&bytes).unwrap_err();

        assert!(matches!(error, StorageError::Cache { .. }));
        assert!(error.wire_message().contains("trailing bytes"));
    }
}
