//! `WireEncode` / `WireDecode` for crate types reused inside wire frames.
//!
//! These types (`FileHandle`, `OpenFlags`, `SecretString`, store configs, errors) live outside the
//! protocol module so their wire layout is kept next to where it is used rather than on the types
//! themselves. Orphan rules permit this because every type is defined in this crate.

use bytes::{Buf, BufMut};

use crate::backend::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3Encryption,
    S3StoreConfig, SecretString, StoreConfig,
};
use crate::error::{StorageError, StorageErrorKind, StorageResult};
use crate::handle::{FileHandle, OpenFlags};
use crate::protocol::model::{ListCursor, WireListEntry};

use super::traits::{WireDecode, WireEncode, get_u8, get_u16, get_u32, get_u64};

impl WireEncode for FileHandle {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u64(self.0);
        Ok(())
    }
}

impl WireDecode for FileHandle {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(FileHandle(get_u64(input)?))
    }
}

impl WireEncode for OpenFlags {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let bits = u8::from(self.read);
        out.put_u8(bits);
        Ok(())
    }
}

impl WireDecode for OpenFlags {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let bits = get_u8(input)?;
        if bits & !0b1 != 0 {
            return Err(StorageError::protocol(format!(
                "unknown open flags 0x{bits:02x}"
            )));
        }
        Ok(OpenFlags {
            read: bits & 1 != 0,
        })
    }
}

// `SecretString` serializes like a plain string on the wire; the type exists to keep credentials
// from leaking in debug output, not to change the layout.
impl WireEncode for SecretString {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self.expose_secret().as_bytes())
    }
}

impl WireDecode for SecretString {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(SecretString::new(String::decode(input)?))
    }
}

// ---- store configs -----------------------------------------------------------------------------
//
// A 1-byte tag selects the variant so new configs can be added without touching callers that only
// handle existing variants.

impl WireEncode for S3Encryption {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::S3 => {
                out.put_u8(1);
                Ok(())
            }
            Self::Kms { key_id } => {
                out.put_u8(2);
                key_id.encode(out)
            }
            Self::Custom { key } => {
                out.put_u8(3);
                key.encode(out)
            }
        }
    }
}

impl WireDecode for S3Encryption {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            1 => Ok(Self::S3),
            2 => Ok(Self::Kms {
                key_id: WireDecode::decode(input)?,
            }),
            3 => Ok(Self::Custom {
                key: WireDecode::decode(input)?,
            }),
            other => Err(StorageError::protocol(format!(
                "unknown S3 encryption type {other}"
            ))),
        }
    }
}

impl WireEncode for StoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::S3(config) => {
                out.put_u8(1);
                config.encode(out)
            }
            Self::S3Compatible(config) => {
                out.put_u8(2);
                config.encode(out)
            }
            Self::Gcs(config) => {
                out.put_u8(3);
                config.encode(out)
            }
            Self::Azure(config) => {
                out.put_u8(4);
                config.encode(out)
            }
        }
    }
}

impl WireDecode for StoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            1 => Ok(Self::S3(S3StoreConfig::decode(input)?)),
            2 => Ok(Self::S3Compatible(S3CompatibleStoreConfig::decode(input)?)),
            3 => Ok(Self::Gcs(GcsStoreConfig::decode(input)?)),
            4 => Ok(Self::Azure(AzureStoreConfig::decode(input)?)),
            other => Err(StorageError::protocol(format!(
                "unknown store config type {other}"
            ))),
        }
    }
}

impl WireEncode for S3StoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.region.encode(out)?;
        self.endpoint.encode(out)?;
        self.access_key_id.encode(out)?;
        self.secret_access_key.encode(out)?;
        self.token.encode(out)?;
        self.allow_http.encode(out)?;
        self.virtual_hosted_style_request.encode(out)?;
        self.skip_signature.encode(out)?;
        self.encryption.encode(out)
    }
}

impl WireDecode for S3StoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            region: WireDecode::decode(input)?,
            endpoint: WireDecode::decode(input)?,
            access_key_id: WireDecode::decode(input)?,
            secret_access_key: WireDecode::decode(input)?,
            token: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            virtual_hosted_style_request: WireDecode::decode(input)?,
            skip_signature: WireDecode::decode(input)?,
            encryption: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for S3CompatibleStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.endpoint.encode(out)?;
        self.region.encode(out)?;
        self.access_key_id.encode(out)?;
        self.secret_access_key.encode(out)?;
        self.token.encode(out)?;
        self.allow_http.encode(out)?;
        self.virtual_hosted_style_request.encode(out)?;
        self.skip_signature.encode(out)?;
        self.encryption.encode(out)
    }
}

impl WireDecode for S3CompatibleStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            endpoint: WireDecode::decode(input)?,
            region: WireDecode::decode(input)?,
            access_key_id: WireDecode::decode(input)?,
            secret_access_key: WireDecode::decode(input)?,
            token: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            virtual_hosted_style_request: WireDecode::decode(input)?,
            skip_signature: WireDecode::decode(input)?,
            encryption: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for GcsStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.base_url.encode(out)?;
        self.service_account_path.encode(out)?;
        self.service_account_key.encode(out)?;
        self.application_credentials_path.encode(out)?;
        self.bearer_token.encode(out)?;
        self.skip_signature.encode(out)
    }
}

impl WireDecode for GcsStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            base_url: WireDecode::decode(input)?,
            service_account_path: WireDecode::decode(input)?,
            service_account_key: WireDecode::decode(input)?,
            application_credentials_path: WireDecode::decode(input)?,
            bearer_token: WireDecode::decode(input)?,
            skip_signature: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for AzureStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.account.encode(out)?;
        self.endpoint.encode(out)?;
        self.access_key.encode(out)?;
        self.bearer_token.encode(out)?;
        self.sas_token.encode(out)?;
        self.client_id.encode(out)?;
        self.client_secret.encode(out)?;
        self.tenant_id.encode(out)?;
        self.authority_host.encode(out)?;
        self.allow_http.encode(out)?;
        self.use_emulator.encode(out)
    }
}

impl WireDecode for AzureStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            account: WireDecode::decode(input)?,
            endpoint: WireDecode::decode(input)?,
            access_key: WireDecode::decode(input)?,
            bearer_token: WireDecode::decode(input)?,
            sas_token: WireDecode::decode(input)?,
            client_id: WireDecode::decode(input)?,
            client_secret: WireDecode::decode(input)?,
            tenant_id: WireDecode::decode(input)?,
            authority_host: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            use_emulator: WireDecode::decode(input)?,
        })
    }
}

// ---- error kind --------------------------------------------------------------------------------

impl WireEncode for StorageErrorKind {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u16(self.code());
        Ok(())
    }
}

impl WireDecode for StorageErrorKind {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let code = get_u16(input)?;
        StorageErrorKind::from_code(code).ok_or_else(|| {
            StorageError::protocol(format!("unknown error code {code}"))
        })
    }
}

// ---- Vec<u8> for free-form byte payloads -------------------------------------------------------

impl WireEncode for Vec<u8> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self)
    }
}

impl WireDecode for Vec<u8> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        // Byte fields use the full frame budget as their cap; they are bounded by the outer frame
        // size enforced in [`crate::transport`].
        super::traits::get_bytes(input, super::super::limits::MAX_FRAME_BYTES)
    }
}

// ---- Vec<String> for bounded bulk-delete requests ---------------------------------------------

impl WireEncode for Vec<String> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let limit = super::super::limits::MAX_BULK_DELETE_OBJECT_KEYS;
        if self.len() > limit {
            return Err(StorageError::resource_exhausted(format!(
                "bulk-delete request exceeds {limit} keys"
            )));
        }
        out.put_u32(self.len() as u32);
        for value in self {
            value.encode(out)?;
        }
        Ok(())
    }
}

impl WireDecode for Vec<String> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let len = get_u32(input)? as usize;
        let limit = super::super::limits::MAX_BULK_DELETE_OBJECT_KEYS;
        if len > limit {
            return Err(StorageError::resource_exhausted(format!(
                "bulk-delete request exceeds {limit} keys"
            )));
        }
        // Every encoded string contains at least its four-byte length prefix.
        if len > input.remaining() / std::mem::size_of::<u32>() {
            return Err(StorageError::protocol(format!(
                "bulk-delete request declares {len} keys but only {} bytes remain",
                input.remaining()
            )));
        }
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(String::decode(input)?);
        }
        Ok(values)
    }
}

// ---- u32 / u64 scalar types --------------------------------------------------------------------

impl WireEncode for u32 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u32(*self);
        Ok(())
    }
}

impl WireDecode for u32 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        get_u32(input)
    }
}

impl WireEncode for u64 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u64(*self);
        Ok(())
    }
}

impl WireDecode for u64 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        get_u64(input)
    }
}

impl WireEncode for i64 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_i64(*self);
        Ok(())
    }
}

impl WireDecode for i64 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        if input.remaining() < std::mem::size_of::<i64>() {
            return Err(StorageError::protocol("truncated i64 field"));
        }
        Ok(input.get_i64())
    }
}

// ---- list cursor / entry -----------------------------------------------------------------------
//
// `ListCursor` is a length-prefixed string newtype (server-issued opaque token).
// `WireListEntry` is encoded as `(key, size, etag, last_modified_ms)`.
// bucket/key carrying message in this codec.

impl WireEncode for ListCursor {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self.as_str().as_bytes())
    }
}

impl WireDecode for ListCursor {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self::from_wire(String::decode(input)?))
    }
}

impl WireEncode for WireListEntry {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.key.encode(out)?;
        self.size.encode(out)?;
        self.etag.encode(out)?;
        self.last_modified_ms.encode(out)
    }
}

impl WireDecode for WireListEntry {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            key: WireDecode::decode(input)?,
            size: WireDecode::decode(input)?,
            etag: WireDecode::decode(input)?,
            last_modified_ms: WireDecode::decode(input)?,
        })
    }
}

// ---- Vec<WireListEntry> ------------------------------------------------------------------------
//
// `Vec<u8>` already has a wire impl above; for `Vec<WireListEntry>` (the only other vec-of-T on
// the wire today) we use a length prefix + concatenated element encodings. The cap is the same
// outer-frame budget so a single page cannot exceed the framing limit.

impl WireEncode for Vec<WireListEntry> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let len = u32::try_from(self.len()).map_err(|_| {
            StorageError::protocol("list response too large to encode")
        })?;
        out.put_u32(len);
        for entry in self {
            entry.encode(out)?;
        }
        Ok(())
    }
}

impl WireDecode for Vec<WireListEntry> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let len = get_u32(input)? as usize;
        // Guard against a corrupted frame asking us to allocate a huge vec; one entry is at
        // least 1 byte (length-prefixed empty key + size + etag tag), so an entry count above the
        // remaining frame size is necessarily a malformed frame.
        if len > input.remaining() {
            return Err(StorageError::protocol(format!(
                "list response declares {len} entries but only {} bytes remain",
                input.remaining()
            )));
        }
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(WireListEntry::decode(input)?);
        }
        Ok(entries)
    }
}
