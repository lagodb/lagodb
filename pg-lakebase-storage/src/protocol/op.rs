use crate::error::{StorageError, StorageResult};

pub(crate) const MAGIC: u32 = 0x53544731; // STG1
pub(crate) const VERSION: u16 = 3;
pub(crate) const KIND_REQUEST: u8 = 1;
pub(crate) const KIND_RESPONSE: u8 = 2;

/// Stable opcode embedded after the frame header; responses reuse request codes plus [`WireOp::Error`].
///
/// Staging is not represented on the wire. The database (caller) creates the staging file itself
/// through the filesystem (using [`crate::staging::StagingPathResolver`] to derive the path),
/// and `Upload` is the only staging-related verb — its request carries `(store_id, bucket, key)`
/// so it can be issued from a different connection than the one that wrote the bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WireOp {
    Open,
    Read,
    Close,
    Upload,
    RegisterStore,
    UnregisterStore,
    PurgeStoreCache,
    InvalidateObjectCache,
    Delete,
    DeletePrefix,
    List,
    Head,
    Error,
}

impl WireOp {
    pub(crate) const fn code(self) -> u16 {
        match self {
            Self::Open => 1,
            Self::Read => 2,
            Self::Close => 3,
            Self::Upload => 4,
            Self::RegisterStore => 5,
            Self::UnregisterStore => 6,
            Self::PurgeStoreCache => 7,
            Self::InvalidateObjectCache => 8,
            Self::Delete => 9,
            Self::DeletePrefix => 10,
            Self::List => 11,
            Self::Head => 12,
            Self::Error => 1000,
        }
    }

    pub(crate) fn from_request_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::Open),
            2 => Ok(Self::Read),
            3 => Ok(Self::Close),
            4 => Ok(Self::Upload),
            5 => Ok(Self::RegisterStore),
            6 => Ok(Self::UnregisterStore),
            7 => Ok(Self::PurgeStoreCache),
            8 => Ok(Self::InvalidateObjectCache),
            9 => Ok(Self::Delete),
            10 => Ok(Self::DeletePrefix),
            11 => Ok(Self::List),
            12 => Ok(Self::Head),
            _ => Err(StorageError::protocol(format!("unknown request op {code}"))),
        }
    }

    pub(crate) fn from_response_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::Open),
            2 => Ok(Self::Read),
            3 => Ok(Self::Close),
            4 => Ok(Self::Upload),
            5 => Ok(Self::RegisterStore),
            6 => Ok(Self::UnregisterStore),
            7 => Ok(Self::PurgeStoreCache),
            8 => Ok(Self::InvalidateObjectCache),
            9 => Ok(Self::Delete),
            10 => Ok(Self::DeletePrefix),
            11 => Ok(Self::List),
            12 => Ok(Self::Head),
            1000 => Ok(Self::Error),
            _ => Err(StorageError::protocol(format!(
                "unknown response op {code}"
            ))),
        }
    }
}
