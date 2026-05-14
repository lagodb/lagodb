use crate::error::{StorageError, StorageResult};

pub(crate) const MAGIC: u32 = 0x53544731; // STG1
pub(crate) const VERSION: u16 = 3;
pub(crate) const KIND_REQUEST: u8 = 1;
pub(crate) const KIND_RESPONSE: u8 = 2;

/// Stable opcode embedded after the frame header; responses reuse request codes plus [`WireOp::Error`].
///
/// Staging has a dedicated `StageCreate` opcode. `Commit` and `Abort` do **not** carry a server-side
/// file handle: their requests carry `(store_id, bucket, key)` so they can be issued from a
/// different connection than the `StageCreate` that produced the staging file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WireOp {
    Open,
    Read,
    Close,
    StageCreate,
    Commit,
    Abort,
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
            Self::StageCreate => 4,
            Self::Commit => 5,
            Self::Abort => 6,
            Self::RegisterStore => 7,
            Self::UnregisterStore => 8,
            Self::PurgeStoreCache => 9,
            Self::InvalidateObjectCache => 10,
            Self::Delete => 11,
            Self::DeletePrefix => 12,
            Self::List => 13,
            Self::Head => 14,
            Self::Error => 1000,
        }
    }

    pub(crate) fn from_request_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::Open),
            2 => Ok(Self::Read),
            3 => Ok(Self::Close),
            4 => Ok(Self::StageCreate),
            5 => Ok(Self::Commit),
            6 => Ok(Self::Abort),
            7 => Ok(Self::RegisterStore),
            8 => Ok(Self::UnregisterStore),
            9 => Ok(Self::PurgeStoreCache),
            10 => Ok(Self::InvalidateObjectCache),
            11 => Ok(Self::Delete),
            12 => Ok(Self::DeletePrefix),
            13 => Ok(Self::List),
            14 => Ok(Self::Head),
            _ => Err(StorageError::protocol(format!("unknown request op {code}"))),
        }
    }

    pub(crate) fn from_response_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::Open),
            2 => Ok(Self::Read),
            3 => Ok(Self::Close),
            4 => Ok(Self::StageCreate),
            5 => Ok(Self::Commit),
            6 => Ok(Self::Abort),
            7 => Ok(Self::RegisterStore),
            8 => Ok(Self::UnregisterStore),
            9 => Ok(Self::PurgeStoreCache),
            10 => Ok(Self::InvalidateObjectCache),
            11 => Ok(Self::Delete),
            12 => Ok(Self::DeletePrefix),
            13 => Ok(Self::List),
            14 => Ok(Self::Head),
            1000 => Ok(Self::Error),
            _ => Err(StorageError::protocol(format!(
                "unknown response op {code}"
            ))),
        }
    }
}
