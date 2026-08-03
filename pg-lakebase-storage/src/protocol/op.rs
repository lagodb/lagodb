use crate::error::{StorageError, StorageResult};

pub(crate) const MAGIC: u32 = 0x53544731; // STG1
pub(crate) const VERSION: u16 = 8;
pub(crate) const KIND_REQUEST: u8 = 1;
pub(crate) const KIND_RESPONSE: u8 = 2;

/// Stable opcode embedded after the frame header. Normal operation responses
/// reuse request codes; both attach request forms complete with [`WireOp::Ready`].
///
/// Staging is not represented on the wire. The database (caller) creates the staging file itself
/// through the filesystem (using [`crate::staging::StagingPathResolver`] to derive the path),
/// and `Upload` is the only staging-related verb.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WireOp {
    AttachManaged,
    AttachConfigured,
    Open,
    Read,
    Close,
    Upload,
    InvalidateObjectCache,
    Delete,
    DeletePrefix,
    List,
    Head,
    DeleteObjects,
    CloseList,
    ProbeStore,
    Ready,
    Error,
}

impl WireOp {
    pub(crate) const fn code(self) -> u16 {
        match self {
            Self::AttachManaged => 1,
            Self::AttachConfigured => 2,
            Self::Open => 3,
            Self::Read => 4,
            Self::Close => 5,
            Self::Upload => 6,
            Self::InvalidateObjectCache => 7,
            Self::Delete => 8,
            Self::DeletePrefix => 9,
            Self::List => 10,
            Self::Head => 11,
            Self::DeleteObjects => 12,
            Self::CloseList => 13,
            Self::ProbeStore => 14,
            Self::Ready => 15,
            Self::Error => 1000,
        }
    }

    pub(crate) fn from_request_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::AttachManaged),
            2 => Ok(Self::AttachConfigured),
            3 => Ok(Self::Open),
            4 => Ok(Self::Read),
            5 => Ok(Self::Close),
            6 => Ok(Self::Upload),
            7 => Ok(Self::InvalidateObjectCache),
            8 => Ok(Self::Delete),
            9 => Ok(Self::DeletePrefix),
            10 => Ok(Self::List),
            11 => Ok(Self::Head),
            12 => Ok(Self::DeleteObjects),
            13 => Ok(Self::CloseList),
            14 => Ok(Self::ProbeStore),
            15 => Err(StorageError::protocol("ready op is not valid in requests")),
            _ => Err(StorageError::protocol(format!("unknown request op {code}"))),
        }
    }

    pub(crate) fn from_response_code(code: u16) -> StorageResult<Self> {
        match code {
            1 => Ok(Self::AttachManaged),
            2 => Ok(Self::AttachConfigured),
            3 => Ok(Self::Open),
            4 => Ok(Self::Read),
            5 => Ok(Self::Close),
            6 => Ok(Self::Upload),
            7 => Ok(Self::InvalidateObjectCache),
            8 => Ok(Self::Delete),
            9 => Ok(Self::DeletePrefix),
            10 => Ok(Self::List),
            11 => Ok(Self::Head),
            12 => Ok(Self::DeleteObjects),
            13 => Ok(Self::CloseList),
            14 => Ok(Self::ProbeStore),
            15 => Ok(Self::Ready),
            1000 => Ok(Self::Error),
            _ => Err(StorageError::protocol(format!(
                "unknown response op {code}"
            ))),
        }
    }
}
