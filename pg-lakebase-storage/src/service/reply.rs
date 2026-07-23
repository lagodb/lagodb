//! Service responses.
//!
//! [`ServiceReply`] bundles a typed [`CommandOutput`] with an optional out-of-band
//! [`ResponseAttachment`]. The attachment is currently only used to piggyback a POSIX
//! [`std::fs::File`] through SCM_RIGHTS when a `CompleteFile` open is promoted to direct-IO.
//!
//! [`ReadBody`] (and its [`ReadFileRange`] partner) pin a [`crate::cache::CacheActivityGuard`]
//! across the wire response so the cache cannot evict a file while the writer is still
//! streaming from it.

use std::fmt;

#[cfg(test)]
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::cache::CacheActivityGuard;
#[cfg(test)]
use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::protocol::{ListCursor, WireListEntry};

/// Service response envelope returned by [`StorageService::execute`](crate::service::StorageService::execute).
pub(crate) struct ServiceReply {
    pub output: CommandOutput,
    pub attachment: Option<ResponseAttachment>,
}

impl ServiceReply {
    pub(crate) fn new(output: CommandOutput) -> Self {
        Self {
            output,
            attachment: None,
        }
    }

    pub(crate) fn with_attachment(
        output: CommandOutput,
        attachment: ResponseAttachment,
    ) -> Self {
        Self {
            output,
            attachment: Some(attachment),
        }
    }
}

/// Out-of-band payload that accompanies certain replies (currently only SCM_RIGHTS file descriptors).
pub(crate) enum ResponseAttachment {
    File(std::fs::File),
}

#[derive(Debug)]
pub(crate) enum CommandOutput {
    Open(OpenOutput),
    Head(HeadOutput),
    Read(ReadOutput),
    Close,
    Upload(UploadOutput),
    RegisterStore(RegisterStoreOutput),
    UnregisterStore(UnregisterStoreOutput),
    PurgeStoreCache,
    ProbeStore(crate::backend::StorageProbeResult),
    InvalidateObjectCache(InvalidateObjectCacheOutput),
    Delete,
    DeletePrefix(DeletePrefixOutput),
    DeleteObjects(DeleteObjectsOutput),
    List(ListOutput),
    CloseList,
}

impl CommandOutput {
    /// Builds a [`CommandOutput::Read`] carrying in-memory bytes.
    pub(crate) fn read_bytes(data: bytes::Bytes, eof: bool) -> Self {
        Self::Read(ReadOutput {
            body: ReadBody::Bytes(data),
            eof,
        })
    }

    /// Builds a [`CommandOutput::Read`] that streams from a cache-resident file while pinning
    /// its `read_guard`.
    pub(crate) fn read_file_range(
        file: tokio::fs::File,
        offset: u64,
        len: u32,
        eof: bool,
        read_guard: CacheActivityGuard,
    ) -> Self {
        Self::Read(ReadOutput {
            body: ReadBody::FileRange(ReadFileRange {
                file,
                offset,
                len,
                _read_guard: read_guard,
            }),
            eof,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OpenOutput {
    pub handle: FileHandle,
    pub size: u64,
    pub direct_io: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeadOutput {
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegisterStoreOutput {
    pub replaced: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UnregisterStoreOutput {
    pub removed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidateObjectCacheOutput {
    pub removed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeletePrefixOutput {
    pub deleted: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteObjectsOutput {
    pub deleted: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListOutput {
    pub entries: Vec<WireListEntry>,
    pub next_cursor: Option<ListCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UploadOutput {
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ReadOutput {
    pub body: ReadBody,
    pub eof: bool,
}

impl ReadOutput {
    #[cfg(test)]
    pub(crate) async fn into_bytes(self) -> StorageResult<(Vec<u8>, bool)> {
        Ok((self.body.into_bytes().await?, self.eof))
    }
}

pub(crate) enum ReadBody {
    Bytes(bytes::Bytes),
    FileRange(ReadFileRange),
}

impl ReadBody {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Bytes(data) => data.len(),
            Self::FileRange(range) => range.len as usize,
        }
    }

    #[cfg(test)]
    async fn into_bytes(self) -> StorageResult<Vec<u8>> {
        match self {
            Self::Bytes(data) => Ok(data.to_vec()),
            Self::FileRange(mut range) => {
                let mut data = vec![0; range.len as usize];
                range
                    .file
                    .seek(std::io::SeekFrom::Start(range.offset))
                    .await?;
                range.file.read_exact(&mut data).await.map_err(|error| {
                    StorageError::io(
                        "failed to materialize read file range for test",
                        error,
                    )
                })?;
                Ok(data)
            }
        }
    }
}

impl fmt::Debug for ReadBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(data) => f.debug_tuple("Bytes").field(&data.len()).finish(),
            Self::FileRange(range) => range.fmt(f),
        }
    }
}

pub(crate) struct ReadFileRange {
    pub file: tokio::fs::File,
    pub offset: u64,
    pub len: u32,
    _read_guard: CacheActivityGuard,
}

impl fmt::Debug for ReadFileRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileRange")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}
