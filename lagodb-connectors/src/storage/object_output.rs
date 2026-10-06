//! Object allocation and key generation for one write statement.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use chrono::{Datelike, Utc};
use lagodb_core::diag::PgReportError;
use lagodb_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pgrx::PgSqlErrorCode;
use uuid::Uuid;

use crate::error::ConnectorError;

use super::{ObjectLocationKind, ResolvedStorageLocation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectFileSuffix(&'static str);

impl ObjectFileSuffix {
    pub(crate) const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Output keys follow the object-store immutability contract.
///
/// Prefix output always allocates operation-unique keys. Exact output is for
/// publishing to a previously unused key; overwriting an existing key is not
/// a supported publication protocol and requires explicit cache coordination
/// outside this writer. Local exact COPY output uses PostgreSQL's overwrite
/// semantics; local directory output retains the unique-file contract.
pub(crate) enum ObjectOutput {
    LocalExact {
        path: Option<PathBuf>,
    },
    LocalPrefix {
        keys: PartitionedKeyGenerator,
        target_file_bytes: NonZeroU64,
    },
    Exact {
        object: Option<ObjectAccess>,
    },
    Prefix {
        access: ObjectPrefixAccess,
        keys: PartitionedKeyGenerator,
        target_file_bytes: NonZeroU64,
    },
}

/// One allocated output object together with its transaction disposition.
pub(crate) enum AllocatedObject {
    Remote {
        object: ObjectAccess,
        delete_on_abort: bool,
    },
    Local {
        path: PathBuf,
        delete_on_abort: bool,
    },
}

impl ObjectOutput {
    pub(crate) fn resolve(
        location: &ResolvedStorageLocation,
        kind: ObjectLocationKind,
        prefix_target_file_bytes: impl FnOnce() -> NonZeroU64,
    ) -> Result<Self, ConnectorError> {
        if let Some(path) = location.local_path() {
            if !Path::new(path).is_absolute() {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INVALID_NAME,
                    "relative path not allowed for local file output",
                )
                .into());
            }
            return Ok(match kind {
                ObjectLocationKind::Exact => Self::LocalExact {
                    path: Some(PathBuf::from(path)),
                },
                ObjectLocationKind::Prefix => Self::LocalPrefix {
                    keys: PartitionedKeyGenerator::new(path.to_owned()),
                    target_file_bytes: prefix_target_file_bytes(),
                },
            });
        }
        let manager = StorageManager::from_pg_gucs()?;
        match kind {
            ObjectLocationKind::Exact => Ok(Self::Exact {
                object: Some(location.acquire_object_access(&manager)?),
            }),
            ObjectLocationKind::Prefix => {
                let prefix = location.normalized_prefix();
                Ok(Self::Prefix {
                    access: location.acquire_prefix_access(&manager, &prefix)?,
                    keys: PartitionedKeyGenerator::new(prefix),
                    target_file_bytes: prefix_target_file_bytes(),
                })
            }
        }
    }

    /// PostgreSQL opens and truncates an exact local COPY file before running
    /// the query. Prefix allocation remains demand-driven.
    pub(crate) const fn open_before_execution(&self) -> bool {
        matches!(self, Self::LocalExact { .. })
    }

    pub(crate) const fn should_roll(&self, estimated_file_bytes: u64) -> bool {
        match self {
            Self::Exact { .. } | Self::LocalExact { .. } => false,
            Self::Prefix {
                target_file_bytes, ..
            }
            | Self::LocalPrefix {
                target_file_bytes, ..
            } => estimated_file_bytes >= target_file_bytes.get(),
        }
    }

    pub(crate) fn allocate_next(
        &mut self,
        suffix: ObjectFileSuffix,
    ) -> Result<AllocatedObject, ConnectorError> {
        match self {
            Self::LocalExact { path } => Ok(AllocatedObject::Local {
                path: path.take().expect("an exact output is allocated only once"),
                delete_on_abort: false,
            }),
            Self::LocalPrefix { keys, .. } => Ok(AllocatedObject::Local {
                path: PathBuf::from(keys.next_key(suffix)),
                delete_on_abort: true,
            }),
            Self::Exact { object } => Ok(AllocatedObject::Remote {
                object: object
                    .take()
                    .expect("an exact output is allocated only once"),
                delete_on_abort: false,
            }),
            Self::Prefix { access, keys, .. } => Ok(AllocatedObject::Remote {
                object: access.object(&keys.next_key(suffix))?,
                delete_on_abort: true,
            }),
        }
    }
}

/// One operation-scoped, collision-resistant object-key sequence.
pub(crate) struct PartitionedKeyGenerator {
    directory: Box<str>,
    writer_id: Uuid,
    sequence: u32,
}

impl PartitionedKeyGenerator {
    fn new(prefix: String) -> Self {
        let today = Utc::now().date_naive();
        let directory = format!(
            "{}{}/{:02}/{:02}/",
            prefix,
            today.year(),
            today.month(),
            today.day()
        );
        Self {
            directory: directory.into(),
            writer_id: Uuid::now_v7(),
            sequence: 0,
        }
    }

    fn next_key(&mut self, suffix: ObjectFileSuffix) -> String {
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("one statement cannot create more than u32::MAX objects");
        format!(
            "{}part-{}-{sequence:05}.{}",
            self.directory,
            self.writer_id,
            suffix.as_str()
        )
    }
}
