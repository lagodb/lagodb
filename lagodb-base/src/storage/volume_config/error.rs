use std::path::PathBuf;

use lagodb_core::diag::{SqlStateError, error_source_chain_detail};
use lagodb_core::storage::volume::StorageVolumeId;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum LocationValidationError {
    #[error("location must use s3://, gs:// or az://")]
    MissingScheme,
    #[error("location provider is not supported")]
    UnsupportedProvider,
    #[error("object namespace is invalid")]
    InvalidNamespace,
    #[error("configured root prefix is not canonical")]
    InvalidRootPrefix,
    #[error("provider options do not match the selected provider")]
    ProviderOptions(#[source] serde_json::Error),
}

#[derive(Debug, Error)]
pub(crate) enum CredentialValidationError {
    #[error("credential JSON does not match a supported credential shape")]
    InvalidShape(#[source] serde_json::Error),
    #[error("credential type does not match location provider")]
    ProviderMismatch,
    #[error("credential fields must not be empty")]
    EmptyFields,
}

#[derive(Debug, Error)]
pub(crate) enum SnapshotValidationError {
    #[error("unsupported storage volume format version {0}")]
    UnsupportedFormat(u32),
    #[error("next_volume_id is outside the allocation range")]
    InvalidNextVolumeId,
    #[error("storage volume id {volume_id} has invalid location")]
    InvalidLocation {
        volume_id: StorageVolumeId,
        #[source]
        source: LocationValidationError,
    },
    #[error("storage volume id {volume_id} has invalid credential")]
    InvalidCredential {
        volume_id: StorageVolumeId,
        #[source]
        source: CredentialValidationError,
    },
    #[error("duplicate storage volume id {0}")]
    DuplicateId(StorageVolumeId),
    #[error("allocated storage volume id {0} is not below next_volume_id")]
    IdNotBelowNext(StorageVolumeId),
    #[error("storage volume id {0} has an invalid creation timestamp")]
    InvalidCreatedAt(StorageVolumeId),
    #[error("storage volume id {0} has an expiration before creation")]
    InvalidExpiration(StorageVolumeId),
    #[error("storage volume id {0} has an invalid tablespace OID")]
    InvalidTablespaceOid(StorageVolumeId),
    #[error("tablespace OID {0} is bound to more than one storage volume")]
    DuplicateBoundTablespace(u32),
    #[error("storage volume id {0} has a retirement mark before creation")]
    InvalidRetirementMark(StorageVolumeId),
    #[error("storage volume id {0} has a purge deadline before retirement")]
    InvalidRetirementPurge(StorageVolumeId),
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ConfigSecurityError {
    #[error("path is not a real directory")]
    NotDirectory,
    #[error("path is not a regular file")]
    NotRegularFile,
    #[error("path has the wrong owner")]
    WrongOwner,
    #[error("path permissions are {actual:o}, expected {expected:o}")]
    WrongMode { actual: u32, expected: u32 },
}

#[derive(Debug, Error)]
pub(crate) enum StorageVolumeError {
    #[error("storage volume name must contain 1..=63 UTF-8 bytes and no NUL")]
    InvalidName,
    #[error("invalid storage volume location")]
    InvalidLocation(#[from] LocationValidationError),
    #[error("invalid storage volume credential")]
    InvalidCredential(#[from] CredentialValidationError),
    #[error("storage volume {0:?} does not exist")]
    NotFound(String),
    #[error("storage volume id {0} does not exist")]
    NotFoundId(StorageVolumeId),
    #[error(
        "storage volume binding target changed while CREATE TABLESPACE was in progress (expected id {expected_id}, found id {actual_id})"
    )]
    BindingConflict {
        expected_id: StorageVolumeId,
        actual_id: StorageVolumeId,
    },
    #[error("storage volume name {0:?} conflicts with a different configuration")]
    NameConflict(String),
    #[error("storage volume id space is exhausted")]
    IdExhausted,
    #[error("storage volume is already bound to tablespace OID {0}")]
    AlreadyBound(u32),
    #[error("tablespace OID {0} is already bound to another storage volume")]
    TablespaceAlreadyBound(u32),
    #[error("storage volume is not bound to a tablespace")]
    NotBound,
    #[error("storage volume has expired and cannot be bound")]
    Expired,
    #[error("tablespace OID is invalid")]
    InvalidTablespaceOid,
    #[error("storage volume cannot be {operation} in its current lifecycle")]
    LifecycleOperation { operation: &'static str },
    #[error("storage volume expiration must be a positive number of seconds")]
    InvalidTtl,
    #[error("storage volume timestamp arithmetic overflowed")]
    TimestampOverflow,
    #[error("storage volume config {operation} failed for {path:?}")]
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
        // `true` means rename completed; only the following durability step failed.
        published: bool,
        #[source]
        source: std::io::Error,
    },
    #[error("storage volume config JSON is invalid in {path:?}")]
    ConfigJson {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("storage volume config snapshot is invalid")]
    InvalidSnapshot(#[source] SnapshotValidationError),
    #[error("storage volume config security check failed for {path:?}")]
    ConfigSecurity {
        path: PathBuf,
        #[source]
        source: ConfigSecurityError,
    },
    #[error("storage backend configuration is invalid")]
    BackendConfig(#[from] lagodb_storage::StorageError),
    #[error("storage volume invariant failed: {0}")]
    Invariant(&'static str),
}

impl StorageVolumeError {
    pub(crate) fn config_io(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::config_io_with_publish(operation, path, false, source)
    }

    pub(crate) fn config_io_published(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::config_io_with_publish(operation, path, true, source)
    }

    fn config_io_with_publish(
        operation: &'static str,
        path: impl Into<PathBuf>,
        published: bool,
        source: std::io::Error,
    ) -> Self {
        Self::ConfigIo {
            operation,
            path: path.into(),
            published,
            source,
        }
    }

    pub(crate) const fn was_published(&self) -> bool {
        matches!(
            self,
            Self::ConfigIo {
                published: true,
                ..
            }
        )
    }

    pub(crate) fn diagnostic_message(&self) -> String {
        match error_source_chain_detail(self) {
            Some(detail) => format!("{self}\n{detail}"),
            None => self.to_string(),
        }
    }
}

impl SqlStateError for StorageVolumeError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::NotFound(_) | Self::NotFoundId(_) => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }
            Self::BindingConflict { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::NameConflict(_) => PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            Self::AlreadyBound(_) | Self::TablespaceAlreadyBound(_) => {
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE
            }
            Self::NotBound | Self::LifecycleOperation { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE
            }
            Self::Expired => PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            Self::IdExhausted => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            Self::InvalidTablespaceOid | Self::InvalidTtl => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::ConfigIo { .. } => PgSqlErrorCode::ERRCODE_IO_ERROR,
            Self::ConfigJson { .. } | Self::ConfigSecurity { .. } => {
                PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR
            }
            Self::InvalidSnapshot(_) => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
            Self::TimestampOverflow => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::Invariant(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::InvalidName
            | Self::InvalidLocation(_)
            | Self::InvalidCredential(_)
            | Self::BackendConfig(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
        }
    }
}
