use std::path::PathBuf;

use pg_lakebase_core::diag::{SqlStateError, error_source_chain_detail};
use pg_lakebase_core::storage_volume::StorageVolumeId;
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
    #[error("storage volume id {0} has an invalid bound tablespace OID")]
    InvalidBoundTablespace(StorageVolumeId),
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
    #[error("storage volume name {0:?} conflicts with a different configuration")]
    NameConflict(String),
    #[error("storage volume id space is exhausted")]
    IdExhausted,
    #[error("storage volume is already bound to tablespace OID {0}")]
    AlreadyBound(u32),
    #[error("storage volume config {operation} failed for {path:?}")]
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
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
    BackendConfig(#[from] pg_lakebase_storage::StorageError),
    #[error("storage volume invariant failed: {0}")]
    Invariant(&'static str),
}

impl StorageVolumeError {
    pub(crate) fn config_io(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::ConfigIo {
            operation,
            path: path.into(),
            source,
        }
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
            Self::NotFound(_) => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            Self::NameConflict(_) => PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            Self::AlreadyBound(_) => PgSqlErrorCode::ERRCODE_OBJECT_IN_USE,
            Self::IdExhausted => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            Self::ConfigIo { .. } => PgSqlErrorCode::ERRCODE_IO_ERROR,
            Self::ConfigJson { .. } | Self::ConfigSecurity { .. } => {
                PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR
            }
            Self::InvalidSnapshot(_) => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
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
