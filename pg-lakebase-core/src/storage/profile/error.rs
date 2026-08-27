use lagodb_storage::StorageError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::SqlStateError;

/// Errors produced while parsing or selecting a PostgreSQL storage profile.
#[derive(Debug, Error)]
pub enum StorageProfileError {
    #[error("invalid storage profile option {option:?}: {reason}")]
    InvalidOption {
        option: Box<str>,
        reason: &'static str,
    },

    #[error("invalid object URI: {reason}")]
    InvalidObjectUri { reason: &'static str },

    #[error("storage profile catalog access failed: {0}")]
    Catalog(#[from] crate::diag::PgError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(
        "storage profile {server:?} is ambiguous with {other:?} for scope {scope:?}"
    )]
    AmbiguousScope {
        server: Box<str>,
        other: Box<str>,
        scope: Box<str>,
    },

    #[error("no accessible storage profile covers object location {location:?}")]
    NoMatchingProfile { location: Box<str> },

    #[error("foreign server {server:?} is not accessible under this storage policy")]
    UnavailableServer { server: Box<str> },

    #[error("foreign server {server:?} does not exist")]
    ServerNotFound { server: Box<str> },

    #[error(
        "foreign server {server:?} is not owned by the required storage provider"
    )]
    ServerPolicyMismatch { server: Box<str> },

    #[error("permission denied for foreign server {server:?}")]
    ServerUsageDenied { server: Box<str> },

    #[error("foreign server {server:?} has no user mapping for the effective user")]
    UserMappingMissing { server: Box<str> },

    #[error("object URI is outside foreign server {server:?} scope")]
    ServerOutsideScope { server: Box<str> },

    #[error("foreign server {server:?} cannot access {scheme} object URIs")]
    ProviderMismatch {
        server: Box<str>,
        scheme: &'static str,
    },
}

impl StorageProfileError {
    pub(crate) fn invalid_option(option: &str, reason: &'static str) -> Self {
        Self::InvalidOption {
            option: option.into(),
            reason,
        }
    }

    pub(crate) const fn invalid_object_uri(reason: &'static str) -> Self {
        Self::InvalidObjectUri { reason }
    }
}

impl SqlStateError for StorageProfileError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Catalog(error) => error.sql_error_code(),
            Self::Storage(error) => error.sql_error_code(),
            Self::InvalidOption { .. } => {
                PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME
            }
            Self::InvalidObjectUri { .. }
            | Self::AmbiguousScope { .. }
            | Self::NoMatchingProfile { .. }
            | Self::UnavailableServer { .. }
            | Self::ServerPolicyMismatch { .. }
            | Self::ProviderMismatch { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::ServerNotFound { .. } | Self::UserMappingMissing { .. } => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }
            Self::ServerUsageDenied { .. } | Self::ServerOutsideScope { .. } => {
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
            }
        }
    }
}
