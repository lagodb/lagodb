//! Backend-local catalog-backed access to the shared object-storage service.
//!
//! The module owns the PostgreSQL catalog boundary, the per-backend cache, and
//! catalog invalidation lifecycle. The storage service sees one configured context per
//! socket and has no PostgreSQL catalog concepts.

mod access;
mod cache;
mod catalog;
mod handle;
mod identity;
mod manager;

use pg_lakebase_storage::{StorageError, StorageErrorKind};
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::SqlStateError;

impl SqlStateError for StorageError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self.kind() {
            StorageErrorKind::InvalidPath | StorageErrorKind::Configuration => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            StorageErrorKind::NotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            StorageErrorKind::Unsupported => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            StorageErrorKind::Busy => PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
            StorageErrorKind::ResourceExhausted => {
                PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
            }
            StorageErrorKind::Io
            | StorageErrorKind::Backend
            | StorageErrorKind::Cache
            | StorageErrorKind::CacheFillAborted
            | StorageErrorKind::Ambiguous => PgSqlErrorCode::ERRCODE_IO_ERROR,
            StorageErrorKind::Protocol
            | StorageErrorKind::ClosedHandle
            | StorageErrorKind::ExpiredCursor
            | StorageErrorKind::Conflict => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

pub use access::{ObjectAccess, ObjectPrefixAccess};
pub(crate) use catalog::ForeignCatalog;
pub use catalog::{
    ForeignOption, ForeignOptionIter, ForeignOptionView, StorageOptions,
};
pub use handle::StorageHandle;
pub use identity::StorageIdentity;
pub use manager::{StorageAcquireError, StorageConfigProvider, StorageManager};
