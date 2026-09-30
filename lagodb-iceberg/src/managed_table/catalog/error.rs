//! Local Iceberg metadata catalog failures and their SQLSTATE classification.

use std::fmt::{Display, Formatter};

use lagodb_core::diag::{PgError, SqlStateError};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCatalogOperation {
    Access,
    Insert,
    Read,
    Update,
    Delete,
}

impl Display for MetadataCatalogOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Access => f.write_str("access"),
            Self::Insert => f.write_str("insert"),
            Self::Read => f.write_str("read"),
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

#[derive(Error, Debug)]
pub enum MetadataCatalogError {
    #[error("failed to {operation} iceberg.iceberg_metadata catalog: {source}")]
    Operation {
        operation: MetadataCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("metadata catalog record not found for relid: {0}")]
    NotFound(pg_sys::Oid),

    #[error("metadata catalog record already exists for relid: {0}")]
    AlreadyExists(pg_sys::Oid),

    #[error("invalid metadata catalog record: {0}")]
    InvalidRecord(String),

    #[error("optimistic locking failed: metadata location changed concurrently")]
    Conflict,
}

impl SqlStateError for MetadataCatalogError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Operation { source, .. } => source.sql_error_code(),
            Self::NotFound(_) => PgSqlErrorCode::ERRCODE_NO_DATA_FOUND,
            Self::AlreadyExists(_) => PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION,
            Self::InvalidRecord(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::Conflict => PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE,
        }
    }
}
