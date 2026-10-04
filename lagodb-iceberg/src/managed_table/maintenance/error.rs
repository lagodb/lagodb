//! Iceberg VACUUM failures and their SQLSTATE classification.

use lagodb_core::diag::SqlStateError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergVacuumError {
    #[error("gc.enabled must be true")]
    GcDisabled,
    #[error(
        "VACUUM cannot be combined with DML, DDL, TRUNCATE, or DROP for the same relation"
    )]
    ActionConflict,
    #[error("invalid Iceberg VACUUM policy: {0}")]
    InvalidPolicy(String),
    #[error("Iceberg VACUUM path is outside the relation-owned table root: {0}")]
    UnsafePath(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("Iceberg relation {relid} unexpectedly owns a PostgreSQL TOAST relation")]
    UnexpectedToastRelation { relid: pg_sys::Oid },
}

impl SqlStateError for IcebergVacuumError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidPolicy(_) => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            Self::GcDisabled
            | Self::ActionConflict
            | Self::UnsafePath(_)
            | Self::UnexpectedToastRelation { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::ResourceLimit(_) => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        }
    }
}
