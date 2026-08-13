//! Errors for format-neutral COPY byte adapters.

use std::error::Error as StdError;

use pg_lakebase_storage::StorageError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{PgError, PgReportError, SqlStateError};

#[derive(Debug, Error)]
pub enum CopyError {
    #[error("COPY data callback failed: {source}")]
    Provider {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(
        "COPY data callback returned {actual} bytes; expected EOF or at most {max} bytes"
    )]
    InvalidByteCount { actual: usize, max: usize },

    #[error(
        "COPY data callback was invoked without an installed source or destination"
    )]
    MissingCallbackState,

    #[error("invalid PostgreSQL COPY column layout: {0}")]
    InvalidColumnLayout(&'static str),

    #[error("COPY row encoder was used after finish")]
    EncoderFinished,

    #[error(transparent)]
    Postgres(#[from] PgReportError),
}

impl CopyError {
    pub fn provider<E>(error: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::Provider {
            sqlerrcode: error.sql_error_code(),
            source: Box::new(error),
        }
    }

    pub(crate) fn invalid_byte_count(actual: usize, max: usize) -> Self {
        Self::InvalidByteCount { actual, max }
    }

    pub fn storage(error: StorageError) -> Self {
        Self::Storage(error)
    }

    pub(crate) const fn invalid_column_layout(reason: &'static str) -> Self {
        Self::InvalidColumnLayout(reason)
    }

    pub(crate) const fn encoder_finished() -> Self {
        Self::EncoderFinished
    }
}

impl From<PgError> for CopyError {
    fn from(error: PgError) -> Self {
        Self::Postgres(PgReportError::from_pg_error(error))
    }
}

impl SqlStateError for CopyError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Provider { sqlerrcode, .. } => *sqlerrcode,
            Self::Storage(error) => error.sql_error_code(),
            Self::InvalidByteCount { .. }
            | Self::MissingCallbackState
            | Self::InvalidColumnLayout(_)
            | Self::EncoderFinished => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::Postgres(error) => error.sql_error_code(),
        }
    }
}
