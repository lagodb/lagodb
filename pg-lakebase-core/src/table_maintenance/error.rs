use std::ffi::CStr;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};

#[derive(Debug)]
pub struct TableMaintenanceError(Box<TableMaintenanceErrorKind>);

#[derive(Debug, Error)]
enum TableMaintenanceErrorKind {
    #[error("table-maintenance provider {provider:?} failed: {source}")]
    ProviderRuntime {
        provider: &'static CStr,
        #[source]
        source: Box<TableMaintenanceError>,
    },

    #[error("table-maintenance provider error: {source}")]
    Provider {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("table-maintenance framework error: {message}")]
    Framework { message: String },

    #[error("table-maintenance internal error: {source}")]
    Internal {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("{message}")]
    PgReport {
        sqlerrcode: PgSqlErrorCode,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

impl TableMaintenanceError {
    fn new(kind: TableMaintenanceErrorKind) -> Self {
        Self(Box::new(kind))
    }

    pub fn provider<E>(source: E) -> Self
    where
        E: SqlStateError + std::error::Error + Send + Sync + 'static,
    {
        Self::new(TableMaintenanceErrorKind::Provider {
            sqlerrcode: source.sql_error_code(),
            source: Box::new(source),
        })
    }

    pub fn framework(message: impl Into<String>) -> Self {
        Self::new(TableMaintenanceErrorKind::Framework {
            message: message.into(),
        })
    }

    pub fn internal<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::new(TableMaintenanceErrorKind::Internal {
            source: Box::new(source),
        })
    }

    pub(crate) fn with_provider(self, provider: &'static CStr) -> Self {
        Self::new(TableMaintenanceErrorKind::ProviderRuntime {
            provider,
            source: Box::new(self),
        })
    }
}

impl std::fmt::Display for TableMaintenanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, formatter)
    }
}

impl std::error::Error for TableMaintenanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl SqlStateError for TableMaintenanceError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            TableMaintenanceErrorKind::ProviderRuntime { source, .. } => {
                source.sql_error_code()
            }
            TableMaintenanceErrorKind::Provider { sqlerrcode, .. }
            | TableMaintenanceErrorKind::PgReport { sqlerrcode, .. } => *sqlerrcode,
            TableMaintenanceErrorKind::Framework { .. }
            | TableMaintenanceErrorKind::Internal { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<PgReportError> for TableMaintenanceError {
    fn from(error: PgReportError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let report = error.into_report();
        Self::new(TableMaintenanceErrorKind::PgReport {
            sqlerrcode,
            message: report.message().to_owned(),
            detail: report.detail().map(str::to_owned),
            hint: report.hint().map(str::to_owned),
        })
    }
}

impl From<TableMaintenanceError> for ErrorReport {
    fn from(error: TableMaintenanceError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let (message, postgres_detail, postgres_hint) = match &*error.0 {
            TableMaintenanceErrorKind::PgReport {
                message,
                detail,
                hint,
                ..
            } => (message.clone(), detail.clone(), hint.clone()),
            _ => (error.to_string(), None, None),
        };
        let detail = join_error_details([
            postgres_detail,
            error_source_chain_detail(&error),
        ]);
        let mut report = ErrorReport::new(sqlerrcode, message, "");
        if let Some(detail) = detail {
            report = report.set_detail(detail);
        }
        if let Some(hint) = postgres_hint {
            report = report.set_hint(hint);
        }
        report
    }
}
