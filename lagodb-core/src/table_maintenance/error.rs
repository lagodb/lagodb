use std::ffi::CStr;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};

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

    #[error("{report}")]
    PgReport { report: PgReportParts },
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

    pub(crate) fn report(self) -> ! {
        PgReportError::raise(ErrorReport::from(self))
    }

    fn append_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        match &*self.0 {
            TableMaintenanceErrorKind::ProviderRuntime { source, .. } => {
                source.append_pg_report_extras(details, hints);
            }
            TableMaintenanceErrorKind::PgReport { report } => {
                if let Some(detail) = report.detail.clone() {
                    details.push(detail);
                }
                if let Some(hint) = report.hint.clone() {
                    hints.push(hint);
                }
            }
            TableMaintenanceErrorKind::Provider { .. }
            | TableMaintenanceErrorKind::Framework { .. }
            | TableMaintenanceErrorKind::Internal { .. } => {}
        }
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
            TableMaintenanceErrorKind::Provider { sqlerrcode, .. } => *sqlerrcode,
            TableMaintenanceErrorKind::PgReport { report } => report.sqlerrcode,
            TableMaintenanceErrorKind::Framework { .. }
            | TableMaintenanceErrorKind::Internal { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<PgReportError> for TableMaintenanceError {
    fn from(error: PgReportError) -> Self {
        Self::new(TableMaintenanceErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl PgReportableError for TableMaintenanceError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<TableMaintenanceError> for ErrorReport {
    fn from(error: TableMaintenanceError) -> Self {
        error.into_error_report()
    }
}
