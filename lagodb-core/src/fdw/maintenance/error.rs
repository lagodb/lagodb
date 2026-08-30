//! Error conversion for FDW maintenance callback boundaries.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::Display;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};

use super::super::provider::ForeignDataWrapper;

/// PostgreSQL callback phase attached at the FFI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignTableMaintenancePhase {
    Analyze,
    AcquireSampleRows,
    Truncate,
}

impl ForeignTableMaintenancePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Analyze => "AnalyzeForeignTable",
            Self::AcquireSampleRows => "AcquireSampleRowsFunc",
            Self::Truncate => "ExecForeignTruncate",
        }
    }
}

#[derive(Debug)]
pub struct ForeignTableMaintenanceError(Box<ForeignTableMaintenanceErrorKind>);

#[derive(Debug, Error)]
enum ForeignTableMaintenanceErrorKind {
    #[error("FDW {provider:?} {phase} callback failed: {source}", phase = phase.as_str())]
    Callback {
        provider: &'static CStr,
        phase: ForeignTableMaintenancePhase,
        #[source]
        source: Box<ForeignTableMaintenanceError>,
    },
    #[error("FDW provider maintenance error: {source}")]
    Provider {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{message}")]
    Framework {
        sqlerrcode: PgSqlErrorCode,
        message: String,
    },
    #[error("{report}")]
    PgReport { report: PgReportParts },
}

impl ForeignTableMaintenanceError {
    fn new(kind: ForeignTableMaintenanceErrorKind) -> Self {
        Self(Box::new(kind))
    }

    /// Wrap a provider/domain error and preserve its SQLSTATE.
    pub fn provider<E>(source: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::new(ForeignTableMaintenanceErrorKind::Provider {
            sqlerrcode: source.sql_error_code(),
            source: Box::new(source),
        })
    }

    pub fn unsupported(message: impl Display) -> Self {
        Self::new(ForeignTableMaintenanceErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            message: message.to_string(),
        })
    }

    pub(crate) fn framework(message: impl Display) -> Self {
        Self::new(ForeignTableMaintenanceErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            message: message.to_string(),
        })
    }

    pub(crate) fn with_callback_phase<P: ForeignDataWrapper>(
        self,
        phase: ForeignTableMaintenancePhase,
    ) -> Self {
        if matches!(&*self.0, ForeignTableMaintenanceErrorKind::Callback { .. }) {
            return self;
        }
        Self::new(ForeignTableMaintenanceErrorKind::Callback {
            provider: P::NAME,
            phase,
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
            ForeignTableMaintenanceErrorKind::Callback { source, .. } => {
                source.append_pg_report_extras(details, hints)
            }
            ForeignTableMaintenanceErrorKind::PgReport { report } => {
                if let Some(detail) = report.detail.clone() {
                    details.push(detail);
                }
                if let Some(hint) = report.hint.clone() {
                    hints.push(hint);
                }
            }
            ForeignTableMaintenanceErrorKind::Provider { .. }
            | ForeignTableMaintenanceErrorKind::Framework { .. } => {}
        }
    }
}

impl Display for ForeignTableMaintenanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.0, formatter)
    }
}

impl StdError for ForeignTableMaintenanceError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl SqlStateError for ForeignTableMaintenanceError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignTableMaintenanceErrorKind::Callback { source, .. } => {
                source.sql_error_code()
            }
            ForeignTableMaintenanceErrorKind::Provider { sqlerrcode, .. }
            | ForeignTableMaintenanceErrorKind::Framework { sqlerrcode, .. } => {
                *sqlerrcode
            }
            ForeignTableMaintenanceErrorKind::PgReport { report } => {
                report.sqlerrcode
            }
        }
    }
}

impl From<PgReportError> for ForeignTableMaintenanceError {
    fn from(error: PgReportError) -> Self {
        Self::new(ForeignTableMaintenanceErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl PgReportableError for ForeignTableMaintenanceError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<ForeignTableMaintenanceError> for ErrorReport {
    fn from(error: ForeignTableMaintenanceError) -> Self {
        error.into_error_report()
    }
}
