//! FDW framework errors and PostgreSQL error-report conversion.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::Display;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::{PgLogLevel, PgSqlErrorCode};
use thiserror::Error;

use super::super::provider::ForeignDataWrapper;
use super::super::row_identity::ForeignRowIdentityError;
use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};
use crate::plan_data::PlanDataError;

/// PostgreSQL callback phase attached by the framework at an FFI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignScanPhase {
    RelSize,
    Paths,
    Plan,
    Begin,
    Iterate,
    ReScan,
    End,
    Explain,
}

impl ForeignScanPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RelSize => "GetForeignRelSize",
            Self::Paths => "GetForeignPaths",
            Self::Plan => "GetForeignPlan",
            Self::Begin => "BeginForeignScan",
            Self::Iterate => "IterateForeignScan",
            Self::ReScan => "ReScanForeignScan",
            Self::End => "EndForeignScan",
            Self::Explain => "ExplainForeignScan",
        }
    }
}

/// Framework-level error for planner and executor callbacks.
///
/// Provider domain errors are boxed only on an error path.  Normal scan rows
/// do not construct this type, allocate a source chain, or format a message.
#[derive(Debug)]
pub struct ForeignScanError(Box<ForeignScanErrorKind>);

#[derive(Debug, Error)]
enum ForeignScanErrorKind {
    #[error("FDW {provider:?} {phase} callback failed: {source}", phase = phase.as_str())]
    Callback {
        provider: &'static CStr,
        phase: ForeignScanPhase,
        #[source]
        source: Box<ForeignScanError>,
    },
    #[error("FDW provider error: {source}")]
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
    #[error("FDW private-data codec error: {source}")]
    PrivateCodec {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{report}")]
    PgReport { report: PgReportParts },
}

impl Display for ForeignScanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.0, formatter)
    }
}

impl StdError for ForeignScanError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl ForeignScanError {
    fn new(kind: ForeignScanErrorKind) -> Self {
        Self(Box::new(kind))
    }

    /// Wrap a provider/domain error and preserve its SQLSTATE.
    pub fn provider<E>(error: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::new(ForeignScanErrorKind::Provider {
            sqlerrcode: error.sql_error_code(),
            source: Box::new(error),
        })
    }

    /// Plan, private-data, layout, or slot invariant failure.
    pub(crate) fn framework(message: impl Display) -> Self {
        Self::new(ForeignScanErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            message: message.to_string(),
        })
    }

    /// Report a capability that this provider or framework does not support.
    /// This is distinct from a corrupt plan or an invariant violation and is
    /// reported with `FEATURE_NOT_SUPPORTED`.
    pub fn unsupported(message: impl Display) -> Self {
        Self::new(ForeignScanErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            message: message.to_string(),
        })
    }

    pub(crate) fn private_codec(
        error: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::new(ForeignScanErrorKind::PrivateCodec {
            source: Box::new(error),
        })
    }

    pub(crate) fn slot_not_filled(provider: &'static CStr) -> Self {
        Self::framework(format_args!(
            "FDW provider {provider:?} returned Ok(true) without filling the scan slot"
        ))
    }

    pub(crate) fn with_callback_phase<P: ForeignDataWrapper>(
        self,
        phase: ForeignScanPhase,
    ) -> Self {
        Self::new(ForeignScanErrorKind::Callback {
            provider: P::NAME,
            phase,
            source: Box::new(self),
        })
    }

    pub(crate) fn report(self) -> ! {
        PgReportError::raise(ErrorReport::from(self))
    }

    /// Report a teardown error without interrupting framework cleanup.
    pub(crate) fn report_warning(self) {
        ErrorReport::from(self).report(PgLogLevel::WARNING);
    }
}

impl SqlStateError for ForeignScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignScanErrorKind::Callback { source, .. } => source.sql_error_code(),
            ForeignScanErrorKind::Provider { sqlerrcode, .. }
            | ForeignScanErrorKind::Framework { sqlerrcode, .. } => *sqlerrcode,
            ForeignScanErrorKind::PgReport { report } => report.sqlerrcode,
            ForeignScanErrorKind::PrivateCodec { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<PgReportError> for ForeignScanError {
    fn from(error: PgReportError) -> Self {
        Self::new(ForeignScanErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl From<PlanDataError> for ForeignScanError {
    fn from(error: PlanDataError) -> Self {
        Self::private_codec(error)
    }
}

impl From<ForeignRowIdentityError> for ForeignScanError {
    fn from(error: ForeignRowIdentityError) -> Self {
        Self::framework(error)
    }
}

impl PgReportableError for ForeignScanError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<ForeignScanError> for ErrorReport {
    fn from(error: ForeignScanError) -> Self {
        error.into_error_report()
    }
}

impl ForeignScanError {
    fn append_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        match &*self.0 {
            ForeignScanErrorKind::Callback { source, .. } => {
                source.append_pg_report_extras(details, hints)
            }
            ForeignScanErrorKind::PgReport { report } => {
                if let Some(detail) = report.detail.clone() {
                    details.push(detail);
                }
                if let Some(hint) = report.hint.clone() {
                    hints.push(hint);
                }
            }
            _ => {}
        }
    }
}
