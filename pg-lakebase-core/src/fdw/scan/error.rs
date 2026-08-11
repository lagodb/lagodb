//! FDW framework errors and PostgreSQL error-report conversion.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::{PgLogLevel, PgSqlErrorCode};

use super::super::provider::ForeignDataWrapper;
use super::super::row_identity::ForeignRowIdentityError;
use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};
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
        }
    }
}

/// Framework-level error for planner and executor callbacks.
///
/// Provider domain errors are boxed only on an error path.  Normal scan rows
/// do not construct this type, allocate a source chain, or format a message.
#[derive(Debug)]
pub struct ForeignScanError(Box<ForeignScanErrorKind>);

#[derive(Debug)]
enum ForeignScanErrorKind {
    Callback {
        provider: &'static CStr,
        phase: ForeignScanPhase,
        source: Box<ForeignScanError>,
    },
    Provider {
        sqlerrcode: PgSqlErrorCode,
        source: Box<dyn StdError + Send + Sync>,
    },
    Framework {
        sqlerrcode: PgSqlErrorCode,
        message: String,
    },
    PrivateCodec {
        source: Box<dyn StdError + Send + Sync>,
    },
    PgReport {
        sqlerrcode: PgSqlErrorCode,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

impl Display for ForeignScanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &*self.0 {
            ForeignScanErrorKind::Callback {
                provider,
                phase,
                source,
            } => write!(
                f,
                "FDW {provider:?} {} callback failed: {source}",
                phase.as_str()
            ),
            ForeignScanErrorKind::Provider { source, .. } => {
                write!(f, "FDW provider error: {source}")
            }
            ForeignScanErrorKind::Framework { message, .. } => f.write_str(message),
            ForeignScanErrorKind::PrivateCodec { source } => {
                write!(f, "FDW private-data codec error: {source}")
            }
            ForeignScanErrorKind::PgReport { message, .. } => f.write_str(message),
        }
    }
}

impl StdError for ForeignScanError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &*self.0 {
            ForeignScanErrorKind::Callback { source, .. } => Some(source),
            ForeignScanErrorKind::Provider { source, .. }
            | ForeignScanErrorKind::PrivateCodec { source } => Some(source.as_ref()),
            ForeignScanErrorKind::Framework { .. }
            | ForeignScanErrorKind::PgReport { .. } => None,
        }
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

    /// Restore the memory context that was current on callback entry, then
    /// raise this error as a PostgreSQL ERROR.
    pub(crate) fn report_after_switch(self, prior_ctx: pg_sys::MemoryContext) -> ! {
        // SAFETY: every trampoline captures `prior_ctx` from PostgreSQL at
        // callback entry and keeps it live until error reporting completes.
        unsafe {
            pg_sys::MemoryContextSwitchTo(prior_ctx);
        }
        self.report(PgLogLevel::ERROR)
    }

    /// Report a teardown error without interrupting framework cleanup.
    pub(crate) fn report_warning(self) {
        ErrorReport::from(self).report(PgLogLevel::WARNING);
    }

    fn report(self, level: PgLogLevel) -> ! {
        ErrorReport::from(self).report(level);
        unreachable!()
    }
}

impl SqlStateError for ForeignScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignScanErrorKind::Callback { source, .. } => source.sql_error_code(),
            ForeignScanErrorKind::Provider { sqlerrcode, .. }
            | ForeignScanErrorKind::Framework { sqlerrcode, .. }
            | ForeignScanErrorKind::PgReport { sqlerrcode, .. } => *sqlerrcode,
            ForeignScanErrorKind::PrivateCodec { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<PgReportError> for ForeignScanError {
    fn from(error: PgReportError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let report = error.into_report();
        Self::new(ForeignScanErrorKind::PgReport {
            sqlerrcode,
            message: report.message().to_owned(),
            detail: report.detail().map(str::to_owned),
            hint: report.hint().map(str::to_owned),
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

impl From<ForeignScanError> for ErrorReport {
    fn from(error: ForeignScanError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let mut details = Vec::new();
        if let Some(chain) = error_source_chain_detail(&error) {
            details.push(chain);
        }

        let mut hints = Vec::new();
        collect_pg_report_parts(&error, &mut details, &mut hints);
        let mut report = ErrorReport::new(sqlerrcode, report_message(&error), "");
        if let Some(detail) = join_error_details(details.into_iter().map(Some)) {
            report = report.set_detail(detail);
        }
        if let Some(hint) = join_error_details(hints.into_iter().map(Some)) {
            report = report.set_hint(hint);
        }
        report
    }
}

fn report_message(error: &ForeignScanError) -> String {
    match &*error.0 {
        ForeignScanErrorKind::Callback {
            provider,
            phase,
            source,
        } => format!(
            "FDW {provider:?} {} callback failed: {source}",
            phase.as_str()
        ),
        ForeignScanErrorKind::PgReport { message, .. } => message.clone(),
        _ => error.to_string(),
    }
}

fn collect_pg_report_parts(
    error: &ForeignScanError,
    details: &mut Vec<String>,
    hints: &mut Vec<String>,
) {
    match &*error.0 {
        ForeignScanErrorKind::Callback { source, .. } => {
            collect_pg_report_parts(source, details, hints)
        }
        ForeignScanErrorKind::PgReport { detail, hint, .. } => {
            if let Some(detail) = detail.clone() {
                details.push(detail);
            }
            if let Some(hint) = hint.clone() {
                hints.push(hint);
            }
        }
        _ => {}
    }
}
