//! Errors returned by the FDW option validator.

use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};

/// Error returned by [`super::ForeignDataWrapper::validate`].
#[derive(Debug)]
pub struct ForeignValidationError(Box<ForeignValidationErrorKind>);

#[derive(Debug, Error)]
enum ForeignValidationErrorKind {
    #[error("FDW validator provider error: {source}")]
    Provider {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{message}")]
    PgReport {
        sqlerrcode: PgSqlErrorCode,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

impl ForeignValidationError {
    fn new(kind: ForeignValidationErrorKind) -> Self {
        Self(Box::new(kind))
    }

    /// Wrap a provider error and preserve its SQLSTATE and source chain.
    pub fn provider<E>(error: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::new(ForeignValidationErrorKind::Provider {
            sqlerrcode: error.sql_error_code(),
            source: Box::new(error),
        })
    }
}

impl Display for ForeignValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&*self.0, f)
    }
}

impl StdError for ForeignValidationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl SqlStateError for ForeignValidationError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignValidationErrorKind::Provider { sqlerrcode, .. }
            | ForeignValidationErrorKind::PgReport { sqlerrcode, .. } => *sqlerrcode,
        }
    }
}

impl From<PgReportError> for ForeignValidationError {
    fn from(error: PgReportError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let report = error.into_report();
        Self::new(ForeignValidationErrorKind::PgReport {
            sqlerrcode,
            message: report.message().to_owned(),
            detail: report.detail().map(str::to_owned),
            hint: report.hint().map(str::to_owned),
        })
    }
}

impl From<ForeignValidationError> for ErrorReport {
    fn from(error: ForeignValidationError) -> Self {
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

fn report_message(error: &ForeignValidationError) -> String {
    match &*error.0 {
        ForeignValidationErrorKind::PgReport { message, .. } => message.clone(),
        _ => error.to_string(),
    }
}

fn collect_pg_report_parts(
    error: &ForeignValidationError,
    details: &mut Vec<String>,
    hints: &mut Vec<String>,
) {
    if let ForeignValidationErrorKind::PgReport { detail, hint, .. } = &*error.0 {
        if let Some(detail) = detail.clone() {
            details.push(detail);
        }
        if let Some(hint) = hint.clone() {
            hints.push(hint);
        }
    }
}
