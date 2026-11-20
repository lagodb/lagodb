//! Errors returned by the FDW option validator.

use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use pgrx::datum::Array;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use super::provider::ForeignDataWrapper;
use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};
use crate::storage::foreign::ForeignOptionView;

/// Run `P::validate` for the generated SQL validator.
///
/// PostgreSQL passes the catalog's `name=value` `text[]`, or SQL NULL from a
/// direct call. Like `file_fdw_validator`, convert it with
/// `untransformRelOptions` so validation reads the same `DefElem` list as
/// every catalog-backed [`ForeignOptionView`].
pub fn run_validator<P: ForeignDataWrapper>(
    options: Option<Array<'_, &str>>,
    catalog: Option<pg_sys::Oid>,
) -> Result<(), ForeignValidationError> {
    let options = options.map_or(pg_sys::Datum::null(), |options| {
        pg_sys::Datum::from(options.into_array_type())
    });
    // SAFETY: `options` is NULL or the detoasted `text[]` argument of this
    // call. untransformRelOptions returns NIL for NULL, raises an ERROR for
    // NULL elements, and copies the names and values into the current memory
    // context, which outlives the view.
    let list = unsafe { pg_sys::untransformRelOptions(options) };
    // SAFETY: `list` is the DefElem list created above.
    P::validate(unsafe { ForeignOptionView::from_raw(list) }, catalog)
}

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
    #[error("{report}")]
    PgReport { report: PgReportParts },
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
        match error.try_into_pg_report() {
            Ok(report) => report.into(),
            Err(error) => Self::new(ForeignValidationErrorKind::Provider {
                sqlerrcode: error.sql_error_code(),
                source: Box::new(error),
            }),
        }
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
            ForeignValidationErrorKind::Provider { sqlerrcode, .. } => *sqlerrcode,
            ForeignValidationErrorKind::PgReport { report } => report.sqlerrcode,
        }
    }
}

impl From<PgReportError> for ForeignValidationError {
    fn from(error: PgReportError) -> Self {
        Self::new(ForeignValidationErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl PgReportableError for ForeignValidationError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<ForeignValidationError> for ErrorReport {
    fn from(error: ForeignValidationError) -> Self {
        error.into_error_report()
    }
}

impl ForeignValidationError {
    fn append_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        if let ForeignValidationErrorKind::PgReport { report } = &*self.0 {
            if let Some(detail) = report.detail.clone() {
                details.push(detail);
            }
            if let Some(hint) = report.hint.clone() {
                hints.push(hint);
            }
        }
    }
}
