//! Error conversion for the `IMPORT FOREIGN SCHEMA` FFI boundary.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::{PgLogLevel, PgSqlErrorCode};
use thiserror::Error;

use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};

use super::super::provider::ForeignDataWrapper;

#[derive(Debug)]
pub struct ForeignImportError(Box<ForeignImportErrorKind>);

#[derive(Debug, Error)]
enum ForeignImportErrorKind {
    #[error("FDW {provider:?} ImportForeignSchema callback failed: {source}")]
    Callback {
        provider: &'static CStr,
        #[source]
        source: Box<ForeignImportError>,
    },
    #[error("FDW provider import error: {source}")]
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

impl ForeignImportError {
    fn new(kind: ForeignImportErrorKind) -> Self {
        Self(Box::new(kind))
    }

    pub fn provider<E>(source: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::new(ForeignImportErrorKind::Provider {
            sqlerrcode: source.sql_error_code(),
            source: Box::new(source),
        })
    }

    pub(crate) fn with_callback<P: ForeignDataWrapper>(self) -> Self {
        if matches!(&*self.0, ForeignImportErrorKind::Callback { .. }) {
            return self;
        }
        Self::new(ForeignImportErrorKind::Callback {
            provider: P::NAME,
            source: Box::new(self),
        })
    }

    pub(crate) fn report_after_switch(
        self,
        prior_context: pg_sys::MemoryContext,
    ) -> ! {
        unsafe { pg_sys::MemoryContextSwitchTo(prior_context) };
        ErrorReport::from(self).report(PgLogLevel::ERROR);
        unreachable!()
    }

    fn collect_report_parts(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        match &*self.0 {
            ForeignImportErrorKind::Callback { source, .. } => {
                source.collect_report_parts(details, hints);
            }
            ForeignImportErrorKind::PgReport { detail, hint, .. } => {
                if let Some(detail) = detail.clone() {
                    details.push(detail);
                }
                if let Some(hint) = hint.clone() {
                    hints.push(hint);
                }
            }
            ForeignImportErrorKind::Provider { .. } => {}
        }
    }
}

impl Display for ForeignImportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&*self.0, formatter)
    }
}

impl StdError for ForeignImportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl SqlStateError for ForeignImportError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignImportErrorKind::Callback { source, .. } => {
                source.sql_error_code()
            }
            ForeignImportErrorKind::Provider { sqlerrcode, .. }
            | ForeignImportErrorKind::PgReport { sqlerrcode, .. } => *sqlerrcode,
        }
    }
}

impl From<PgReportError> for ForeignImportError {
    fn from(error: PgReportError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let report = error.into_report();
        Self::new(ForeignImportErrorKind::PgReport {
            sqlerrcode,
            message: report.message().to_owned(),
            detail: report.detail().map(str::to_owned),
            hint: report.hint().map(str::to_owned),
        })
    }
}

impl From<ForeignImportError> for ErrorReport {
    fn from(error: ForeignImportError) -> Self {
        let sqlerrcode = error.sql_error_code();
        let mut details = Vec::new();
        if let Some(chain) = error_source_chain_detail(&error) {
            details.push(chain);
        }
        let mut hints = Vec::new();
        error.collect_report_parts(&mut details, &mut hints);

        let mut report = ErrorReport::new(sqlerrcode, error.to_string(), "");
        if let Some(detail) = join_error_details(details.into_iter().map(Some)) {
            report = report.set_detail(detail);
        }
        if let Some(hint) = join_error_details(hints.into_iter().map(Some)) {
            report = report.set_hint(hint);
        }
        report
    }
}
