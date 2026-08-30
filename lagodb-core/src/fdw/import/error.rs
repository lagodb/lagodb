//! Error conversion for the `IMPORT FOREIGN SCHEMA` FFI boundary.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};

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
    #[error("{report}")]
    PgReport { report: PgReportParts },
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

    pub(crate) fn report(self) -> ! {
        PgReportError::raise(ErrorReport::from(self))
    }

    fn append_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        match &*self.0 {
            ForeignImportErrorKind::Callback { source, .. } => {
                source.append_pg_report_extras(details, hints);
            }
            ForeignImportErrorKind::PgReport { report } => {
                if let Some(detail) = report.detail.clone() {
                    details.push(detail);
                }
                if let Some(hint) = report.hint.clone() {
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
            ForeignImportErrorKind::Provider { sqlerrcode, .. } => *sqlerrcode,
            ForeignImportErrorKind::PgReport { report } => report.sqlerrcode,
        }
    }
}

impl From<PgReportError> for ForeignImportError {
    fn from(error: PgReportError) -> Self {
        Self::new(ForeignImportErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl PgReportableError for ForeignImportError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<ForeignImportError> for ErrorReport {
    fn from(error: ForeignImportError) -> Self {
        error.into_error_report()
    }
}
