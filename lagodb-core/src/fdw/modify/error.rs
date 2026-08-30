//! Errors for the FDW modify callback boundary.

use core::ffi::CStr;
use std::error::Error as StdError;
use std::fmt::Display;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use super::contract::{FdwModify, ForeignModifyOperation};
use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};
use crate::plan_data::PlanDataError;

/// PostgreSQL modify callback phase attached at the FFI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignModifyPhase {
    Capabilities,
    AddUpdateTargets,
    Plan,
    Begin,
    BeginInsert,
    Insert,
    BatchInsert,
    Update,
    Delete,
    End,
    EndInsert,
}

impl ForeignModifyPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Capabilities => "IsForeignRelUpdatable",
            Self::AddUpdateTargets => "AddForeignUpdateTargets",
            Self::Plan => "PlanForeignModify",
            Self::Begin => "BeginForeignModify",
            Self::BeginInsert => "BeginForeignInsert",
            Self::Insert => "ExecForeignInsert",
            Self::BatchInsert => "ExecForeignBatchInsert",
            Self::Update => "ExecForeignUpdate",
            Self::Delete => "ExecForeignDelete",
            Self::End => "EndForeignModify",
            Self::EndInsert => "EndForeignInsert",
        }
    }
}

/// Framework error for foreign INSERT/UPDATE/DELETE planning and execution.
#[derive(Debug)]
pub struct ForeignModifyError(Box<ForeignModifyErrorKind>);

#[derive(Debug, Error)]
enum ForeignModifyErrorKind {
    #[error(
        "FDW provider {provider:?} callback {phase} failed: {source}",
        phase = phase.as_str()
    )]
    Runtime {
        provider: &'static CStr,
        phase: ForeignModifyPhase,
        #[source]
        source: Box<ForeignModifyError>,
    },
    #[error("FDW provider modify error: {source}")]
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
    #[error("FDW modify private-data codec error: {source}")]
    PrivateCodec {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{report}")]
    PgReport { report: PgReportParts },
}

impl Display for ForeignModifyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.0, formatter)
    }
}

impl StdError for ForeignModifyError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl ForeignModifyError {
    fn new(kind: ForeignModifyErrorKind) -> Self {
        Self(Box::new(kind))
    }

    pub fn provider<E>(error: E) -> Self
    where
        E: SqlStateError + StdError + Send + Sync + 'static,
    {
        Self::new(ForeignModifyErrorKind::Provider {
            sqlerrcode: error.sql_error_code(),
            source: Box::new(error),
        })
    }

    pub(crate) fn framework(message: impl Display) -> Self {
        Self::new(ForeignModifyErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            message: message.to_string(),
        })
    }

    pub fn unsupported(message: impl Display) -> Self {
        Self::new(ForeignModifyErrorKind::Framework {
            sqlerrcode: PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            message: message.to_string(),
        })
    }

    pub(crate) fn self_modified(operation: ForeignModifyOperation) -> Self {
        let action = match operation {
            ForeignModifyOperation::Update => "updated",
            ForeignModifyOperation::Delete => "deleted",
            ForeignModifyOperation::Insert => {
                return Self::framework(
                    "foreign INSERT provider returned a self-modified outcome",
                );
            }
        };
        Self::new(ForeignModifyErrorKind::PgReport {
            report: PgReportParts::new(
                PgSqlErrorCode::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION,
                format!(
                    "tuple to be {action} was already modified by an operation triggered by the current command"
                ),
                None,
                Some(
                    "Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows."
                        .to_owned(),
                ),
            ),
        })
    }

    pub(crate) fn with_provider_phase<P: FdwModify>(
        self,
        phase: ForeignModifyPhase,
    ) -> Self {
        if matches!(&*self.0, ForeignModifyErrorKind::Runtime { .. }) {
            return self;
        }
        Self::new(ForeignModifyErrorKind::Runtime {
            provider: P::NAME,
            phase,
            source: Box::new(self),
        })
    }

    pub(crate) fn report(self) -> ! {
        PgReportError::raise(ErrorReport::from(self))
    }
}

impl SqlStateError for ForeignModifyError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            ForeignModifyErrorKind::Runtime { source, .. } => source.sql_error_code(),
            ForeignModifyErrorKind::Provider { sqlerrcode, .. }
            | ForeignModifyErrorKind::Framework { sqlerrcode, .. } => *sqlerrcode,
            ForeignModifyErrorKind::PgReport { report } => report.sqlerrcode,
            ForeignModifyErrorKind::PrivateCodec { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<PlanDataError> for ForeignModifyError {
    fn from(error: PlanDataError) -> Self {
        Self::new(ForeignModifyErrorKind::PrivateCodec {
            source: Box::new(error),
        })
    }
}

impl From<PgReportError> for ForeignModifyError {
    fn from(error: PgReportError) -> Self {
        Self::new(ForeignModifyErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(error),
        })
    }
}

impl PgReportableError for ForeignModifyError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_pg_report_extras(details, hints);
    }
}

impl From<ForeignModifyError> for ErrorReport {
    fn from(error: ForeignModifyError) -> Self {
        error.into_error_report()
    }
}

impl ForeignModifyError {
    fn append_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        match &*self.0 {
            ForeignModifyErrorKind::Runtime { source, .. } => {
                source.append_pg_report_extras(details, hints)
            }
            ForeignModifyErrorKind::PgReport { report } => {
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
