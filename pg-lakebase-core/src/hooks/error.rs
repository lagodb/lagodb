use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

#[derive(Debug, Clone, Copy)]
pub(crate) enum UtilityHookPhase {
    Pre,
    PostSuccess,
}

impl UtilityHookPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pre => "pre",
            Self::PostSuccess => "post-success",
        }
    }
}

#[derive(Debug)]
enum HookErrorContext {
    Utility {
        hook_name: &'static str,
        phase: UtilityHookPhase,
        node_tag: pg_sys::NodeTag,
    },
    ObjectAccess {
        hook_name: &'static str,
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: Option<pg_sys::Oid>,
        sub_id: i32,
    },
    ObjectAccessStr {
        hook_name: &'static str,
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_name: Option<String>,
        sub_id: i32,
    },
}

impl HookErrorContext {
    fn detail(&self) -> String {
        match self {
            Self::Utility {
                hook_name,
                phase,
                node_tag,
            } => format!(
                "while running utility hook '{}' in {} phase for {:?}",
                hook_name,
                phase.as_str(),
                node_tag
            ),
            Self::ObjectAccess {
                hook_name,
                access,
                class_id,
                object_id,
                sub_id,
            } => format!(
                "while running object access hook '{}' for access={} class_id={} object_id={} sub_id={}",
                hook_name,
                access,
                class_id,
                object_id
                    .map(|oid| oid.to_string())
                    .unwrap_or_else(|| "<none>".to_string()),
                sub_id
            ),
            Self::ObjectAccessStr {
                hook_name,
                access,
                class_id,
                object_name,
                sub_id,
            } => format!(
                "while running string object access hook '{}' for access={} class_id={} object_name={} sub_id={}",
                hook_name,
                access,
                class_id,
                object_name.as_deref().unwrap_or("<none>"),
                sub_id
            ),
        }
    }
}

#[derive(Debug)]
struct HookErrorInner {
    sqlerrcode: PgSqlErrorCode,
    message: String,
    context: Option<HookErrorContext>,
    postgres_detail: Option<String>,
    postgres_hint: Option<String>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Error type for PostgreSQL hook implementations.
///
/// Hook implementations can use `?` with errors implementing [`SqlStateError`].
/// The router adds hook-specific context at the PostgreSQL boundary.
///
/// Payload fields are boxed so public `Result<_, HookError>` callbacks keep a
/// small `Err` variant, matching [`crate::diag::PgReportError`].
#[derive(Debug)]
pub struct HookError(Box<HookErrorInner>);

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.message)
    }
}

impl std::error::Error for HookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::with_code(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
    }

    pub fn with_code(sqlerrcode: PgSqlErrorCode, message: impl Into<String>) -> Self {
        Self(Box::new(HookErrorInner {
            sqlerrcode,
            message: message.into(),
            context: None,
            postgres_detail: None,
            postgres_hint: None,
            source: None,
        }))
    }

    pub fn with_source<E>(sqlerrcode: PgSqlErrorCode, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let message = source.to_string();
        Self(Box::new(HookErrorInner {
            sqlerrcode,
            message,
            context: None,
            postgres_detail: None,
            postgres_hint: None,
            source: Some(Box::new(source)),
        }))
    }

    pub(crate) fn with_utility_context(
        mut self,
        hook_name: &'static str,
        phase: UtilityHookPhase,
        node_tag: pg_sys::NodeTag,
    ) -> Self {
        self.0.context = Some(HookErrorContext::Utility {
            hook_name,
            phase,
            node_tag,
        });
        self
    }

    pub(crate) fn with_object_access_context(
        mut self,
        hook_name: &'static str,
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: Option<pg_sys::Oid>,
        sub_id: i32,
    ) -> Self {
        self.0.context = Some(HookErrorContext::ObjectAccess {
            hook_name,
            access,
            class_id,
            object_id,
            sub_id,
        });
        self
    }

    pub(crate) fn with_object_access_str_context(
        mut self,
        hook_name: &'static str,
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_name: Option<String>,
        sub_id: i32,
    ) -> Self {
        self.0.context = Some(HookErrorContext::ObjectAccessStr {
            hook_name,
            access,
            class_id,
            object_name,
            sub_id,
        });
        self
    }
}

impl<E> From<E> for HookError
where
    E: SqlStateError,
{
    fn from(value: E) -> Self {
        Self::with_source(value.sql_error_code(), value)
    }
}

impl From<PgReportError> for HookError {
    fn from(err: PgReportError) -> Self {
        let sqlerrcode = err.sql_error_code();
        let report = err.into_report();
        Self(Box::new(HookErrorInner {
            sqlerrcode,
            message: report.message().to_string(),
            context: None,
            postgres_detail: report.detail().map(str::to_owned),
            postgres_hint: report.hint().map(str::to_owned),
            source: None,
        }))
    }
}

impl From<HookError> for ErrorReport {
    fn from(value: HookError) -> Self {
        let HookErrorInner {
            sqlerrcode,
            message,
            context,
            postgres_detail,
            postgres_hint,
            source,
        } = *value.0;

        let context_detail = context.as_ref().map(HookErrorContext::detail);
        let source_detail = source.as_deref().and_then(|source| {
            let err: &(dyn std::error::Error + Send + Sync) = source;
            error_source_chain_detail(err)
        });
        let detail =
            join_error_details([context_detail, postgres_detail, source_detail]);
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

pub type UtilityHookError = HookError;
pub type ObjectAccessHookError = HookError;
