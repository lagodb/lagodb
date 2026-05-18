use crate::diag::SqlStateError;
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

/// Error type for PostgreSQL hook implementations.
///
/// Hook implementations can use `?` with errors implementing [`SqlStateError`].
/// The router adds hook-specific context at the PostgreSQL boundary.
#[derive(Debug)]
pub struct HookError {
    sqlerrcode: PgSqlErrorCode,
    message: String,
    context: Option<HookErrorContext>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::with_code(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
    }

    pub fn with_code(sqlerrcode: PgSqlErrorCode, message: impl Into<String>) -> Self {
        Self {
            sqlerrcode,
            message: message.into(),
            context: None,
            source: None,
        }
    }

    pub fn with_source<E>(sqlerrcode: PgSqlErrorCode, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let message = source.to_string();
        Self {
            sqlerrcode,
            message,
            context: None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn with_utility_context(
        mut self,
        hook_name: &'static str,
        phase: UtilityHookPhase,
        node_tag: pg_sys::NodeTag,
    ) -> Self {
        self.context = Some(HookErrorContext::Utility {
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
        self.context = Some(HookErrorContext::ObjectAccess {
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
        self.context = Some(HookErrorContext::ObjectAccessStr {
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

impl From<HookError> for ErrorReport {
    fn from(value: HookError) -> Self {
        let report = ErrorReport::new(value.sqlerrcode, value.message, "");
        match value.context {
            Some(context) => report.set_detail(context.detail()),
            None => report,
        }
    }
}

pub type UtilityHookError = HookError;
pub type ObjectAccessHookError = HookError;
