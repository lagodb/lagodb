use crate::diag::{ReportableError, SqlStateError};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone, Copy)]
enum UtilityHookPhase {
    Pre,
    Post,
}

impl UtilityHookPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pre => "pre",
            Self::Post => "post",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UtilityHookErrorContext {
    hook_name: &'static str,
    phase: UtilityHookPhase,
    node_tag: pg_sys::NodeTag,
}

impl UtilityHookErrorContext {
    fn detail(self) -> String {
        format!(
            "while running utility hook '{}' in {} phase for {:?}",
            self.hook_name,
            self.phase.as_str(),
            self.node_tag
        )
    }
}

/// Error type for utility hooks.
///
/// Hook implementations should be able to use `?` without attaching ad-hoc
/// string prefixes. The router adds hook name, phase, and node tag at the
/// boundary while this type preserves the SQLSTATE chosen by the domain error.
#[derive(Debug)]
pub struct UtilityHookError {
    sqlerrcode: PgSqlErrorCode,
    message: String,
    context: Option<UtilityHookErrorContext>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for UtilityHookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for UtilityHookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl UtilityHookError {
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

    fn with_context(
        mut self,
        hook_name: &'static str,
        phase: UtilityHookPhase,
        tag: pg_sys::NodeTag,
    ) -> Self {
        self.context = Some(UtilityHookErrorContext {
            hook_name,
            phase,
            node_tag: tag,
        });
        self
    }
}

impl<E> From<E> for UtilityHookError
where
    E: SqlStateError,
{
    fn from(value: E) -> Self {
        Self::with_source(value.sql_error_code(), value)
    }
}

impl From<UtilityHookError> for ErrorReport {
    fn from(value: UtilityHookError) -> Self {
        let report = ErrorReport::new(value.sqlerrcode, value.message, "");
        match value.context {
            Some(context) => report.set_detail(context.detail()),
            None => report,
        }
    }
}

/// A safe wrapper around `pg_sys::Node`
pub struct UtilityNode<'a> {
    ptr: *mut pg_sys::Node,
    _marker: PhantomData<&'a mut pg_sys::Node>,
}

impl<'a> UtilityNode<'a> {
    pub unsafe fn new(ptr: *mut pg_sys::Node) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    pub fn is_a<T>(&self, tag: pg_sys::NodeTag) -> Option<&T> {
        unsafe { ((*self.ptr).type_ == tag).then(|| &*(self.ptr as *const T)) }
    }

    pub fn is_a_mut<T>(&mut self, tag: pg_sys::NodeTag) -> Option<&mut T> {
        unsafe { ((*self.ptr).type_ == tag).then(|| &mut *(self.ptr as *mut T)) }
    }
}

pub trait UtilityHook {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError>;
    fn on_post(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError>;
}

static REGISTRY: RwLock<Vec<(pg_sys::NodeTag, Arc<dyn UtilityHook + Send + Sync>)>> =
    RwLock::new(Vec::new());

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

pub fn register_utility_hook(
    tag: pg_sys::NodeTag,
    hook: Box<dyn UtilityHook + Send + Sync>,
) {
    REGISTRY.write().unwrap().push((tag, Arc::from(hook)));

    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        prev
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn process_utility_router(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::os::raw::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) {
    unsafe {
        let target_node = (*pstmt).utilityStmt;
        let tag = (*target_node).type_;

        // Copy the original statement before on_pre might modify it.
        // copyObjectImpl allocates in CurrentMemoryContext, so PostgreSQL will manage its lifetime.
        let copied_node =
            pg_sys::copyObjectImpl(target_node as *const std::ffi::c_void)
                as *mut pg_sys::Node;

        let mut safe_node = UtilityNode::new(target_node);
        let mut safe_node_copy = UtilityNode::new(copied_node);

        // Pre-hooks
        let hooks = { REGISTRY.read().unwrap().clone() };
        for (reg_tag, hook) in hooks.iter() {
            if *reg_tag == tag {
                hook.on_pre(&mut safe_node)
                    .map_err(|err| {
                        err.with_context(hook.name(), UtilityHookPhase::Pre, tag)
                    })
                    .report_unwrap();
            }
        }

        // TODO: Consider wrapping these PG calls with PgTryBuilder to safely handle
        // longjmp (elog(ERROR)) and ensure Rust destructors (like safe_node_copy) are called properly.
        // Currently, if standard_ProcessUtility errors, Rust stack unwinding might be bypassed.
        match PREV_PROCESS_UTILITY.get() {
            Some(Some(prev)) => {
                prev(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    completion_tag,
                );
            }
            _ => {
                pg_sys::standard_ProcessUtility(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    completion_tag,
                );
            }
        }

        for (reg_tag, hook) in hooks.iter() {
            if *reg_tag == tag {
                hook.on_post(&mut safe_node_copy)
                    .map_err(|err| {
                        err.with_context(hook.name(), UtilityHookPhase::Post, tag)
                    })
                    .report_unwrap();
            }
        }
    }
}
