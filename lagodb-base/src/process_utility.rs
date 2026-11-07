//! Runtime-owned `ProcessUtility_hook` router.

mod consumer;
mod copy_route;
mod hooks;
mod tablespace_policy;
mod vacuum_full;

use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use lagodb_core::diag::{PgReportError, ReportableError};
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{pg_guard, pg_sys};

use crate::table_provider_registry::TableProviderRegistry;
use crate::worker;
use tablespace_policy::StorageVolumeTablespacePolicy;

pub(crate) use consumer::PreparedUtilityConsumers;
pub(crate) use hooks::PreparedUtilityHooks;
#[cfg(test)]
pub(crate) use hooks::registered_hook_count;

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

type ProcessUtilityHookFn = unsafe extern "C-unwind" fn(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
);

#[derive(Clone, Copy)]
pub(super) struct ProcessUtilityArgs {
    pub(crate) pstmt: *mut pg_sys::PlannedStmt,
    pub(crate) query_string: *const c_char,
    pub(crate) read_only_tree: bool,
    pub(crate) context: pg_sys::ProcessUtilityContext::Type,
    pub(crate) params: *mut pg_sys::ParamListInfoData,
    pub(crate) query_env: *mut pg_sys::QueryEnvironment,
    pub(crate) dest: *mut pg_sys::DestReceiver,
    pub(crate) completion_tag: *mut pg_sys::QueryCompletion,
}

impl ProcessUtilityArgs {
    unsafe fn target_node(self) -> *mut pg_sys::Node {
        unsafe { (*self.pstmt).utilityStmt }
    }

    /// Save the original statement through all post-hooks. Ordinary commands
    /// and nested ANALYZE use the caller's context. Top-level maintenance uses
    /// a separate Portal child, reclaimed by PostgreSQL after ProcessUtility
    /// returns, independently of the shorter-lived maintenance plan.
    ///
    /// # Safety
    /// These arguments must describe the live backend ProcessUtility call.
    /// The caller's context must remain live through post-hooks; top-level
    /// maintenance must run inside a live Portal.
    unsafe fn copy_original_for_hooks(self) -> *mut pg_sys::Node {
        // SAFETY: the arguments supply the live statement and execution context.
        unsafe {
            let node = self.target_node();
            let is_top_level = self.context
                == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL;
            let previous =
                if (*node).type_ == pg_sys::NodeTag::T_VacuumStmt && is_top_level {
                    let snapshot_context = pg_sys::AllocSetContextCreateExtended(
                        pg_sys::PortalContext,
                        c"lagodb utility hook snapshot".as_ptr(),
                        pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                        pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                        pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
                    );
                    Some(pg_sys::MemoryContextSwitchTo(snapshot_context))
                } else {
                    None
                };
            let copied =
                pg_sys::copyObjectImpl(node.cast::<c_void>()).cast::<pg_sys::Node>();
            if let Some(previous) = previous {
                pg_sys::MemoryContextSwitchTo(previous);
            }
            copied
        }
    }

    /// Give a utility callback the same writable-tree guarantee as
    /// PostgreSQL's `standard_ProcessUtility` path.
    ///
    /// `read_only_tree` means the `PlannedStmt` belongs to a caller that may
    /// reuse it. A pre-hook or consumer can transform expressions or analyze
    /// a raw query, so it must not mutate that original tree. Callers invoke
    /// this only after routing establishes that a mutable callback will run.
    unsafe fn writable_copy(self) -> Self {
        if !self.read_only_tree {
            return self;
        }
        let pstmt = unsafe {
            pg_sys::copyObjectImpl(self.pstmt.cast::<c_void>())
                .cast::<pg_sys::PlannedStmt>()
        };
        Self {
            pstmt,
            read_only_tree: false,
            ..self
        }
    }

    /// Arguments that run `utility` in place of the original statement. The
    /// copy lives in the Portal because PostgreSQL's maintenance commands
    /// change transactions while it is in use, and it leaves a tree that may
    /// be shared with a cached plan untouched.
    ///
    /// # Safety
    /// These arguments must describe a live top-level ProcessUtility call
    /// inside a Portal; `utility` must outlive the returned arguments' use.
    unsafe fn with_utility(self, utility: *mut pg_sys::Node) -> Self {
        // SAFETY: the Portal outlives this command; PostgreSQL restores its
        // own context after ProcessUtility returns.
        unsafe {
            let previous = pg_sys::MemoryContextSwitchTo(pg_sys::PortalContext);
            let pstmt = pg_sys::copyObjectImpl(self.pstmt.cast::<c_void>())
                .cast::<pg_sys::PlannedStmt>();
            pg_sys::MemoryContextSwitchTo(previous);
            (*pstmt).utilityStmt = utility;
            Self {
                pstmt,
                read_only_tree: false,
                ..self
            }
        }
    }

    unsafe fn call_standard(self) {
        unsafe {
            pg_sys::standard_ProcessUtility(
                self.pstmt,
                self.query_string,
                self.read_only_tree,
                self.context,
                self.params,
                self.query_env,
                self.dest,
                self.completion_tag,
            );
        }
    }

    unsafe fn call_previous(self, previous: ProcessUtilityHookFn) {
        // SAFETY: the guard turns a parent ERROR into an unwind, so Rust state
        // held across this call drops; the closure only calls the parent hook.
        unsafe {
            pg_guard_ffi_boundary(|| {
                previous(
                    self.pstmt,
                    self.query_string,
                    self.read_only_tree,
                    self.context,
                    self.params,
                    self.query_env,
                    self.dest,
                    self.completion_tag,
                );
            });
        }
    }

    unsafe fn call_parent(self) {
        match PREV_PROCESS_UTILITY.get() {
            Some(Some(previous)) => unsafe { self.call_previous(*previous) },
            _ => unsafe { self.call_standard() },
        }
    }
}

pub(crate) fn init() {
    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let previous = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        previous
    });
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn process_utility_router(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) {
    unsafe {
        let args = ProcessUtilityArgs {
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        };
        let target_node = args.target_node();

        // Lifecycle preflight is deliberately first.
        worker::preflight(target_node);

        let tag = (*target_node).type_;
        let utility_hooks = hooks::snapshot(tag);
        let has_matching_hooks = utility_hooks.has_matching_hooks();
        let has_matching_consumers = consumer::has_registered_consumer(tag);
        let mut tablespace_policy = StorageVolumeTablespacePolicy::for_tag(tag);
        let has_tablespace_policy = tablespace_policy.is_some();
        let copy_from_route = tag == pg_sys::NodeTag::T_CopyStmt
            && (*target_node.cast::<pg_sys::CopyStmt>()).is_from;
        let may_consume_vacuum = tag == pg_sys::NodeTag::T_VacuumStmt
            && TableProviderRegistry::has_available_provider();

        if !has_matching_hooks
            && !has_matching_consumers
            && !has_tablespace_policy
            && !may_consume_vacuum
            && !copy_from_route
        {
            if tag == pg_sys::NodeTag::T_CopyStmt {
                // SAFETY: `tag` proves that `target_node` is the live CopyStmt
                // for this ProcessUtility invocation.
                if let Some(error) = copy_route::unclaimed_uri_error(target_node) {
                    error.report();
                }
            }
            args.call_parent();
            return;
        }

        let original_node =
            has_matching_hooks.then(|| args.copy_original_for_hooks());

        let args = if read_only_tree && (has_matching_hooks || has_tablespace_policy)
        {
            args.writable_copy()
        } else {
            args
        };
        let target_node = args.target_node();

        if let Some(policy) = tablespace_policy.as_mut() {
            policy
                .prepare(
                    target_node,
                    context
                        == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL,
                )
                .map_err(PgReportError::from_domain_error)
                .report_unwrap();
        }

        utility_hooks.for_each(|descriptor| {
            descriptor.on_pre.expect("validated utility pre-hook")(
                descriptor.context,
                args.pstmt,
                args.query_string,
            );
        });

        // A pre-hook may rewrite a non-runtime-owned utility node. Native routes,
        // lifecycle, and consumer selection must therefore use the final utility
        // tag and COPY direction, not the pre-hook input. Hooks for a
        // runtime-owned policy are validation-only and preserve its tag and
        // identity.
        let target_node = args.target_node();
        let final_tag = (*target_node).type_;
        let selected_consumer = consumer::select(final_tag, args).report_unwrap();
        let consumer_consumed = selected_consumer.is_some();
        if final_tag == pg_sys::NodeTag::T_CopyStmt && !consumer_consumed {
            // SAFETY: `final_tag` proves that the current utility node is the
            // live PostgreSQL CopyStmt supplied to this invocation.
            if let Some(error) = copy_route::unclaimed_uri_error(target_node) {
                error.report();
            }
        }
        if let Some(consumer) = selected_consumer {
            let consumer_args = args.writable_copy();
            // SAFETY: the selected descriptor was validated at registration,
            // and the callback arguments remain live for this invocation.
            consumer.consume(consumer_args).report_unwrap();
        }
        let copy_consumed =
            if final_tag == pg_sys::NodeTag::T_CopyStmt && !consumer_consumed {
                copy_route::try_route(args).report_unwrap()
            } else {
                false
            };
        let maintenance_consumed =
            if final_tag == pg_sys::NodeTag::T_VacuumStmt && !consumer_consumed {
                vacuum_full::try_route(args).report_unwrap()
            } else {
                false
            };
        let consumed = consumer_consumed || copy_consumed || maintenance_consumed;
        // Intentionally do not delegate consumed COPY commands to the captured
        // parent ProcessUtility hook: the format consumer or shared AM fallback
        // has already executed them, and delegation would execute them again.
        // Downstream hooks therefore do not observe these commands; outer hooks
        // that called this router can still observe them. This is an accepted
        // extension-composition boundary, including ordinary Iceberg COPY FROM.
        // Unclaimed native COPY retains parent delegation, and LagoDB's own
        // post-hooks still run below for successfully consumed commands.
        // A consumed VACUUM FULL has already delegated its native relations and
        // ANALYZE phase to the parent, as separate statements.
        if !consumed {
            args.call_parent();
        } else {
            // standard_ProcessUtility() performs this once after the command
            // implementation returns. A consumer that replaces standard must
            // preserve the same visibility boundary before post-hooks run.
            pg_sys::CommandCounterIncrement();
        }

        if let Some(original_node) = original_node {
            utility_hooks.for_each(|descriptor| {
                descriptor.on_post.expect("validated utility post-hook")(
                    descriptor.context,
                    original_node,
                );
            });
        }
    }
}
