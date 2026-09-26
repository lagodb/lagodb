//! Runtime-owned `ProcessUtility_hook` router.

mod analyze_router;
mod consumer;
mod copy_route;
mod maintenance_command;
mod provider_maintenance;
mod tablespace_policy;
mod truncate_router;
mod vacuum_router;

use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use crate::descriptor_registry::{
    DescriptorNode, DescriptorRegistry, DescriptorSnapshot,
};
use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::runtime_api::UtilityHookDescriptor;
use lagodb_core::table_maintenance::TableMaintenanceRouter;
use pgrx::{pg_guard, pg_sys};

use crate::worker;
use maintenance_command::MaintenanceCommandScope;
use tablespace_policy::StorageVolumeTablespacePolicy;

pub(crate) use consumer::{
    PreparedUtilityConsumers, commit_consumers, prepare_consumers,
};

type UtilityHookRegistry = DescriptorRegistry<UtilityHookDescriptor>;

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

pub(crate) struct PreparedUtilityHooks {
    // Each node is allocated before the runtime registration transaction is
    // committed. The box keeps its address stable while the registry stores
    // a raw backend-lifetime pointer; commit therefore cannot allocate or
    // publish only part of this prepared batch.
    #[allow(clippy::vec_box)]
    nodes: Vec<Box<DescriptorNode<UtilityHookDescriptor>>>,
}

#[derive(Clone, Copy)]
struct UtilityHookSnapshot {
    descriptors: DescriptorSnapshot<UtilityHookDescriptor>,
    tag: u32,
}

impl UtilityHookSnapshot {
    fn new(
        descriptors: DescriptorSnapshot<UtilityHookDescriptor>,
        tag: pg_sys::NodeTag,
    ) -> Self {
        Self {
            descriptors,
            tag: tag as u32,
        }
    }

    fn has_matching_hooks(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    fn for_each(self, mut callback: impl FnMut(UtilityHookDescriptor)) {
        self.descriptors.for_each(|descriptor| {
            if descriptor.tag == self.tag {
                callback(descriptor);
            }
        });
    }
}

thread_local! {
    static UTILITY_HOOKS: UtilityHookRegistry = const { DescriptorRegistry::new() };
}

fn valid_descriptor(descriptor: &UtilityHookDescriptor) -> bool {
    descriptor.struct_size == std::mem::size_of::<UtilityHookDescriptor>() as u32
        && !descriptor.context.is_null()
        && descriptor.on_pre.is_some()
        && descriptor.on_post.is_some()
}

pub(crate) fn prepare_hooks(
    descriptors: &[UtilityHookDescriptor],
) -> Option<PreparedUtilityHooks> {
    if !descriptors.iter().all(valid_descriptor) {
        return None;
    }
    Some(PreparedUtilityHooks {
        nodes: descriptors
            .iter()
            .copied()
            .map(DescriptorNode::new)
            .collect(),
    })
}

pub(crate) fn commit_hooks(prepared: PreparedUtilityHooks) {
    UTILITY_HOOKS.with(|registry| {
        let _ = registry.commit(prepared.nodes);
    });
}

#[cfg(test)]
pub(crate) fn registered_hook_count() -> usize {
    UTILITY_HOOKS.with(|registry| {
        let mut count = 0;
        registry.snapshot().for_each(|_| count += 1);
        count
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C-unwind" fn pre(
        _context: *mut c_void,
        _planned_stmt: *mut pg_sys::PlannedStmt,
        _query_string: *const c_char,
    ) {
    }
    unsafe extern "C-unwind" fn post(
        _context: *mut c_void,
        _node: *mut pg_sys::Node,
    ) {
    }

    fn descriptor(tag: pg_sys::NodeTag) -> UtilityHookDescriptor {
        UtilityHookDescriptor {
            struct_size: std::mem::size_of::<UtilityHookDescriptor>() as u32,
            tag: tag as u32,
            context: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            on_pre: Some(pre),
            on_post: Some(post),
        }
    }

    #[test]
    fn descriptor_validation_rejects_invalid_size_and_callbacks() {
        let mut candidate = descriptor(pg_sys::NodeTag::T_CommentStmt);
        assert!(valid_descriptor(&candidate));

        candidate.struct_size = 0;
        assert!(!valid_descriptor(&candidate));
        candidate.struct_size =
            std::mem::size_of::<UtilityHookDescriptor>() as u32 + 1;
        assert!(!valid_descriptor(&candidate));
        candidate.struct_size = std::mem::size_of::<UtilityHookDescriptor>() as u32;
        candidate.on_post = None;
        assert!(!valid_descriptor(&candidate));
    }

    #[test]
    fn snapshot_excludes_descriptors_appended_later() {
        let registry = UtilityHookRegistry::new();
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));
        let snapshot = UtilityHookSnapshot::new(
            registry.snapshot(),
            pg_sys::NodeTag::T_CommentStmt,
        );
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        let mut count = 0;
        snapshot.for_each(|_| count += 1);
        assert_eq!(count, 1);
        assert!(snapshot.has_matching_hooks());
    }

    #[test]
    fn snapshot_filters_by_node_tag() {
        let registry = UtilityHookRegistry::new();
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        assert!(
            UtilityHookSnapshot::new(
                registry.snapshot(),
                pg_sys::NodeTag::T_CommentStmt,
            )
            .has_matching_hooks()
        );
        assert!(
            !UtilityHookSnapshot::new(
                registry.snapshot(),
                pg_sys::NodeTag::T_CreateStmt,
            )
            .has_matching_hooks()
        );
    }

    #[test]
    fn snapshot_runs_matching_hooks_in_fifo_registration_order() {
        let registry = UtilityHookRegistry::new();
        let mut first_context = 1_u8;
        let mut second_context = 2_u8;
        let mut first = descriptor(pg_sys::NodeTag::T_CommentStmt);
        first.context = std::ptr::from_mut(&mut first_context).cast();
        let mut second = descriptor(pg_sys::NodeTag::T_CommentStmt);
        second.context = std::ptr::from_mut(&mut second_context).cast();
        registry.append(first);
        registry.append(second);

        let mut order = Vec::new();
        UtilityHookSnapshot::new(registry.snapshot(), pg_sys::NodeTag::T_CommentStmt)
            .for_each(|descriptor| order.push(descriptor.context));

        assert_eq!(order, vec![first.context, second.context]);
    }
}

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

// Shared with LagodbMaintenanceUtilityArgs in the runtime C command bridge.
#[repr(C)]
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
        unsafe {
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
        }
    }

    unsafe fn call_parent(self) {
        match PREV_PROCESS_UTILITY.get() {
            Some(Some(previous)) => unsafe { self.call_previous(*previous) },
            _ => unsafe { self.call_standard() },
        }
    }

    unsafe fn check_maintenance_recursion(self) {
        // SAFETY: the caller supplies live ProcessUtility arguments.
        let target_node = unsafe { self.target_node() };
        if unsafe { (*target_node).type_ } == pg_sys::NodeTag::T_VacuumStmt {
            unsafe {
                MaintenanceCommandScope::check_recursion(
                    target_node.cast::<pg_sys::VacuumStmt>(),
                );
            }
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
        // Reject a recursive maintenance command before any LagoDB pre-hook
        // can perform catalog work or rewrite its utility node.
        args.check_maintenance_recursion();
        let target_node = args.target_node();

        // Lifecycle preflight is deliberately first and is a no-op unless the
        // runtime was initialized from shared_preload_libraries.
        worker::preflight(target_node);

        let tag = (*target_node).type_;
        let hooks = UTILITY_HOOKS
            .with(|registry| UtilityHookSnapshot::new(registry.snapshot(), tag));
        let has_matching_hooks = hooks.has_matching_hooks();
        let has_matching_consumers = consumer::has_registered_consumer(tag);
        let mut tablespace_policy = StorageVolumeTablespacePolicy::for_tag(tag);
        let has_tablespace_policy = tablespace_policy.is_some();
        let copy_from_route = tag == pg_sys::NodeTag::T_CopyStmt
            && (*target_node.cast::<pg_sys::CopyStmt>()).is_from;
        let may_consume_vacuum = tag == pg_sys::NodeTag::T_VacuumStmt
            && TableMaintenanceRouter::has_providers();
        let may_consume_truncate = tag == pg_sys::NodeTag::T_TruncateStmt
            && crate::table_provider_registry::has_partitioned_table_provider();

        if !has_matching_hooks
            && !has_matching_consumers
            && !has_tablespace_policy
            && !may_consume_vacuum
            && !may_consume_truncate
            && !copy_from_route
        {
            if tag == pg_sys::NodeTag::T_CopyStmt {
                // SAFETY: `tag` proves that `target_node` is the live CopyStmt
                // for this ProcessUtility invocation.
                if let Some(error) = copy_route::unclaimed_uri_error(target_node) {
                    error.report();
                }
            }
            if tag == pg_sys::NodeTag::T_VacuumStmt {
                MaintenanceCommandScope::execute(args);
            } else {
                args.call_parent();
            }
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

        hooks.for_each(|descriptor| {
            descriptor.on_pre.expect("validated utility pre-hook")(
                descriptor.context,
                args.pstmt,
                args.query_string,
            );
        });

        // A pre-hook can replace the utility node. Check the final node as a
        // separate boundary invariant so a non-maintenance input cannot be
        // rewritten into a recursive VACUUM/ANALYZE.
        args.check_maintenance_recursion();

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
        let is_maintenance = final_tag == pg_sys::NodeTag::T_VacuumStmt;
        let maintenance_consumed = if is_maintenance {
            MaintenanceCommandScope::execute(args)
        } else {
            false
        };
        let truncate_consumed = if final_tag == pg_sys::NodeTag::T_TruncateStmt {
            truncate_router::try_route(target_node.cast::<pg_sys::TruncateStmt>())
                .report_unwrap()
        } else {
            false
        };
        let consumed = consumer_consumed || maintenance_consumed || truncate_consumed;
        // Maintenance's C scope already performed parent fallback while its
        // recursion state was active. Other utility routes still delegate here.
        if !consumed && !is_maintenance {
            args.call_parent();
        } else if consumed {
            // standard_ProcessUtility() performs this once after the command
            // implementation returns. A consumer that replaces standard must
            // preserve the same visibility boundary before post-hooks run.
            pg_sys::CommandCounterIncrement();
        }

        if let Some(original_node) = original_node {
            hooks.for_each(|descriptor| {
                descriptor.on_post.expect("validated utility post-hook")(
                    descriptor.context,
                    original_node,
                );
            });
        }
        if let Some(policy) = tablespace_policy.as_mut() {
            policy
                .complete()
                .map_err(PgReportError::from_domain_error)
                .report_unwrap();
        }
    }
}
