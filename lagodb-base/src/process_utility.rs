//! Runtime-owned `ProcessUtility_hook` router.

mod copy_route;
mod full_router;

use std::cell::Cell;
use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use lagodb_core::diag::ReportableError;
use lagodb_core::runtime_api::UtilityHookDescriptor;
use pgrx::{pg_guard, pg_sys};

use crate::hooks;

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

struct UtilityHookNode {
    descriptor: UtilityHookDescriptor,
    next: Cell<*const UtilityHookNode>,
}

struct UtilityHookDirectory {
    head: Cell<*const UtilityHookNode>,
    tail: Cell<*const UtilityHookNode>,
}

impl UtilityHookDirectory {
    const fn new() -> Self {
        Self {
            head: Cell::new(std::ptr::null()),
            tail: Cell::new(std::ptr::null()),
        }
    }

    fn append_node(&self, node: Box<UtilityHookNode>) {
        let node = Box::into_raw(node);
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: tail is a backend-lifetime node previously published by
            // this single-threaded backend directory.
            unsafe { (*tail).next.set(node) };
        }
    }

    #[cfg(test)]
    fn append(&self, descriptor: UtilityHookDescriptor) {
        self.append_node(Box::new(UtilityHookNode {
            descriptor,
            next: Cell::new(std::ptr::null()),
        }));
    }

    fn commit(&self, prepared: PreparedUtilityHooks) {
        for node in prepared.nodes {
            self.append_node(node);
        }
    }

    fn snapshot(&self, tag: pg_sys::NodeTag) -> UtilityHookSnapshot {
        UtilityHookSnapshot {
            first: self.head.get(),
            last: self.tail.get(),
            tag: tag as u32,
        }
    }
}

pub(crate) struct PreparedUtilityHooks {
    // Each node is allocated before the runtime registration transaction is
    // committed. The box keeps its address stable while the directory stores
    // a raw backend-lifetime pointer; commit therefore cannot allocate or
    // publish only part of this prepared batch.
    #[allow(clippy::vec_box)]
    nodes: Vec<Box<UtilityHookNode>>,
}

#[derive(Clone, Copy)]
struct UtilityHookSnapshot {
    first: *const UtilityHookNode,
    last: *const UtilityHookNode,
    tag: u32,
}

impl UtilityHookSnapshot {
    fn has_matching_hooks(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    fn for_each(self, mut callback: impl FnMut(UtilityHookDescriptor)) {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: nodes are leaked for the backend lifetime. The copied
            // tail bounds this walk even if a callback appends another node.
            let node = unsafe { &*current };
            if node.descriptor.tag == self.tag {
                callback(node.descriptor);
            }
            if current == self.last {
                break;
            }
            current = node.next.get();
        }
    }
}

thread_local! {
    static UTILITY_HOOKS: UtilityHookDirectory = const { UtilityHookDirectory::new() };
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
            .map(|descriptor| {
                Box::new(UtilityHookNode {
                    descriptor,
                    next: Cell::new(std::ptr::null()),
                })
            })
            .collect(),
    })
}

pub(crate) fn commit_hooks(prepared: PreparedUtilityHooks) {
    UTILITY_HOOKS.with(|directory| directory.commit(prepared));
}

#[cfg(test)]
pub(crate) fn registered_hook_count() -> usize {
    UTILITY_HOOKS.with(|directory| {
        let mut count = 0;
        let mut current = directory.head.get();
        while !current.is_null() {
            count += 1;
            current = unsafe { (*current).next.get() };
        }
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
        let directory = UtilityHookDirectory::new();
        directory.append(descriptor(pg_sys::NodeTag::T_CommentStmt));
        let snapshot = directory.snapshot(pg_sys::NodeTag::T_CommentStmt);
        directory.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        let mut count = 0;
        snapshot.for_each(|_| count += 1);
        assert_eq!(count, 1);
        assert!(snapshot.has_matching_hooks());
    }

    #[test]
    fn snapshot_filters_by_node_tag() {
        let directory = UtilityHookDirectory::new();
        directory.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        assert!(
            directory
                .snapshot(pg_sys::NodeTag::T_CommentStmt)
                .has_matching_hooks()
        );
        assert!(
            !directory
                .snapshot(pg_sys::NodeTag::T_CreateStmt)
                .has_matching_hooks()
        );
    }

    #[test]
    fn snapshot_runs_matching_hooks_in_fifo_registration_order() {
        let directory = UtilityHookDirectory::new();
        let mut first_context = 1_u8;
        let mut second_context = 2_u8;
        let mut first = descriptor(pg_sys::NodeTag::T_CommentStmt);
        first.context = std::ptr::from_mut(&mut first_context).cast();
        let mut second = descriptor(pg_sys::NodeTag::T_CommentStmt);
        second.context = std::ptr::from_mut(&mut second_context).cast();
        directory.append(first);
        directory.append(second);

        let mut order = Vec::new();
        directory
            .snapshot(pg_sys::NodeTag::T_CommentStmt)
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

    pub(super) unsafe fn call_parent_with_node(self, node: *mut pg_sys::Node) {
        let original_node = unsafe { (*self.pstmt).utilityStmt };
        unsafe { (*self.pstmt).utilityStmt = node };
        let mut completion = pg_sys::QueryCompletion::default();
        let nested = Self {
            completion_tag: &mut completion,
            ..self
        };
        match PREV_PROCESS_UTILITY.get() {
            Some(Some(previous)) => unsafe { nested.call_previous(*previous) },
            _ => unsafe { nested.call_standard() },
        }
        unsafe { (*self.pstmt).utilityStmt = original_node };
    }

    pub(super) unsafe fn complete_vacuum(self) {
        unsafe {
            pg_sys::SetQueryCompletion(
                self.completion_tag,
                pg_sys::CommandTag::CMDTAG_VACUUM,
                0,
            );
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

        // Lifecycle preflight is deliberately first and is a no-op unless the
        // runtime was initialized from shared_preload_libraries.
        hooks::preflight(target_node);

        let tag = (*target_node).type_;
        let hooks = UTILITY_HOOKS.with(|directory| directory.snapshot(tag));
        let has_matching_hooks = hooks.has_matching_hooks();
        let has_matching_consumers =
            crate::utility_consumer::has_registered_consumer(tag);
        let runtime_handles = crate::storage::volume_config::handles_utility(tag);
        let copy_from_route = tag == pg_sys::NodeTag::T_CopyStmt
            && (*target_node.cast::<pg_sys::CopyStmt>()).is_from;
        let may_consume_vacuum = tag == pg_sys::NodeTag::T_VacuumStmt;

        if !has_matching_hooks
            && !has_matching_consumers
            && !runtime_handles
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

        let original_node = (has_matching_hooks || runtime_handles).then(|| {
            let old_context = if may_consume_vacuum {
                Some(pg_sys::MemoryContextSwitchTo(pg_sys::PortalContext))
            } else {
                None
            };
            let copied = pg_sys::copyObjectImpl(target_node.cast::<c_void>())
                .cast::<pg_sys::Node>();
            if let Some(old_context) = old_context {
                pg_sys::MemoryContextSwitchTo(old_context);
            }
            copied
        });

        let args = if read_only_tree && (has_matching_hooks || runtime_handles) {
            args.writable_copy()
        } else {
            args
        };
        let target_node = args.target_node();

        if runtime_handles {
            crate::storage::volume_config::utility_pre(
                target_node,
                context == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL,
            );
        }

        hooks.for_each(|descriptor| {
            descriptor.on_pre.expect("validated utility pre-hook")(
                descriptor.context,
                args.pstmt,
                args.query_string,
            );
        });

        // A pre-hook is allowed to rewrite the utility node. Native routes,
        // lifecycle, and consumer selection must therefore use the final
        // utility tag and COPY direction, not the pre-hook input.
        let target_node = args.target_node();
        let final_tag = (*target_node).type_;
        let may_consume_vacuum = final_tag == pg_sys::NodeTag::T_VacuumStmt;
        let selected_consumer =
            crate::utility_consumer::select(final_tag, args).report_unwrap();
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
            // SAFETY: the selection points to a backend-lifetime directory
            // node and is consumed within the same ProcessUtility callback.
            consumer.consume(consumer_args).report_unwrap();
        }
        let consumed = consumer_consumed
            || (may_consume_vacuum
                && full_router::try_route_vacuum_full(
                    target_node.cast(),
                    args,
                    context
                        == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL,
                ));
        if !consumed {
            args.call_parent();
        }

        if let Some(original_node) = original_node {
            hooks.for_each(|descriptor| {
                descriptor.on_post.expect("validated utility post-hook")(
                    descriptor.context,
                    original_node,
                );
            });
            if runtime_handles {
                crate::storage::volume_config::utility_post(original_node);
            }
        }
    }
}
