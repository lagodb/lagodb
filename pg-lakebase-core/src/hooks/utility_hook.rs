use super::error::{UtilityHookError, UtilityHookPhase};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::sync::{OnceLock, RwLock};

/// Type-level binding between a marker type, PostgreSQL utility statement
/// struct, and its [`pg_sys::NodeTag`].
///
/// # Safety
///
/// Implementors must guarantee that `TAG` is the PostgreSQL node tag for
/// `Node`.
pub unsafe trait UtilityStmtNode {
    type Node;
    const TAG: pg_sys::NodeTag;
}

macro_rules! impl_utility_stmt_node {
    ($marker:ident, $node:ty, $tag:path) => {
        pub struct $marker;

        unsafe impl UtilityStmtNode for $marker {
            type Node = $node;
            const TAG: pg_sys::NodeTag = $tag;
        }
    };
}

impl_utility_stmt_node!(CopyStmtNode, pg_sys::CopyStmt, pg_sys::NodeTag::T_CopyStmt);
impl_utility_stmt_node!(
    AlterTableStmtNode,
    pg_sys::AlterTableStmt,
    pg_sys::NodeTag::T_AlterTableStmt
);
impl_utility_stmt_node!(
    AlterTableMoveAllStmtNode,
    pg_sys::AlterTableMoveAllStmt,
    pg_sys::NodeTag::T_AlterTableMoveAllStmt
);
impl_utility_stmt_node!(
    CreateTableAsStmtNode,
    pg_sys::CreateTableAsStmt,
    pg_sys::NodeTag::T_CreateTableAsStmt
);
impl_utility_stmt_node!(
    CreateStmtNode,
    pg_sys::CreateStmt,
    pg_sys::NodeTag::T_CreateStmt
);
impl_utility_stmt_node!(
    CreateTableSpaceStmtNode,
    pg_sys::CreateTableSpaceStmt,
    pg_sys::NodeTag::T_CreateTableSpaceStmt
);
impl_utility_stmt_node!(
    RenameStmtNode,
    pg_sys::RenameStmt,
    pg_sys::NodeTag::T_RenameStmt
);
impl_utility_stmt_node!(
    AlterTableSpaceOptionsStmtNode,
    pg_sys::AlterTableSpaceOptionsStmt,
    pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt
);

/// A safe wrapper around `pg_sys::Node`
pub struct UtilityNode<'a> {
    ptr: *mut pg_sys::Node,
    _marker: PhantomData<&'a pg_sys::Node>,
}

impl<'a> UtilityNode<'a> {
    /// # Safety
    /// `ptr` must be a valid pointer to a PostgreSQL Node with appropriate lifetime.
    pub unsafe fn new(ptr: *mut pg_sys::Node) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    pub fn tag(&self) -> pg_sys::NodeTag {
        unsafe { (*self.ptr).type_ }
    }

    pub fn cast<T>(&self) -> Option<&T::Node>
    where
        T: UtilityStmtNode,
    {
        unsafe { (self.tag() == T::TAG).then(|| &*(self.ptr as *const T::Node)) }
    }

    pub fn cast_mut<T>(&mut self) -> Option<&mut T::Node>
    where
        T: UtilityStmtNode,
    {
        unsafe { (self.tag() == T::TAG).then(|| &mut *(self.ptr as *mut T::Node)) }
    }
}

/// Context for a utility hook after the command completed successfully.
pub struct PostUtilityContext<'a> {
    original_stmt: UtilityNode<'a>,
}

impl<'a> PostUtilityContext<'a> {
    unsafe fn new(original_stmt: *mut pg_sys::Node) -> Self {
        Self {
            original_stmt: unsafe { UtilityNode::new(original_stmt) },
        }
    }

    /// The statement as it looked before any `on_pre` hook mutated it.
    pub fn original_stmt(&self) -> &UtilityNode<'a> {
        &self.original_stmt
    }

    pub fn tag(&self) -> pg_sys::NodeTag {
        self.original_stmt.tag()
    }
}

pub trait UtilityHook {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn on_pre(&self, stmt: &mut UtilityNode) -> Result<(), UtilityHookError>;
    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError>;
}

type UtilityHookEntry = (pg_sys::NodeTag, Box<dyn UtilityHook + Send + Sync>);
type UtilityHookList = &'static [UtilityHookEntry];

// Utility hooks are backend-lifetime extension metadata.  Registration happens
// during extension initialization, then the registry is frozen once and the
// ProcessUtility router sees only an immutable static slice.  The matching-hook
// path may call a PostgreSQL utility dispatcher directly and then resume Rust
// for post hooks, so the snapshot crossing that direct call must not own Drop
// state such as an Arc<Vec<_>> or a lock guard.
//
// A PostgreSQL backend is single-threaded, so this lock is not for runtime
// concurrency: it only provides the interior mutability/`Sync` required to
// mutate a `static` during initialization. It is written a handful of times
// at startup (register/freeze) and is never touched on the hot path, which
// reads the lock-free `FROZEN_REGISTRY` snapshot instead.
static BUILDING_REGISTRY: RwLock<Vec<UtilityHookEntry>> = RwLock::new(Vec::new());
static FROZEN_REGISTRY: OnceLock<UtilityHookList> = OnceLock::new();

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
struct ProcessUtilityArgs {
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
}

impl ProcessUtilityArgs {
    unsafe fn target_node(self) -> *mut pg_sys::Node {
        unsafe { (*self.pstmt).utilityStmt }
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

    unsafe fn call_prev_direct(self, prev: ProcessUtilityHookFn) {
        // `prev` is a saved hook dispatcher.  It can re-enter pgrx callbacks
        // with their own `#[pg_guard]` boundary, so do not wrap it in a manual
        // `pg_guard_ffi_boundary`.  The matching-hook path resumes Rust after
        // successful return to run post hooks; router state crossing this call
        // must therefore be trivially deallocated.
        unsafe {
            prev(
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

    unsafe fn tail_chain(self, prev: pg_sys::ProcessUtility_hook_type) {
        match prev {
            Some(prev) => unsafe {
                prev(
                    self.pstmt,
                    self.query_string,
                    self.read_only_tree,
                    self.context,
                    self.params,
                    self.query_env,
                    self.dest,
                    self.completion_tag,
                );
            },
            None => unsafe {
                self.call_standard();
            },
        }
    }
}

fn current_hooks() -> Option<UtilityHookList> {
    FROZEN_REGISTRY
        .get()
        .copied()
        .filter(|hooks| !hooks.is_empty())
}

fn install_process_utility_hook() {
    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        prev
    });
}

pub fn register_utility_hook(
    tag: pg_sys::NodeTag,
    hook: Box<dyn UtilityHook + Send + Sync>,
) {
    let hook_name = hook.name();
    let mut entries = BUILDING_REGISTRY.write().unwrap();
    if FROZEN_REGISTRY.get().is_some() {
        panic!("register_utility_hook called after freeze_utility_hooks");
    }

    if entries.iter().any(|(existing_tag, existing_hook)| {
        *existing_tag == tag && existing_hook.name() == hook_name
    }) {
        return;
    }

    entries.push((tag, hook));
}

/// Freeze registered utility hooks and install the ProcessUtility router.
///
/// Call this once after all [`register_utility_hook`] calls in extension
/// initialization.  After freezing, the router reads a single immutable
/// backend-lifetime snapshot, so direct dispatcher calls do not carry Rust
/// ownership state across PostgreSQL ERROR/longjmp paths.
pub fn freeze_utility_hooks() {
    let should_install = {
        if let Some(hooks) = FROZEN_REGISTRY.get().copied() {
            !hooks.is_empty()
        } else {
            let mut entries = BUILDING_REGISTRY.write().unwrap();
            if let Some(hooks) = FROZEN_REGISTRY.get().copied() {
                !hooks.is_empty()
            } else {
                let hooks: UtilityHookList = if entries.is_empty() {
                    &[]
                } else {
                    Box::leak(std::mem::take(&mut *entries).into_boxed_slice())
                };
                if FROZEN_REGISTRY.set(hooks).is_err() {
                    unreachable!("utility hook registry frozen concurrently");
                }
                !hooks.is_empty()
            }
        }
    };

    if should_install {
        install_process_utility_hook();
    }
}

#[pg_guard]
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
        let tag = (*target_node).type_;

        let Some(hooks) = current_hooks() else {
            args.tail_chain(PREV_PROCESS_UTILITY.get().copied().flatten());
            return;
        };

        let has_matching_hooks = hooks.iter().any(|(reg_tag, _)| *reg_tag == tag);
        if !has_matching_hooks {
            args.tail_chain(PREV_PROCESS_UTILITY.get().copied().flatten());
            return;
        }

        // Only deep-copy the statement tree when hooks need the pre-mutation
        // snapshot; this avoids copyObjectImpl overhead for unhooked tags.
        let copied_node =
            pg_sys::copyObjectImpl(target_node as *const std::ffi::c_void)
                as *mut pg_sys::Node;

        let mut safe_node = UtilityNode::new(target_node);
        for (reg_tag, hook) in hooks.iter() {
            if *reg_tag == tag {
                hook.on_pre(&mut safe_node)
                    .map_err(|err| {
                        err.with_utility_context(
                            hook.name(),
                            UtilityHookPhase::Pre,
                            tag,
                        )
                    })
                    .report_unwrap();
            }
        }

        match PREV_PROCESS_UTILITY.get() {
            Some(Some(prev)) => {
                args.call_prev_direct(*prev);
            }
            _ => {
                args.call_standard();
            }
        }

        let post_context = PostUtilityContext::new(copied_node);
        for (reg_tag, hook) in hooks.iter() {
            if *reg_tag == tag {
                hook.on_post(&post_context)
                    .map_err(|err| {
                        err.with_utility_context(
                            hook.name(),
                            UtilityHookPhase::PostSuccess,
                            tag,
                        )
                    })
                    .report_unwrap();
            }
        }
    }
}
