use super::error::{UtilityHookError, UtilityHookPhase};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};

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

type UtilityHookEntry = (pg_sys::NodeTag, Arc<dyn UtilityHook + Send + Sync>);
type UtilityHookList = Arc<Vec<UtilityHookEntry>>;

static REGISTRY: RwLock<Option<UtilityHookList>> = RwLock::new(None);

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

fn current_hooks() -> Option<UtilityHookList> {
    REGISTRY.read().unwrap().clone()
}

pub fn register_utility_hook(
    tag: pg_sys::NodeTag,
    hook: Box<dyn UtilityHook + Send + Sync>,
) {
    let hook_name = hook.name();
    let mut registry = REGISTRY.write().unwrap();
    let mut next: Vec<UtilityHookEntry> = registry
        .as_ref()
        .map(|list| Vec::clone(list))
        .unwrap_or_default();
    if next.iter().any(|(existing_tag, existing_hook)| {
        *existing_tag == tag && existing_hook.name() == hook_name
    }) {
        return;
    }
    next.push((tag, Arc::from(hook)));
    *registry = Some(Arc::new(next));
    drop(registry);

    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        prev
    });
}

#[allow(clippy::too_many_arguments)]
unsafe fn invoke_prev_process_utility(
    prev: unsafe extern "C-unwind" fn(
        pstmt: *mut pg_sys::PlannedStmt,
        query_string: *const std::os::raw::c_char,
        read_only_tree: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: *mut pg_sys::ParamListInfoData,
        query_env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        completion_tag: *mut pg_sys::QueryCompletion,
    ),
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
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
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
        });
    }
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

        let hooks = current_hooks();
        let has_matching_hooks = hooks
            .as_ref()
            .map(|list| list.iter().any(|(reg_tag, _)| *reg_tag == tag))
            .unwrap_or(false);

        // Only deep-copy the statement tree when hooks need the pre-mutation
        // snapshot; this avoids copyObjectImpl overhead for unhooked tags.
        let copied_node = if has_matching_hooks {
            let copied =
                pg_sys::copyObjectImpl(target_node as *const std::ffi::c_void)
                    as *mut pg_sys::Node;

            let mut safe_node = UtilityNode::new(target_node);
            // SAFETY: `has_matching_hooks` is true => `hooks` is Some.
            for (reg_tag, hook) in hooks.as_ref().unwrap().iter() {
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
            Some(copied)
        } else {
            None
        };

        match PREV_PROCESS_UTILITY.get() {
            Some(Some(prev)) => {
                invoke_prev_process_utility(
                    *prev,
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

        if let Some(copied_node) = copied_node {
            let post_context = PostUtilityContext::new(copied_node);
            // SAFETY: copied_node is Some only when `hooks` is Some.
            for (reg_tag, hook) in hooks.as_ref().unwrap().iter() {
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
}
