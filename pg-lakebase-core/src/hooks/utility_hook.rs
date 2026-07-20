use super::error::{UtilityHookError, UtilityHookPhase};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::ffi::c_void;
use std::marker::PhantomData;

use crate::runtime_api::{
    HOOK_DESCRIPTOR_VERSION, RoutedUtilityPostHook, RoutedUtilityPreHook,
    UtilityHookDescriptorV1,
};

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

struct ExternalHookContext {
    tag: pg_sys::NodeTag,
    hook: Box<dyn UtilityHook + Send + Sync>,
}

// This is only a pre-publication registry in an AM-linked core copy. `freeze`
// transfers fixed-layout descriptors to the runtime-owned registry through the
// rendezvous API. It never installs a ProcessUtility hook in the AM DSO.
thread_local! {
    static BUILDING_REGISTRY: RefCell<Vec<UtilityHookEntry>> = const { RefCell::new(Vec::new()) };
}

pub(super) struct PreparedUtilityHooks {
    // Each descriptor stores a raw pointer to its context before this vector is
    // published. Boxing keeps those pointee addresses stable while the vector
    // itself is built, moved into the registration batch, or restored.
    #[allow(clippy::vec_box)]
    contexts: Vec<Box<ExternalHookContext>>,
    descriptors: Vec<UtilityHookDescriptorV1>,
}

impl PreparedUtilityHooks {
    pub(super) fn descriptors(&self) -> &[UtilityHookDescriptorV1] {
        &self.descriptors
    }

    pub(super) fn publish_contexts(self) {
        for context in self.contexts {
            let _ = Box::into_raw(context);
        }
    }

    pub(super) fn restore(self) {
        BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.extend(
                self.contexts
                    .into_iter()
                    .map(|context| (context.tag, context.hook)),
            );
        });
    }
}

#[derive(Clone, Copy)]
pub(super) struct UtilityHookCallbacks {
    on_pre: RoutedUtilityPreHook,
    on_post: RoutedUtilityPostHook,
}

impl UtilityHookCallbacks {
    pub(super) const BACKEND: Self = Self {
        on_pre: route_external_pre_hook,
        on_post: route_external_post_hook,
    };
}

#[pg_guard]
unsafe extern "C-unwind" fn route_external_pre_hook(
    context: *mut c_void,
    node: *mut pg_sys::Node,
) {
    // SAFETY: the runtime's atomic AM registration rejected null context
    // pointers, and this callback is stored with the originating layout.
    let context = unsafe { &*context.cast::<ExternalHookContext>() };
    // SAFETY: the runtime passes the live PlannedStmt utility node supplied to
    // its ProcessUtility callback.
    let mut node = unsafe { UtilityNode::new(node) };
    let tag = node.tag();
    context
        .hook
        .on_pre(&mut node)
        .map_err(|error| {
            error.with_utility_context(
                context.hook.name(),
                UtilityHookPhase::Pre,
                tag,
            )
        })
        .report_unwrap();
}

#[pg_guard]
unsafe extern "C-unwind" fn route_external_post_hook(
    context: *mut c_void,
    original_node: *mut pg_sys::Node,
) {
    // SAFETY: the runtime's atomic AM registration rejected null context
    // pointers, and this callback is stored with the originating layout.
    let context = unsafe { &*context.cast::<ExternalHookContext>() };
    // SAFETY: the runtime passes its live copyObject snapshot of the original
    // utility statement.
    let post_context = unsafe { PostUtilityContext::new(original_node) };
    let tag = post_context.tag();
    context
        .hook
        .on_post(&post_context)
        .map_err(|error| {
            error.with_utility_context(
                context.hook.name(),
                UtilityHookPhase::PostSuccess,
                tag,
            )
        })
        .report_unwrap();
}

pub fn register_utility_hook(
    tag: pg_sys::NodeTag,
    hook: Box<dyn UtilityHook + Send + Sync>,
) {
    let hook_name = hook.name();
    if super::hooks_frozen() {
        panic!("register_utility_hook called after freeze_hooks");
    }
    BUILDING_REGISTRY.with_borrow_mut(|entries| {
        if entries.iter().any(|(existing_tag, existing_hook)| {
            *existing_tag == tag && existing_hook.name() == hook_name
        }) {
            return;
        }
        entries.push((tag, hook));
    });
}

pub(super) fn prepare_utility_hooks(
    callbacks: UtilityHookCallbacks,
) -> PreparedUtilityHooks {
    let entries = BUILDING_REGISTRY.with_borrow_mut(std::mem::take);
    let mut contexts = Vec::with_capacity(entries.len());
    let mut descriptors = Vec::with_capacity(entries.len());
    for (tag, hook) in entries {
        let mut context = Box::new(ExternalHookContext { tag, hook });
        descriptors.push(UtilityHookDescriptorV1 {
            abi_version: HOOK_DESCRIPTOR_VERSION,
            struct_size:
                u32::try_from(std::mem::size_of::<UtilityHookDescriptorV1>())
                    .expect("utility hook descriptor size exceeds u32"),
            tag: tag as u32,
            context: std::ptr::from_mut(context.as_mut()).cast(),
            on_pre: Some(callbacks.on_pre),
            on_post: Some(callbacks.on_post),
        });
        contexts.push(context);
    }
    PreparedUtilityHooks {
        contexts,
        descriptors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C-unwind" fn test_route_hook(
        _context: *mut c_void,
        _node: *mut pg_sys::Node,
    ) {
    }

    const TEST_CALLBACKS: UtilityHookCallbacks = UtilityHookCallbacks {
        on_pre: test_route_hook,
        on_post: test_route_hook,
    };

    struct TestHook;

    impl UtilityHook for TestHook {
        fn on_pre(&self, _stmt: &mut UtilityNode) -> Result<(), UtilityHookError> {
            Ok(())
        }

        fn on_post(
            &self,
            _context: &PostUtilityContext,
        ) -> Result<(), UtilityHookError> {
            Ok(())
        }
    }

    #[test]
    fn restoring_prepared_hooks_repopulates_building_registry() {
        BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.clear();
            entries.push((pg_sys::NodeTag::T_CommentStmt, Box::new(TestHook)));
        });

        let prepared = prepare_utility_hooks(TEST_CALLBACKS);
        assert_eq!(prepared.descriptors().len(), 1);
        assert!(prepared.descriptors()[0].on_pre.is_some());
        assert!(prepared.descriptors()[0].on_post.is_some());
        assert!(BUILDING_REGISTRY.with_borrow(Vec::is_empty));
        prepared.restore();
        assert_eq!(BUILDING_REGISTRY.with_borrow(Vec::len), 1);
        BUILDING_REGISTRY.with_borrow_mut(Vec::clear);
    }
}
