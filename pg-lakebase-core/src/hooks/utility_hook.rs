use super::error::{UtilityHookError, UtilityHookPhase};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::{Cell, RefCell};
use std::ffi::c_void;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::sync::OnceLock;

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
const UTILITY_ROUTER_ABI_VERSION: u32 = 1;
const UTILITY_HOOK_ABI_VERSION: u32 = 1;
const REGISTER_HOOK_OK: u32 = 0;
const REGISTER_HOOK_INVALID: u32 = 1;
const UTILITY_ROUTER_RENDEZVOUS: &std::ffi::CStr = c"pg_lakebase.utility_router.v1";

type RoutedPreHook = unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::Node);
type RoutedPostHook = unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::Node);

/// Fixed-layout descriptor copied into the runtime-owned router.
///
/// Only opaque context and C-ABI callbacks cross the shared-library boundary;
/// Rust trait objects remain owned and interpreted by the AM that created them.
#[repr(C)]
#[derive(Clone, Copy)]
struct UtilityHookDescriptorV1 {
    abi_version: u32,
    struct_size: u32,
    tag: u32,
    context: *mut c_void,
    on_pre: Option<RoutedPreHook>,
    on_post: Option<RoutedPostHook>,
}

#[repr(C)]
struct UtilityRouterApiV1 {
    abi_version: u32,
    struct_size: u32,
    register_hook: unsafe extern "C-unwind" fn(*const UtilityHookDescriptorV1) -> u32,
}

struct ExternalHookContext {
    hook: Box<dyn UtilityHook + Send + Sync>,
}

struct RoutedHookNode {
    descriptor: UtilityHookDescriptorV1,
    next: Cell<*const RoutedHookNode>,
}

#[derive(Clone, Copy)]
struct RoutedHookSnapshot {
    first: *const RoutedHookNode,
    last: *const RoutedHookNode,
    tag: u32,
}

impl RoutedHookSnapshot {
    fn capture(tag: pg_sys::NodeTag) -> Self {
        let (first, last) = ROUTED_REGISTRY
            .with(|registry| (registry.head.get(), registry.tail.get()));
        Self {
            first,
            last,
            tag: tag as u32,
        }
    }

    fn has_matching_hooks(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    fn for_each(self, mut callback: impl FnMut(UtilityHookDescriptorV1)) {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: routed nodes are leaked for the backend lifetime and
            // their descriptor/next fields remain valid after publication.
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

struct RoutedHookRegistry {
    head: Cell<*const RoutedHookNode>,
    tail: Cell<*const RoutedHookNode>,
}

impl RoutedHookRegistry {
    const fn new() -> Self {
        Self {
            head: Cell::new(std::ptr::null()),
            tail: Cell::new(std::ptr::null()),
        }
    }

    fn push(&self, descriptor: UtilityHookDescriptorV1) {
        // Router metadata has backend lifetime, matching PostgreSQL's hook
        // pointers. Leaking the node also makes snapshots safe across direct
        // ProcessUtility calls that may commit or raise ERROR.
        let node = Box::into_raw(Box::new(RoutedHookNode {
            descriptor,
            next: Cell::new(std::ptr::null()),
        }));
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: tail is the previously leaked final node. Registration
            // is backend-single-threaded and only mutates that node's next
            // cell before publishing the new tail.
            unsafe { (*tail).next.set(node) };
        }
    }
}

// This is only a pre-publication registry in an AM-linked core copy. `freeze`
// transfers fixed-layout descriptors to the runtime-owned registry through the
// rendezvous API. It never installs a ProcessUtility hook in the AM DSO.
thread_local! {
    static BUILDING_REGISTRY: RefCell<Vec<UtilityHookEntry>> = const { RefCell::new(Vec::new()) };
    static ROUTED_REGISTRY: RoutedHookRegistry = const { RoutedHookRegistry::new() };
}
static FROZEN_REGISTRY: OnceLock<()> = OnceLock::new();

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

unsafe extern "C" {
    fn find_rendezvous_variable(name: *const c_char) -> *mut *mut c_void;
}

static UTILITY_ROUTER_API: UtilityRouterApiV1 = UtilityRouterApiV1 {
    abi_version: UTILITY_ROUTER_ABI_VERSION,
    struct_size: std::mem::size_of::<UtilityRouterApiV1>() as u32,
    register_hook: register_routed_hook,
};

static ROUTER_API_CACHE: OnceLock<&'static UtilityRouterApiV1> = OnceLock::new();

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
pub(crate) struct ProcessUtilityArgs {
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

    pub(crate) unsafe fn call_parent_with_node(self, node: *mut pg_sys::Node) {
        let original_node = unsafe { (*self.pstmt).utilityStmt };
        unsafe { (*self.pstmt).utilityStmt = node };
        let mut completion = pg_sys::QueryCompletion::default();
        let nested = Self {
            completion_tag: &mut completion,
            ..self
        };
        match PREV_PROCESS_UTILITY.get() {
            Some(Some(prev)) => unsafe { nested.call_prev_direct(*prev) },
            _ => unsafe { nested.call_standard() },
        }
        unsafe { (*self.pstmt).utilityStmt = original_node };
    }

    pub(crate) unsafe fn complete_vacuum(self) {
        unsafe {
            pg_sys::SetQueryCompletion(
                self.completion_tag,
                pg_sys::CommandTag::CMDTAG_VACUUM,
                0,
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

fn install_process_utility_hook() {
    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_router);
        prev
    });
}

pub(crate) fn install_runtime_owned_router() {
    // SAFETY: PostgreSQL owns a backend-lifetime rendezvous slot for this
    // constant NUL-terminated name.
    let slot =
        unsafe { find_rendezvous_variable(UTILITY_ROUTER_RENDEZVOUS.as_ptr()) };
    assert!(
        !slot.is_null(),
        "PostgreSQL returned a null utility-router rendezvous slot"
    );
    // SAFETY: the null check above establishes a live rendezvous slot.
    let published = unsafe { *slot };
    let owned_api = (&UTILITY_ROUTER_API as *const UtilityRouterApiV1)
        .cast_mut()
        .cast::<c_void>();
    if !published.is_null() && published != owned_api {
        panic!("a different pg_lakebase utility router is already published");
    }
    // SAFETY: owned_api points to immutable static storage in the runtime DSO.
    unsafe { *slot = owned_api };
    install_process_utility_hook();
}

fn runtime_router_api() -> Option<&'static UtilityRouterApiV1> {
    if let Some(api) = ROUTER_API_CACHE.get() {
        return Some(*api);
    }
    // SAFETY: PostgreSQL owns a backend-lifetime rendezvous slot for this
    // constant NUL-terminated name.
    let slot =
        unsafe { find_rendezvous_variable(UTILITY_ROUTER_RENDEZVOUS.as_ptr()) };
    if slot.is_null() {
        return None;
    }
    // SAFETY: the slot itself is live; the published pointer is validated for
    // null, ABI version, and structure size before it is cached or invoked.
    let api = unsafe { *slot }.cast::<UtilityRouterApiV1>();
    let api = unsafe { api.as_ref() }?;
    let expected_size =
        u32::try_from(std::mem::size_of::<UtilityRouterApiV1>()).ok()?;
    let api = (api.abi_version == UTILITY_ROUTER_ABI_VERSION
        && api.struct_size >= expected_size)
        .then_some(api)?;
    let _ = ROUTER_API_CACHE.set(api);
    Some(api)
}

#[pg_guard]
unsafe extern "C-unwind" fn register_routed_hook(
    descriptor: *const UtilityHookDescriptorV1,
) -> u32 {
    // SAFETY: the caller passes a pointer valid for this registration call;
    // as_ref also handles a null descriptor without dereferencing it.
    let Some(descriptor) = (unsafe { descriptor.as_ref() }) else {
        return REGISTER_HOOK_INVALID;
    };
    if descriptor.abi_version != UTILITY_HOOK_ABI_VERSION
        || descriptor.struct_size
            < std::mem::size_of::<UtilityHookDescriptorV1>() as u32
        || descriptor.context.is_null()
        || descriptor.on_pre.is_none()
        || descriptor.on_post.is_none()
    {
        return REGISTER_HOOK_INVALID;
    }
    ROUTED_REGISTRY.with(|hooks| {
        hooks.push(*descriptor);
        REGISTER_HOOK_OK
    })
}

#[pg_guard]
unsafe extern "C-unwind" fn route_external_pre_hook(
    context: *mut c_void,
    node: *mut pg_sys::Node,
) {
    // SAFETY: register_routed_hook rejected null context pointers and this
    // callback is stored together with the originating context layout.
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
    // SAFETY: register_routed_hook rejected null context pointers and this
    // callback is stored together with the originating context layout.
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
    if FROZEN_REGISTRY.get().is_some() {
        panic!("register_utility_hook called after freeze_utility_hooks");
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

/// Freeze registered utility hooks into the runtime-owned ProcessUtility router.
///
/// Call this once after all [`register_utility_hook`] calls in extension
/// initialization. The runtime extension must already have published the
/// rendezvous API. The AM keeps ownership of its Rust hook objects for the
/// backend lifetime, while the runtime stores only opaque callback descriptors.
pub fn freeze_utility_hooks() {
    if FROZEN_REGISTRY.get().is_some() {
        return;
    }
    let entries = BUILDING_REGISTRY.with_borrow_mut(std::mem::take);
    if !entries.is_empty() {
        let api = runtime_router_api().unwrap_or_else(|| {
            panic!(
                "pg_lakebase runtime utility router is not available; load pg_lakebase_runtime before registering AM hooks"
            )
        });
        for (tag, hook) in entries {
            // The callback context stays owned by the registering AM DSO for
            // the backend lifetime. Only its opaque pointer crosses rendezvous.
            let context = Box::into_raw(Box::new(ExternalHookContext { hook }));
            let descriptor = UtilityHookDescriptorV1 {
                abi_version: UTILITY_HOOK_ABI_VERSION,
                struct_size: std::mem::size_of::<UtilityHookDescriptorV1>() as u32,
                tag: tag as u32,
                context: context.cast(),
                on_pre: Some(route_external_pre_hook),
                on_post: Some(route_external_post_hook),
            };
            // SAFETY: api was ABI/size validated and descriptor remains live
            // for the duration of the registration callback.
            let result = unsafe { (api.register_hook)(&descriptor) };
            if result != REGISTER_HOOK_OK {
                // SAFETY: registration failed, so the raw pointer was not
                // published and ownership can be reconstructed locally.
                unsafe { drop(Box::from_raw(context)) };
                panic!("runtime utility hook registration failed with code {result}");
            }
        }
    }
    if FROZEN_REGISTRY.set(()).is_err() {
        unreachable!("utility hook registry frozen concurrently");
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

        let hooks = RoutedHookSnapshot::capture(tag);
        let has_matching_hooks = hooks.has_matching_hooks();
        #[cfg(feature = "pg17")]
        let may_consume = tag == pg_sys::NodeTag::T_VacuumStmt;
        #[cfg(not(feature = "pg17"))]
        let may_consume = false;

        if !has_matching_hooks && !may_consume {
            args.tail_chain(PREV_PROCESS_UTILITY.get().copied().flatten());
            return;
        }

        // Only deep-copy the statement tree when hooks need the pre-mutation
        // snapshot; this avoids copyObjectImpl overhead for unhooked tags.
        // A routed VACUUM can commit multiple transactions before post hooks
        // run, so its snapshot must share the Portal's lifetime.
        let copied_node = has_matching_hooks.then(|| {
            let old_context = if may_consume {
                Some(pg_sys::MemoryContextSwitchTo(pg_sys::PortalContext))
            } else {
                None
            };
            let copied =
                pg_sys::copyObjectImpl(target_node as *const std::ffi::c_void)
                    as *mut pg_sys::Node;
            if let Some(old_context) = old_context {
                pg_sys::MemoryContextSwitchTo(old_context);
            }
            copied
        });

        hooks.for_each(|descriptor| {
            // SAFETY: descriptor validation requires a non-null backend-lifetime
            // context and callback; target_node is the current PlannedStmt node.
            descriptor.on_pre.expect("validated utility pre-hook")(
                descriptor.context,
                target_node,
            );
        });

        #[cfg(feature = "pg17")]
        if may_consume
            && crate::table_maintenance::try_route_vacuum_full(
                target_node.cast(),
                args,
                context == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL,
            )
        {
            if let Some(copied_node) = copied_node {
                hooks.for_each(|descriptor| {
                    // SAFETY: copied_node is the backend-owned copy of the
                    // original statement and descriptor was ABI-validated.
                    descriptor.on_post.expect("validated utility post-hook")(
                        descriptor.context,
                        copied_node,
                    );
                });
            }
            return;
        }

        match PREV_PROCESS_UTILITY.get() {
            Some(Some(prev)) => {
                args.call_prev_direct(*prev);
            }
            _ => {
                args.call_standard();
            }
        }

        if let Some(copied_node) = copied_node {
            hooks.for_each(|descriptor| {
                // SAFETY: copied_node is the backend-owned copy of the original
                // statement and descriptor was ABI-validated.
                descriptor.on_post.expect("validated utility post-hook")(
                    descriptor.context,
                    copied_node,
                );
            });
        }
    }
}
