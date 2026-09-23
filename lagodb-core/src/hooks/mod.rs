//! PostgreSQL hooks framework
//!
//! This module provides safe wrappers around various PostgreSQL hooks:
//! - `utility_hook`: ProcessUtility hook for DDL statements
//! - `object_access_hook`: Object access hook for permission and access control

mod error;
pub mod object_access_hook;
mod planning;
mod table_scan;
mod utility_consumer;
pub mod utility_hook;

use std::cell::Cell;
use std::mem::size_of;
use std::ptr;

use crate::runtime_api::{
    ProviderIdentity, ProviderRegistration, RuntimeApiError, RuntimeClient,
    RuntimeRegistrationError, TableProvider, TableScanDescriptor,
    TableScanWorkerDescriptor,
};

pub use crate::runtime_api::{
    OBJECT_ACCESS_DROP, OBJECT_ACCESS_FUNCTION_EXECUTE,
    OBJECT_ACCESS_NAMESPACE_SEARCH, OBJECT_ACCESS_POST_ALTER,
    OBJECT_ACCESS_POST_CREATE, OBJECT_ACCESS_TRUNCATE, ObjectAccessFilter,
};
#[doc(hidden)]
pub use error::UtilityHookPhase;
pub use error::{HookError, ObjectAccessHookError, UtilityHookError};

pub use object_access_hook::{
    ObjectAccessEvent, ObjectAccessHook, ObjectAccessStrEvent, ObjectAccessStrHook,
    register_object_access_hook, register_object_access_str_hook,
};
pub use utility_consumer::{CopyConsumer, CopyRoute, register_copy_consumer};
pub use utility_hook::{
    AlterDatabaseStmtNode, AlterTableMoveAllStmtNode, AlterTableSpaceOptionsStmtNode,
    AlterTableStmtNode, AlterUserMappingStmtNode, CopyStmtNode,
    CreateForeignTableStmtNode, CreateStmtNode, CreateTableAsStmtNode,
    CreateTableSpaceStmtNode, CreateUserMappingStmtNode, CreatedbStmtNode,
    PostUtilityContext, PreUtilityContext, RenameStmtNode, UtilityHook, UtilityNode,
    UtilityStmtNode, VacuumStmtNode, register_utility_hook,
};

pub(crate) use planning::{register_modify, register_relation_scan};

/// Stage this provider DSO's table-scan descriptor for the next atomic
/// [`freeze_hooks`] transaction.
///
/// Providers should use the typed Arrow table-scan adapter's safe registration
/// method instead of calling this raw entry point.
///
/// # Safety
///
/// The descriptor must satisfy all callback, lifetime, panic-containment, and
/// single-backend-thread contracts documented by [`TableScanDescriptor::new`].
#[doc(hidden)]
pub unsafe fn register_table_scan(descriptor: TableScanDescriptor) {
    table_scan::register(descriptor);
}

/// Stage this provider DSO's optional worker table-scan facet in the same
/// atomic registration transaction as its base table scan.
///
/// # Safety
///
/// The descriptor must satisfy the contracts documented by
/// [`TableScanWorkerDescriptor::new`].
#[doc(hidden)]
pub unsafe fn register_table_scan_worker(descriptor: TableScanWorkerDescriptor) {
    table_scan::register_worker(descriptor);
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FreezeState {
    Building,
    HooksOnly,
    WithProvider,
}

thread_local! {
    static FREEZE_STATE: Cell<FreezeState> = const { Cell::new(FreezeState::Building) };
}

pub(super) fn hooks_frozen() -> bool {
    FREEZE_STATE.get() != FreezeState::Building
}

#[derive(Debug, thiserror::Error)]
pub enum HookRegistrationError {
    #[error(transparent)]
    RuntimeApi(#[from] RuntimeApiError),
    #[error(transparent)]
    Registration(#[from] RuntimeRegistrationError),
    #[error("one provider registered more hooks than the runtime ABI can represent")]
    TooManyHooks,
    #[error(
        "provider hooks were already published without the table provider; provider and hooks must be registered together"
    )]
    ProviderRegisteredAfterFreeze,
    #[error("this provider DSO already published a table provider")]
    ProviderAlreadyRegistered,
}

/// Atomically publish all runtime facets registered by this provider.
///
/// The runtime validates and prepares the complete batch before any descriptor
/// becomes visible. Callbacks within each LagoDB hook family execute in FIFO
/// registration order. Planning relation/upper hooks first chain PostgreSQL's
/// preceding hook; planner pre/post callbacks bracket the preceding or standard
/// planner. Successfully registered callback contexts intentionally live for
/// the backend lifetime.
///
/// # Errors
///
/// Returns an error when the runtime is absent or ABI-incompatible, the batch
/// exceeds ABI limits, or the runtime rejects the complete batch. Failure does
/// not publish a partial batch.
pub fn freeze_hooks(
    provider: &ProviderIdentity,
) -> Result<(), HookRegistrationError> {
    freeze_hooks_with_provider(provider, None)
}

pub(crate) fn freeze_hooks_with_provider(
    provider: &ProviderIdentity,
    table_provider: Option<&TableProvider>,
) -> Result<(), HookRegistrationError> {
    match (FREEZE_STATE.get(), table_provider.is_some()) {
        (FreezeState::Building, _) => {}
        (FreezeState::HooksOnly, true) => {
            return Err(HookRegistrationError::ProviderRegisteredAfterFreeze);
        }
        (FreezeState::WithProvider, true) => {
            return Err(HookRegistrationError::ProviderAlreadyRegistered);
        }
        (FreezeState::HooksOnly | FreezeState::WithProvider, false) => {
            return Ok(());
        }
    }

    // Resolve and validate the runtime before moving hooks out of the
    // provider-local building registries, so a load-order error leaves them intact.
    let runtime = RuntimeClient::connect()?;
    let utility = utility_hook::prepare_utility_hooks(
        utility_hook::UtilityHookCallbacks::BACKEND,
    );
    let consumers = utility_consumer::prepare_copy_consumers();
    let object_access = object_access_hook::prepare_object_access_hooks(
        object_access_hook::ObjectAccessHookCallbacks::BACKEND,
    );
    let planning = planning::descriptors();
    let table_scan = table_scan::descriptor();
    let table_scan_worker = table_scan::worker_descriptor();

    let counts = (
        u32::try_from(utility.descriptors().len()),
        u32::try_from(consumers.descriptors().len()),
        u32::try_from(object_access.descriptors().len()),
        u32::try_from(object_access.str_descriptors().len()),
    );
    let (
        Ok(utility_count),
        Ok(utility_consumer_count),
        Ok(object_access_count),
        Ok(object_access_str_count),
    ) = counts
    else {
        utility.restore();
        consumers.restore();
        object_access.restore();
        return Err(HookRegistrationError::TooManyHooks);
    };
    let utility_hooks = if utility.descriptors().is_empty() {
        ptr::null()
    } else {
        utility.descriptors().as_ptr()
    };
    let utility_consumers = if consumers.descriptors().is_empty() {
        ptr::null()
    } else {
        consumers.descriptors().as_ptr()
    };
    let object_access_hooks = if object_access.descriptors().is_empty() {
        ptr::null()
    } else {
        object_access.descriptors().as_ptr()
    };
    let object_access_str_hooks = if object_access.str_descriptors().is_empty() {
        ptr::null()
    } else {
        object_access.str_descriptors().as_ptr()
    };
    let registration = ProviderRegistration {
        struct_size: u32::try_from(size_of::<ProviderRegistration>())
            .expect("provider registration size exceeds u32"),
        provider,
        table_provider: table_provider.map(ptr::from_ref).unwrap_or(ptr::null()),
        utility_hooks,
        utility_hook_count: utility_count,
        utility_consumers,
        utility_consumer_count,
        object_access_hooks,
        object_access_hook_count: object_access_count,
        object_access_str_hooks,
        object_access_str_hook_count: object_access_str_count,
        relation_scan_planner: planning
            .relation_scan
            .as_ref()
            .map(ptr::from_ref)
            .unwrap_or(ptr::null()),
        modify_planner: planning
            .modify
            .as_ref()
            .map(ptr::from_ref)
            .unwrap_or(ptr::null()),
        table_scan: table_scan
            .as_ref()
            .map(ptr::from_ref)
            .unwrap_or(ptr::null()),
        table_scan_worker: table_scan_worker
            .as_ref()
            .map(ptr::from_ref)
            .unwrap_or(ptr::null()),
    };
    // SAFETY: every descriptor and pointer in `registration` was constructed
    // above from the current core ABI types. Their backing vectors remain live
    // for this synchronous call; published callbacks and contexts are retained
    // for the backend lifetime below.
    if let Err(error) = unsafe { runtime.register_provider(&registration) } {
        utility.restore();
        consumers.restore();
        object_access.restore();
        return Err(error.into());
    }

    utility.publish_contexts();
    consumers.publish_contexts();
    object_access.publish_contexts();
    FREEZE_STATE.set(if table_provider.is_some() {
        FreezeState::WithProvider
    } else {
        FreezeState::HooksOnly
    });
    Ok(())
}
