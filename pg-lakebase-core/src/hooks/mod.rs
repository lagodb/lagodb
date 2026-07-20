//! PostgreSQL hooks framework
//!
//! This module provides safe wrappers around various PostgreSQL hooks:
//! - `utility_hook`: ProcessUtility hook for DDL statements
//! - `object_access_hook`: Object access hook for permission and access control

mod error;
pub mod object_access_hook;
pub mod utility_hook;

use std::cell::Cell;

use crate::runtime_api::{
    AM_REGISTRATION_VERSION, AmRegistrationV1, MaintenanceProviderV1,
    RuntimeApiError, RuntimeClient, RuntimeRegistrationError,
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
pub use utility_hook::{
    AlterTableMoveAllStmtNode, AlterTableSpaceOptionsStmtNode, AlterTableStmtNode,
    CopyStmtNode, CreateStmtNode, CreateTableAsStmtNode, CreateTableSpaceStmtNode,
    PostUtilityContext, RenameStmtNode, UtilityHook, UtilityNode, UtilityStmtNode,
    register_utility_hook,
};

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
    #[error("one AM registered more hooks than the runtime ABI can represent")]
    TooManyHooks,
    #[error(
        "AM hooks were already published without the maintenance provider; provider and hooks must be registered together"
    )]
    ProviderRegisteredAfterFreeze,
    #[error("this AM DSO already published a maintenance provider")]
    ProviderAlreadyRegistered,
}

/// Atomically publish all utility and object-access hooks registered by this AM.
///
/// The runtime validates and prepares the complete batch before any descriptor
/// becomes visible. Within each hook family, callbacks execute in FIFO
/// registration order, followed by the PostgreSQL hook that preceded Lakebase.
/// Successfully registered callback contexts intentionally live for the
/// backend lifetime.
///
/// # Errors
///
/// Returns an error when the runtime is absent or ABI-incompatible, the batch
/// exceeds ABI limits, or the runtime rejects the complete batch. Failure does
/// not publish a partial batch.
pub fn freeze_hooks() -> Result<(), HookRegistrationError> {
    freeze_hooks_with_provider(None)
}

pub(crate) fn freeze_hooks_with_provider(
    maintenance_provider: Option<&MaintenanceProviderV1>,
) -> Result<(), HookRegistrationError> {
    match (FREEZE_STATE.get(), maintenance_provider.is_some()) {
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

    // Resolve and validate the runtime before moving hooks out of the AM-local
    // building registries, so a load-order error leaves them intact.
    let runtime = RuntimeClient::connect()?;
    let utility = utility_hook::prepare_utility_hooks();
    let object_access = object_access_hook::prepare_object_access_hooks();

    let counts = (
        u32::try_from(utility.descriptors().len()),
        u32::try_from(object_access.descriptors().len()),
        u32::try_from(object_access.str_descriptors().len()),
    );
    let (Ok(utility_count), Ok(object_access_count), Ok(object_access_str_count)) =
        counts
    else {
        utility.restore();
        object_access.restore();
        return Err(HookRegistrationError::TooManyHooks);
    };
    let utility_hooks = if utility.descriptors().is_empty() {
        std::ptr::null()
    } else {
        utility.descriptors().as_ptr()
    };
    let object_access_hooks = if object_access.descriptors().is_empty() {
        std::ptr::null()
    } else {
        object_access.descriptors().as_ptr()
    };
    let object_access_str_hooks = if object_access.str_descriptors().is_empty() {
        std::ptr::null()
    } else {
        object_access.str_descriptors().as_ptr()
    };
    let registration = AmRegistrationV1 {
        abi_version: AM_REGISTRATION_VERSION,
        struct_size: u32::try_from(std::mem::size_of::<AmRegistrationV1>())
            .expect("AM registration size exceeds u32"),
        maintenance_provider: maintenance_provider
            .map(std::ptr::from_ref)
            .unwrap_or(std::ptr::null()),
        utility_hooks,
        utility_hook_count: utility_count,
        object_access_hooks,
        object_access_hook_count: object_access_count,
        object_access_str_hooks,
        object_access_str_hook_count: object_access_str_count,
    };
    // SAFETY: every descriptor and pointer in `registration` was constructed
    // above from the current core ABI types. Their backing vectors remain live
    // for this synchronous call; published callbacks and contexts are retained
    // for the backend lifetime below.
    if let Err(error) = unsafe { runtime.register_am(&registration) } {
        utility.restore();
        object_access.restore();
        return Err(error.into());
    }

    utility.publish_contexts();
    object_access.publish_contexts();
    FREEZE_STATE.set(if maintenance_provider.is_some() {
        FreezeState::WithProvider
    } else {
        FreezeState::HooksOnly
    });
    Ok(())
}
