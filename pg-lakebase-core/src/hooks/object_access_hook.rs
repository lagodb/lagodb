use super::error::{HookError, ObjectAccessHookError};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::ffi::{CStr, c_void};
use std::sync::{OnceLock, RwLock};

#[derive(Debug)]
pub enum ObjectAccessEvent<'a> {
    PostCreate {
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessPostCreate>,
    },
    Drop {
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessDrop>,
    },
    PostAlter {
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessPostAlter>,
    },
    NamespaceSearch {
        object_id: pg_sys::Oid,
        arg: Option<&'a mut pg_sys::ObjectAccessNamespaceSearch>,
    },
    FunctionExecute {
        object_id: pg_sys::Oid,
    },
    Truncate {
        object_id: pg_sys::Oid,
    },
    Unknown {
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
    },
}

impl ObjectAccessEvent<'_> {
    pub fn access(&self) -> pg_sys::ObjectAccessType::Type {
        match self {
            Self::PostCreate { .. } => pg_sys::ObjectAccessType::OAT_POST_CREATE,
            Self::Drop { .. } => pg_sys::ObjectAccessType::OAT_DROP,
            Self::PostAlter { .. } => pg_sys::ObjectAccessType::OAT_POST_ALTER,
            Self::NamespaceSearch { .. } => {
                pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH
            }
            Self::FunctionExecute { .. } => {
                pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE
            }
            Self::Truncate { .. } => pg_sys::ObjectAccessType::OAT_TRUNCATE,
            Self::Unknown { access, .. } => *access,
        }
    }

    pub fn class_id(&self) -> pg_sys::Oid {
        match self {
            Self::PostCreate { class_id, .. }
            | Self::Drop { class_id, .. }
            | Self::PostAlter { class_id, .. }
            | Self::Unknown { class_id, .. } => *class_id,
            Self::NamespaceSearch { .. } => pg_sys::NamespaceRelationId,
            Self::FunctionExecute { .. } => pg_sys::ProcedureRelationId,
            Self::Truncate { .. } => pg_sys::RelationRelationId,
        }
    }

    pub fn object_id(&self) -> Option<pg_sys::Oid> {
        match self {
            Self::PostCreate { object_id, .. }
            | Self::Drop { object_id, .. }
            | Self::PostAlter { object_id, .. }
            | Self::NamespaceSearch { object_id, .. }
            | Self::FunctionExecute { object_id }
            | Self::Truncate { object_id }
            | Self::Unknown { object_id, .. } => Some(*object_id),
        }
    }

    pub fn sub_id(&self) -> i32 {
        match self {
            Self::PostCreate { sub_id, .. }
            | Self::Drop { sub_id, .. }
            | Self::PostAlter { sub_id, .. }
            | Self::Unknown { sub_id, .. } => *sub_id,
            Self::NamespaceSearch { .. }
            | Self::FunctionExecute { .. }
            | Self::Truncate { .. } => 0,
        }
    }

    pub fn is_relation(&self) -> bool {
        self.class_id() == pg_sys::RelationRelationId
    }

    pub fn is_namespace(&self) -> bool {
        self.class_id() == pg_sys::NamespaceRelationId
    }
}

#[derive(Debug)]
pub enum ObjectAccessStrEvent<'a> {
    PostCreate {
        class_id: pg_sys::Oid,
        object_name: &'a CStr,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessPostCreate>,
    },
    Drop {
        class_id: pg_sys::Oid,
        object_name: &'a CStr,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessDrop>,
    },
    PostAlter {
        class_id: pg_sys::Oid,
        object_name: &'a CStr,
        sub_id: i32,
        arg: Option<&'a pg_sys::ObjectAccessPostAlter>,
    },
    NamespaceSearch {
        object_name: &'a CStr,
        arg: Option<&'a mut pg_sys::ObjectAccessNamespaceSearch>,
    },
    FunctionExecute {
        object_name: &'a CStr,
    },
    Truncate {
        object_name: &'a CStr,
    },
    Unknown {
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_name: &'a CStr,
        sub_id: i32,
    },
}

impl ObjectAccessStrEvent<'_> {
    pub fn access(&self) -> pg_sys::ObjectAccessType::Type {
        match self {
            Self::PostCreate { .. } => pg_sys::ObjectAccessType::OAT_POST_CREATE,
            Self::Drop { .. } => pg_sys::ObjectAccessType::OAT_DROP,
            Self::PostAlter { .. } => pg_sys::ObjectAccessType::OAT_POST_ALTER,
            Self::NamespaceSearch { .. } => {
                pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH
            }
            Self::FunctionExecute { .. } => {
                pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE
            }
            Self::Truncate { .. } => pg_sys::ObjectAccessType::OAT_TRUNCATE,
            Self::Unknown { access, .. } => *access,
        }
    }

    pub fn class_id(&self) -> pg_sys::Oid {
        match self {
            Self::PostCreate { class_id, .. }
            | Self::Drop { class_id, .. }
            | Self::PostAlter { class_id, .. }
            | Self::Unknown { class_id, .. } => *class_id,
            Self::NamespaceSearch { .. } => pg_sys::NamespaceRelationId,
            Self::FunctionExecute { .. } => pg_sys::ProcedureRelationId,
            Self::Truncate { .. } => pg_sys::RelationRelationId,
        }
    }

    pub fn object_name(&self) -> &CStr {
        match self {
            Self::PostCreate { object_name, .. }
            | Self::Drop { object_name, .. }
            | Self::PostAlter { object_name, .. }
            | Self::NamespaceSearch { object_name, .. }
            | Self::FunctionExecute { object_name }
            | Self::Truncate { object_name }
            | Self::Unknown { object_name, .. } => object_name,
        }
    }

    pub fn sub_id(&self) -> i32 {
        match self {
            Self::PostCreate { sub_id, .. }
            | Self::Drop { sub_id, .. }
            | Self::PostAlter { sub_id, .. }
            | Self::Unknown { sub_id, .. } => *sub_id,
            Self::NamespaceSearch { .. }
            | Self::FunctionExecute { .. }
            | Self::Truncate { .. } => 0,
        }
    }
}

pub trait ObjectAccessHook {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError>;
}

pub trait ObjectAccessStrHook {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn on_access_str(
        &self,
        event: &mut ObjectAccessStrEvent<'_>,
    ) -> Result<(), ObjectAccessHookError>;
}

type ObjectAccessHookList = &'static [Box<dyn ObjectAccessHook + Send + Sync>];
type ObjectAccessStrHookList =
    &'static [Box<dyn ObjectAccessStrHook + Send + Sync>];

// Object-access hooks are backend-lifetime extension metadata.  Registration
// happens during extension initialization, then the registry is frozen once
// and the object-access routers see only an immutable static slice.  The
// matching-hook path tail-chains directly to a saved PostgreSQL hook that may
// raise an ERROR and longjmp, so the snapshot crossing that direct call must
// not own Drop state such as an Arc<Vec<_>> or a lock guard.  Freezing to a
// `&'static` slice keeps the hot path Copy-only and removes that hazard by
// construction (mirrors `utility_hook.rs`).
//
// A PostgreSQL backend is single-threaded, so these locks are not for runtime
// concurrency: they only provide the interior mutability/`Sync` required to
// mutate a `static` during initialization.  They are written a handful of
// times at startup (register/freeze) and are never touched on the hot path,
// which reads the lock-free `FROZEN_*` snapshots instead.
static BUILDING_REGISTRY: RwLock<Vec<Box<dyn ObjectAccessHook + Send + Sync>>> =
    RwLock::new(Vec::new());
static FROZEN_REGISTRY: OnceLock<ObjectAccessHookList> = OnceLock::new();

static STR_BUILDING_REGISTRY: RwLock<
    Vec<Box<dyn ObjectAccessStrHook + Send + Sync>>,
> = RwLock::new(Vec::new());
static STR_FROZEN_REGISTRY: OnceLock<ObjectAccessStrHookList> = OnceLock::new();

static PREV_OBJECT_ACCESS_HOOK: OnceLock<pg_sys::object_access_hook_type> =
    OnceLock::new();
static PREV_OBJECT_ACCESS_STR_HOOK: OnceLock<pg_sys::object_access_hook_type_str> =
    OnceLock::new();

fn current_object_access_hooks() -> Option<ObjectAccessHookList> {
    FROZEN_REGISTRY
        .get()
        .copied()
        .filter(|hooks| !hooks.is_empty())
}

fn current_object_access_str_hooks() -> Option<ObjectAccessStrHookList> {
    STR_FROZEN_REGISTRY
        .get()
        .copied()
        .filter(|hooks| !hooks.is_empty())
}

fn install_object_access_router() {
    PREV_OBJECT_ACCESS_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(object_access_router);
        prev
    });
}

fn install_object_access_str_router() {
    PREV_OBJECT_ACCESS_STR_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::object_access_hook_str;
        pg_sys::object_access_hook_str = Some(object_access_str_router);
        prev
    });
}

pub fn register_object_access_hook(hook: Box<dyn ObjectAccessHook + Send + Sync>) {
    let hook_name = hook.name();
    let mut entries = BUILDING_REGISTRY.write().unwrap();
    if FROZEN_REGISTRY.get().is_some() {
        panic!("register_object_access_hook called after freeze_object_access_hooks");
    }
    if entries.iter().any(|existing| existing.name() == hook_name) {
        return;
    }
    entries.push(hook);
}

pub fn register_object_access_str_hook(
    hook: Box<dyn ObjectAccessStrHook + Send + Sync>,
) {
    let hook_name = hook.name();
    let mut entries = STR_BUILDING_REGISTRY.write().unwrap();
    if STR_FROZEN_REGISTRY.get().is_some() {
        panic!(
            "register_object_access_str_hook called after freeze_object_access_hooks"
        );
    }
    if entries.iter().any(|existing| existing.name() == hook_name) {
        return;
    }
    entries.push(hook);
}

/// Freeze registered object-access hooks and install the routers.
///
/// Call this once after all [`register_object_access_hook`] and
/// [`register_object_access_str_hook`] calls in extension initialization.
/// After freezing, the routers read a single immutable backend-lifetime
/// snapshot, so the direct tail-chain to a saved PostgreSQL hook does not carry
/// Rust ownership state across ERROR/longjmp paths.
pub fn freeze_object_access_hooks() {
    if freeze_object_access_registry() {
        install_object_access_router();
    }
    if freeze_object_access_str_registry() {
        install_object_access_str_router();
    }
}

fn freeze_object_access_registry() -> bool {
    if let Some(hooks) = FROZEN_REGISTRY.get().copied() {
        return !hooks.is_empty();
    }
    let mut entries = BUILDING_REGISTRY.write().unwrap();
    if let Some(hooks) = FROZEN_REGISTRY.get().copied() {
        return !hooks.is_empty();
    }
    let hooks: ObjectAccessHookList = if entries.is_empty() {
        &[]
    } else {
        Box::leak(std::mem::take(&mut *entries).into_boxed_slice())
    };
    if FROZEN_REGISTRY.set(hooks).is_err() {
        unreachable!("object access hook registry frozen concurrently");
    }
    !hooks.is_empty()
}

fn freeze_object_access_str_registry() -> bool {
    if let Some(hooks) = STR_FROZEN_REGISTRY.get().copied() {
        return !hooks.is_empty();
    }
    let mut entries = STR_BUILDING_REGISTRY.write().unwrap();
    if let Some(hooks) = STR_FROZEN_REGISTRY.get().copied() {
        return !hooks.is_empty();
    }
    let hooks: ObjectAccessStrHookList = if entries.is_empty() {
        &[]
    } else {
        Box::leak(std::mem::take(&mut *entries).into_boxed_slice())
    };
    if STR_FROZEN_REGISTRY.set(hooks).is_err() {
        unreachable!("object access str hook registry frozen concurrently");
    }
    !hooks.is_empty()
}

unsafe fn event_from_raw<'a>(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
) -> ObjectAccessEvent<'a> {
    match access {
        pg_sys::ObjectAccessType::OAT_POST_CREATE => ObjectAccessEvent::PostCreate {
            class_id,
            object_id,
            sub_id,
            arg: if arg.is_null() {
                None
            } else {
                Some(unsafe { &*(arg as *const pg_sys::ObjectAccessPostCreate) })
            },
        },
        pg_sys::ObjectAccessType::OAT_DROP => ObjectAccessEvent::Drop {
            class_id,
            object_id,
            sub_id,
            arg: if arg.is_null() {
                None
            } else {
                Some(unsafe { &*(arg as *const pg_sys::ObjectAccessDrop) })
            },
        },
        pg_sys::ObjectAccessType::OAT_POST_ALTER => ObjectAccessEvent::PostAlter {
            class_id,
            object_id,
            sub_id,
            arg: if arg.is_null() {
                None
            } else {
                Some(unsafe { &*(arg as *const pg_sys::ObjectAccessPostAlter) })
            },
        },
        pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH => {
            ObjectAccessEvent::NamespaceSearch {
                object_id,
                arg: if arg.is_null() {
                    None
                } else {
                    Some(unsafe {
                        &mut *(arg as *mut pg_sys::ObjectAccessNamespaceSearch)
                    })
                },
            }
        }
        pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE => {
            ObjectAccessEvent::FunctionExecute { object_id }
        }
        pg_sys::ObjectAccessType::OAT_TRUNCATE => {
            ObjectAccessEvent::Truncate { object_id }
        }
        _ => ObjectAccessEvent::Unknown {
            access,
            class_id,
            object_id,
            sub_id,
        },
    }
}

unsafe fn str_event_from_raw<'a>(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const std::os::raw::c_char,
    sub_id: i32,
    arg: *mut c_void,
) -> ObjectAccessStrEvent<'a> {
    let object_name = unsafe { CStr::from_ptr(object_name) };
    match access {
        pg_sys::ObjectAccessType::OAT_POST_CREATE => {
            ObjectAccessStrEvent::PostCreate {
                class_id,
                object_name,
                sub_id,
                arg: if arg.is_null() {
                    None
                } else {
                    Some(unsafe { &*(arg as *const pg_sys::ObjectAccessPostCreate) })
                },
            }
        }
        pg_sys::ObjectAccessType::OAT_DROP => ObjectAccessStrEvent::Drop {
            class_id,
            object_name,
            sub_id,
            arg: if arg.is_null() {
                None
            } else {
                Some(unsafe { &*(arg as *const pg_sys::ObjectAccessDrop) })
            },
        },
        pg_sys::ObjectAccessType::OAT_POST_ALTER => ObjectAccessStrEvent::PostAlter {
            class_id,
            object_name,
            sub_id,
            arg: if arg.is_null() {
                None
            } else {
                Some(unsafe { &*(arg as *const pg_sys::ObjectAccessPostAlter) })
            },
        },
        pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH => {
            ObjectAccessStrEvent::NamespaceSearch {
                object_name,
                arg: if arg.is_null() {
                    None
                } else {
                    Some(unsafe {
                        &mut *(arg as *mut pg_sys::ObjectAccessNamespaceSearch)
                    })
                },
            }
        }
        pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE => {
            ObjectAccessStrEvent::FunctionExecute { object_name }
        }
        pg_sys::ObjectAccessType::OAT_TRUNCATE => {
            ObjectAccessStrEvent::Truncate { object_name }
        }
        _ => ObjectAccessStrEvent::Unknown {
            access,
            class_id,
            object_name,
            sub_id,
        },
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn object_access_router(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        // `hooks` is a backend-lifetime `&'static` slice (Copy, no Drop), so it
        // carries no Rust ownership state across the tail-chain to `prev`.
        let Some(hooks) = current_object_access_hooks() else {
            if let Some(Some(prev)) = PREV_OBJECT_ACCESS_HOOK.get() {
                prev(access, class_id, object_id, sub_id, arg);
            }
            return;
        };

        // Bound the event's borrow of `arg` to this scope. `ObjectAccessEvent`
        // holds no Drop state, but `NamespaceSearch` borrows `&mut` the same
        // `ObjectAccessNamespaceSearch` that `prev` receives via the raw `arg`
        // pointer below.  Closing the borrow explicitly (rather than leaning on
        // NLL) makes the FFI boundary unambiguous: no live Rust view of `arg`
        // outlives the direct hand-off to the saved PostgreSQL hook.
        {
            let mut event = event_from_raw(access, class_id, object_id, sub_id, arg);
            for hook in hooks.iter() {
                hook.on_access(&mut event)
                    .map_err(|err| {
                        err.with_object_access_context(
                            hook.name(),
                            event.access(),
                            event.class_id(),
                            event.object_id(),
                            event.sub_id(),
                        )
                    })
                    .report_unwrap();
            }
        }

        if let Some(Some(prev)) = PREV_OBJECT_ACCESS_HOOK.get() {
            // Tail-chain only: no Rust logic follows this saved hook call.  If
            // future code must resume Rust after `prev`, do not add a blanket
            // FFI boundary here; first ensure any state crossing the direct
            // hook call is trivially deallocated or prove the callee is a leaf
            // C function.
            prev(access, class_id, object_id, sub_id, arg);
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn object_access_str_router(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const std::os::raw::c_char,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        // `hooks` is a backend-lifetime `&'static` slice (Copy, no Drop), so it
        // carries no Rust ownership state across the tail-chain to `prev`.
        let Some(hooks) = current_object_access_str_hooks() else {
            if let Some(Some(prev)) = PREV_OBJECT_ACCESS_STR_HOOK.get() {
                prev(access, class_id, object_name, sub_id, arg);
            }
            return;
        };

        // Bound the event's borrow of `arg`/`object_name` to this scope.
        // `ObjectAccessStrEvent` holds no Drop state, but `NamespaceSearch`
        // borrows `&mut` the same `ObjectAccessNamespaceSearch` that `prev`
        // receives via the raw `arg` pointer below.  Closing the borrow
        // explicitly (rather than leaning on NLL) makes the FFI boundary
        // unambiguous: no live Rust view of `arg` outlives the direct hand-off
        // to the saved PostgreSQL hook.
        {
            let mut event =
                str_event_from_raw(access, class_id, object_name, sub_id, arg);
            for hook in hooks.iter() {
                hook.on_access_str(&mut event)
                    .map_err(|err: HookError| {
                        err.with_object_access_str_context(
                            hook.name(),
                            event.access(),
                            event.class_id(),
                            Some(event.object_name().to_string_lossy().into_owned()),
                            event.sub_id(),
                        )
                    })
                    .report_unwrap();
            }
        }

        if let Some(Some(prev)) = PREV_OBJECT_ACCESS_STR_HOOK.get() {
            // Tail-chain only: no Rust logic follows this saved hook call.  If
            // future code must resume Rust after `prev`, do not add a blanket
            // FFI boundary here; first ensure any state crossing the direct
            // hook call is trivially deallocated or prove the callee is a leaf
            // C function.
            prev(access, class_id, object_name, sub_id, arg);
        }
    }
}
