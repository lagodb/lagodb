use super::error::{HookError, ObjectAccessHookError};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::ffi::{CStr, c_void};
use std::sync::{Arc, OnceLock, RwLock};

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

type ObjectAccessHookList = Arc<Vec<Arc<dyn ObjectAccessHook + Send + Sync>>>;
type ObjectAccessStrHookList = Arc<Vec<Arc<dyn ObjectAccessStrHook + Send + Sync>>>;

static REGISTRY: RwLock<Option<ObjectAccessHookList>> = RwLock::new(None);
static STR_REGISTRY: RwLock<Option<ObjectAccessStrHookList>> = RwLock::new(None);

static PREV_OBJECT_ACCESS_HOOK: OnceLock<pg_sys::object_access_hook_type> =
    OnceLock::new();
static PREV_OBJECT_ACCESS_STR_HOOK: OnceLock<pg_sys::object_access_hook_type_str> =
    OnceLock::new();

fn current_object_access_hooks() -> Option<ObjectAccessHookList> {
    REGISTRY.read().unwrap().clone()
}

fn current_object_access_str_hooks() -> Option<ObjectAccessStrHookList> {
    STR_REGISTRY.read().unwrap().clone()
}

pub fn register_object_access_hook(hook: Box<dyn ObjectAccessHook + Send + Sync>) {
    let hook_name = hook.name();
    let mut registry = REGISTRY.write().unwrap();
    let mut next: Vec<Arc<dyn ObjectAccessHook + Send + Sync>> = registry
        .as_ref()
        .map(|list| Vec::clone(list))
        .unwrap_or_default();
    if next.iter().any(|existing| existing.name() == hook_name) {
        return;
    }
    next.push(Arc::from(hook));
    *registry = Some(Arc::new(next));
    drop(registry);

    PREV_OBJECT_ACCESS_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(object_access_router);
        prev
    });
}

pub fn register_object_access_str_hook(
    hook: Box<dyn ObjectAccessStrHook + Send + Sync>,
) {
    let hook_name = hook.name();
    let mut registry = STR_REGISTRY.write().unwrap();
    let mut next: Vec<Arc<dyn ObjectAccessStrHook + Send + Sync>> = registry
        .as_ref()
        .map(|list| Vec::clone(list))
        .unwrap_or_default();
    if next.iter().any(|existing| existing.name() == hook_name) {
        return;
    }
    next.push(Arc::from(hook));
    *registry = Some(Arc::new(next));
    drop(registry);

    PREV_OBJECT_ACCESS_STR_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::object_access_hook_str;
        pg_sys::object_access_hook_str = Some(object_access_str_router);
        prev
    });
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

unsafe fn invoke_prev_object_access_hook(
    prev: unsafe extern "C-unwind" fn(
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
        arg: *mut c_void,
    ),
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            prev(access, class_id, object_id, sub_id, arg);
        });
    }
}

unsafe fn invoke_prev_object_access_str_hook(
    prev: unsafe extern "C-unwind" fn(
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_name: *const std::os::raw::c_char,
        sub_id: i32,
        arg: *mut c_void,
    ),
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const std::os::raw::c_char,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            prev(access, class_id, object_name, sub_id, arg);
        });
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
        let mut event = event_from_raw(access, class_id, object_id, sub_id, arg);

        if let Some(hooks) = current_object_access_hooks() {
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
        drop(event);

        if let Some(Some(prev)) = PREV_OBJECT_ACCESS_HOOK.get() {
            invoke_prev_object_access_hook(
                *prev, access, class_id, object_id, sub_id, arg,
            );
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
        let mut event =
            str_event_from_raw(access, class_id, object_name, sub_id, arg);

        if let Some(hooks) = current_object_access_str_hooks() {
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
        drop(event);

        if let Some(Some(prev)) = PREV_OBJECT_ACCESS_STR_HOOK.get() {
            invoke_prev_object_access_str_hook(
                *prev,
                access,
                class_id,
                object_name,
                sub_id,
                arg,
            );
        }
    }
}
