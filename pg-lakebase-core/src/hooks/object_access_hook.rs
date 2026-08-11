use super::error::{HookError, ObjectAccessHookError};
use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::ffi::{CStr, c_void};

use crate::runtime_api::{
    ObjectAccessFilter, ObjectAccessHookDescriptor, ObjectAccessStrHookDescriptor,
    RoutedObjectAccessHook, RoutedObjectAccessStrHook,
};

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

    fn filter(&self) -> ObjectAccessFilter;

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError>;
}

pub trait ObjectAccessStrHook {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn filter(&self) -> ObjectAccessFilter;

    fn on_access_str(
        &self,
        event: &mut ObjectAccessStrEvent<'_>,
    ) -> Result<(), ObjectAccessHookError>;
}

// Backend-local registry: object-access hooks are registered, stored, and
// dispatched entirely within a single PostgreSQL backend thread, so no
// `Send + Sync` bound is required.
type ObjectAccessHookEntry = Box<dyn ObjectAccessHook>;
type ObjectAccessStrHookEntry = Box<dyn ObjectAccessStrHook>;

struct ExternalObjectAccessContext {
    hook: ObjectAccessHookEntry,
}

struct ExternalObjectAccessStrContext {
    hook: ObjectAccessStrHookEntry,
}

pub(super) struct PreparedObjectAccessHooks {
    // Descriptors hold raw context pointers while these vectors are prepared
    // and moved. The boxes are therefore required for stable pointee addresses.
    #[allow(clippy::vec_box)]
    contexts: Vec<Box<ExternalObjectAccessContext>>,
    descriptors: Vec<ObjectAccessHookDescriptor>,
    #[allow(clippy::vec_box)]
    str_contexts: Vec<Box<ExternalObjectAccessStrContext>>,
    str_descriptors: Vec<ObjectAccessStrHookDescriptor>,
}

impl PreparedObjectAccessHooks {
    pub(super) fn descriptors(&self) -> &[ObjectAccessHookDescriptor] {
        &self.descriptors
    }

    pub(super) fn str_descriptors(&self) -> &[ObjectAccessStrHookDescriptor] {
        &self.str_descriptors
    }

    pub(super) fn publish_contexts(self) {
        for context in self.contexts {
            let _ = Box::into_raw(context);
        }
        for context in self.str_contexts {
            let _ = Box::into_raw(context);
        }
    }

    pub(super) fn restore(self) {
        BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.extend(self.contexts.into_iter().map(|context| context.hook));
        });
        STR_BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.extend(self.str_contexts.into_iter().map(|context| context.hook));
        });
    }
}

thread_local! {
    static BUILDING_REGISTRY: RefCell<Vec<ObjectAccessHookEntry>> = const { RefCell::new(Vec::new()) };
    static STR_BUILDING_REGISTRY: RefCell<Vec<ObjectAccessStrHookEntry>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy)]
pub(super) struct ObjectAccessHookCallbacks {
    on_access: RoutedObjectAccessHook,
    on_access_str: RoutedObjectAccessStrHook,
}

impl ObjectAccessHookCallbacks {
    pub(super) const BACKEND: Self = Self {
        on_access: route_external_object_access_hook,
        on_access_str: route_external_object_access_str_hook,
    };
}

pub fn register_object_access_hook(hook: Box<dyn ObjectAccessHook>) {
    let hook_name = hook.name();
    if super::hooks_frozen() {
        panic!("register_object_access_hook called after freeze_hooks");
    }
    BUILDING_REGISTRY.with_borrow_mut(|entries| {
        if entries.iter().any(|existing| existing.name() == hook_name) {
            return;
        }
        entries.push(hook);
    });
}

pub fn register_object_access_str_hook(hook: Box<dyn ObjectAccessStrHook>) {
    let hook_name = hook.name();
    if super::hooks_frozen() {
        panic!("register_object_access_str_hook called after freeze_hooks");
    }
    STR_BUILDING_REGISTRY.with_borrow_mut(|entries| {
        if entries.iter().any(|existing| existing.name() == hook_name) {
            return;
        }
        entries.push(hook);
    });
}

pub(super) fn prepare_object_access_hooks(
    callbacks: ObjectAccessHookCallbacks,
) -> PreparedObjectAccessHooks {
    let entries = BUILDING_REGISTRY.with_borrow_mut(std::mem::take);
    let mut contexts = Vec::with_capacity(entries.len());
    let mut descriptors = Vec::with_capacity(entries.len());
    for hook in entries {
        let filter = hook.filter();
        let mut context = Box::new(ExternalObjectAccessContext { hook });
        descriptors.push(ObjectAccessHookDescriptor {
            struct_size: u32::try_from(
                std::mem::size_of::<ObjectAccessHookDescriptor>(),
            )
            .expect("object-access hook descriptor size exceeds u32"),
            event_mask: filter.event_mask(),
            class_id: filter.class_id(),
            context: std::ptr::from_mut(context.as_mut()).cast(),
            on_access: Some(callbacks.on_access),
        });
        contexts.push(context);
    }

    let entries = STR_BUILDING_REGISTRY.with_borrow_mut(std::mem::take);
    let mut str_contexts = Vec::with_capacity(entries.len());
    let mut str_descriptors = Vec::with_capacity(entries.len());
    for hook in entries {
        let filter = hook.filter();
        let mut context = Box::new(ExternalObjectAccessStrContext { hook });
        str_descriptors.push(ObjectAccessStrHookDescriptor {
            struct_size: u32::try_from(std::mem::size_of::<
                ObjectAccessStrHookDescriptor,
            >())
            .expect("object-access-str hook descriptor size exceeds u32"),
            event_mask: filter.event_mask(),
            class_id: filter.class_id(),
            context: std::ptr::from_mut(context.as_mut()).cast(),
            on_access: Some(callbacks.on_access_str),
        });
        str_contexts.push(context);
    }
    PreparedObjectAccessHooks {
        contexts,
        descriptors,
        str_contexts,
        str_descriptors,
    }
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
unsafe extern "C-unwind" fn route_external_object_access_hook(
    context: *mut c_void,
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
) {
    // SAFETY: runtime validation rejects null contexts and stores this callback
    // together with the originating AM context layout.
    let context = unsafe { &*context.cast::<ExternalObjectAccessContext>() };
    let mut event =
        unsafe { event_from_raw(access, class_id, object_id, sub_id, arg) };
    context
        .hook
        .on_access(&mut event)
        .map_err(|err| {
            err.with_object_access_context(
                context.hook.name(),
                event.access(),
                event.class_id(),
                event.object_id(),
                event.sub_id(),
            )
        })
        .report_unwrap();
}

#[pg_guard]
unsafe extern "C-unwind" fn route_external_object_access_str_hook(
    context: *mut c_void,
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const std::os::raw::c_char,
    sub_id: i32,
    arg: *mut c_void,
) {
    // SAFETY: runtime validation rejects null contexts and stores this callback
    // together with the originating AM context layout.
    let context = unsafe { &*context.cast::<ExternalObjectAccessStrContext>() };
    let mut event =
        unsafe { str_event_from_raw(access, class_id, object_name, sub_id, arg) };
    context
        .hook
        .on_access_str(&mut event)
        .map_err(|err: HookError| {
            err.with_object_access_str_context(
                context.hook.name(),
                event.access(),
                event.class_id(),
                Some(event.object_name().to_string_lossy().into_owned()),
                event.sub_id(),
            )
        })
        .report_unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_api::OBJECT_ACCESS_DROP;

    unsafe extern "C-unwind" fn test_route_object_access_hook(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_id: pg_sys::Oid,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    unsafe extern "C-unwind" fn test_route_object_access_str_hook(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_name: *const std::os::raw::c_char,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    const TEST_CALLBACKS: ObjectAccessHookCallbacks = ObjectAccessHookCallbacks {
        on_access: test_route_object_access_hook,
        on_access_str: test_route_object_access_str_hook,
    };

    struct TestObjectHook;

    impl ObjectAccessHook for TestObjectHook {
        fn filter(&self) -> ObjectAccessFilter {
            ObjectAccessFilter::new(OBJECT_ACCESS_DROP)
        }

        fn on_access(
            &self,
            _event: &mut ObjectAccessEvent<'_>,
        ) -> Result<(), ObjectAccessHookError> {
            Ok(())
        }
    }

    struct TestObjectStrHook;

    impl ObjectAccessStrHook for TestObjectStrHook {
        fn filter(&self) -> ObjectAccessFilter {
            ObjectAccessFilter::new(OBJECT_ACCESS_DROP)
        }

        fn on_access_str(
            &self,
            _event: &mut ObjectAccessStrEvent<'_>,
        ) -> Result<(), ObjectAccessHookError> {
            Ok(())
        }
    }

    #[test]
    fn restoring_prepared_hooks_repopulates_both_building_registries() {
        BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.clear();
            entries.push(Box::new(TestObjectHook));
        });
        STR_BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.clear();
            entries.push(Box::new(TestObjectStrHook));
        });

        let prepared = prepare_object_access_hooks(TEST_CALLBACKS);
        assert_eq!(prepared.descriptors().len(), 1);
        assert_eq!(prepared.str_descriptors().len(), 1);
        assert!(prepared.descriptors()[0].on_access.is_some());
        assert!(prepared.str_descriptors()[0].on_access.is_some());
        assert!(BUILDING_REGISTRY.with_borrow(Vec::is_empty));
        assert!(STR_BUILDING_REGISTRY.with_borrow(Vec::is_empty));
        prepared.restore();
        assert_eq!(BUILDING_REGISTRY.with_borrow(Vec::len), 1);
        assert_eq!(STR_BUILDING_REGISTRY.with_borrow(Vec::len), 1);
        BUILDING_REGISTRY.with_borrow_mut(Vec::clear);
        STR_BUILDING_REGISTRY.with_borrow_mut(Vec::clear);
    }
}
