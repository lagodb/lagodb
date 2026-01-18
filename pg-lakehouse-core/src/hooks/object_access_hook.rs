use crate::diag::ReportableError;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};
use thiserror::Error;

/// Error type for object access hooks
#[derive(Error, Debug)]
pub enum ObjectAccessHookError {
    #[error("{0}")]
    Message(String),
}

impl From<ObjectAccessHookError> for ErrorReport {
    fn from(value: ObjectAccessHookError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, error_message, "")
    }
}

/// A safe wrapper around PostgreSQL object access information
pub struct ObjectAccessContext<'a> {
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut std::ffi::c_void,
    _marker: PhantomData<&'a mut std::ffi::c_void>,
}

impl<'a> ObjectAccessContext<'a> {
    pub unsafe fn new(
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
        arg: *mut std::ffi::c_void,
    ) -> Self {
        Self {
            access,
            class_id,
            object_id,
            sub_id,
            arg,
            _marker: PhantomData,
        }
    }

    pub fn access(&self) -> pg_sys::ObjectAccessType::Type {
        self.access
    }

    pub fn class_id(&self) -> pg_sys::Oid {
        self.class_id
    }

    pub fn object_id(&self) -> pg_sys::Oid {
        self.object_id
    }

    pub fn sub_id(&self) -> i32 {
        self.sub_id
    }

    pub fn arg_ptr(&self) -> *mut std::ffi::c_void {
        self.arg
    }

    pub fn is_post_create(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_POST_CREATE
    }

    pub fn is_post_alter(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_POST_ALTER
    }

    pub fn is_drop(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_DROP
    }

    pub fn is_namespace_search(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH
    }

    pub fn is_function_execute(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE
    }

    pub fn is_truncate(&self) -> bool {
        self.access == pg_sys::ObjectAccessType::OAT_TRUNCATE
    }

    pub fn is_relation(&self) -> bool {
        self.class_id == pg_sys::RelationRelationId
    }

    pub fn is_namespace(&self) -> bool {
        self.class_id == pg_sys::NamespaceRelationId
    }

    pub fn as_post_create(&self) -> Option<&pg_sys::ObjectAccessPostCreate> {
        if self.is_post_create() && !self.arg.is_null() {
            unsafe { Some(&*(self.arg as *const pg_sys::ObjectAccessPostCreate)) }
        } else {
            None
        }
    }

    pub fn as_post_alter(&self) -> Option<&pg_sys::ObjectAccessPostAlter> {
        if self.is_post_alter() && !self.arg.is_null() {
            unsafe { Some(&*(self.arg as *const pg_sys::ObjectAccessPostAlter)) }
        } else {
            None
        }
    }

    pub fn as_drop(&self) -> Option<&pg_sys::ObjectAccessDrop> {
        if self.is_drop() && !self.arg.is_null() {
            unsafe { Some(&*(self.arg as *const pg_sys::ObjectAccessDrop)) }
        } else {
            None
        }
    }

    pub fn as_namespace_search(
        &self,
    ) -> Option<&pg_sys::ObjectAccessNamespaceSearch> {
        if self.is_namespace_search() && !self.arg.is_null() {
            unsafe {
                Some(&*(self.arg as *const pg_sys::ObjectAccessNamespaceSearch))
            }
        } else {
            None
        }
    }

    pub fn as_function_execute(&self) -> Option<()> {
        if self.is_function_execute() {
            Some(())
        } else {
            None
        }
    }

    pub fn as_truncate(&self) -> Option<()> {
        if self.is_truncate() { Some(()) } else { None }
    }
}

/// Trait for implementing object access hooks
pub trait ObjectAccessHook {
    fn on_access(
        &self,
        context: &ObjectAccessContext,
    ) -> Result<(), ObjectAccessHookError>;
}

static REGISTRY: RwLock<Vec<Arc<dyn ObjectAccessHook + Send + Sync>>> =
    RwLock::new(Vec::new());

static PREV_OBJECT_ACCESS_HOOK: OnceLock<pg_sys::object_access_hook_type> =
    OnceLock::new();

/// Register an object access hook
pub fn register_object_access_hook(hook: Box<dyn ObjectAccessHook + Send + Sync>) {
    REGISTRY.write().unwrap().push(Arc::from(hook));

    PREV_OBJECT_ACCESS_HOOK.get_or_init(|| unsafe {
        let prev = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(object_access_router);
        prev
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn object_access_router(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut std::ffi::c_void,
) {
    unsafe {
        let context =
            ObjectAccessContext::new(access, class_id, object_id, sub_id, arg);

        let hooks = { REGISTRY.read().unwrap().clone() };
        for hook in hooks {
            hook.on_access(&context).report_unwrap();
        }

        // Then call previous hook in the chain (if any)
        // This allows our hooks to observe/modify before other extensions
        if let Some(Some(prev)) = PREV_OBJECT_ACCESS_HOOK.get() {
            prev(access, class_id, object_id, sub_id, arg);
        }
    }
}
