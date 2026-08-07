use std::cell::Cell;
use std::ffi::CStr;

use pgrx::prelude::*;

use crate::{error::LakebaseError, lifecycle, registry, worker};

thread_local! {
    static PREFLIGHT_ENABLED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn init() {
    PREFLIGHT_ENABLED.set(true);
}

pub(crate) unsafe fn preflight(node: *mut pg_sys::Node) {
    if !PREFLIGHT_ENABLED.get() {
        return;
    }
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_DropdbStmt => {
            let statement = node.cast::<pg_sys::DropdbStmt>();
            let database_oid =
                unsafe { pg_sys::get_database_oid((*statement).dbname, true) };
            if database_oid != pg_sys::InvalidOid {
                worker::DatabaseLifecycleLock::new(database_oid.to_u32())
                    .acquire_drop();
                lifecycle::request_database_drop(database_oid.to_u32());
                worker::prepare_database_drop(database_oid.to_u32());
            }
        }
        pg_sys::NodeTag::T_CreatedbStmt => lifecycle::request_global_reconcile(),
        pg_sys::NodeTag::T_AlterDatabaseSetStmt => {
            let statement = unsafe { &*node.cast::<pg_sys::AlterDatabaseSetStmt>() };
            let database_oid =
                unsafe { pg_sys::get_database_oid(statement.dbname, false) };
            lifecycle::request_database_workers_wakeup(database_oid.to_u32());
        }
        _ => {}
    }
}

pub(crate) fn drop_extension_workers(
    extension_oid: pg_sys::Oid,
) -> Result<(), LakebaseError> {
    // SAFETY: PostgreSQL invokes OAT_DROP before deleting the pg_extension
    // catalog tuple, so the extension OID is still resolvable here.
    let extension_name = unsafe { pg_sys::get_extension_name(extension_oid) };
    if extension_name.is_null() {
        return Ok(());
    }
    // SAFETY: PostgreSQL returned a non-null NUL-terminated extension name.
    let extension_name = unsafe { CStr::from_ptr(extension_name) };
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let runtime_oid =
        unsafe { pg_sys::get_extension_oid(c"pg_lakebase_runtime".as_ptr(), true) };
    if runtime_oid == pg_sys::InvalidOid {
        return Ok(());
    }
    let runtime_preloaded = worker::is_preloaded();
    if extension_oid == runtime_oid {
        if runtime_preloaded {
            worker::DatabaseLifecycleLock::new(database_oid).acquire_drop();
            lifecycle::request_database_reconcile();
            worker::prepare_database_drop(database_oid);
        }
    } else {
        let has_registrations =
            registry::extension_has_registrations(extension_name)?;
        if !has_registrations {
            return Ok(());
        }
        registry::delete_extension_registrations(extension_name)?;
        if runtime_preloaded {
            worker::DatabaseLifecycleLock::new(database_oid).acquire_drop();
            lifecycle::request_database_reconcile();
            worker::prepare_extension_drop(database_oid, extension_oid.to_u32());
        }
    }
    Ok(())
}
