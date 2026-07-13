use std::ffi::CStr;
use std::sync::OnceLock;

use pgrx::prelude::*;

use crate::{error::LakebaseError, lifecycle, registry, runtime};

static PREV_PROCESS_UTILITY: OnceLock<pg_sys::ProcessUtility_hook_type> =
    OnceLock::new();

pub(crate) fn init() {
    PREV_PROCESS_UTILITY.get_or_init(|| unsafe {
        let previous = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_hook);
        previous
    });
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // PostgreSQL's ProcessUtility_hook ABI.
unsafe extern "C-unwind" fn process_utility_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::os::raw::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) {
    let node = unsafe { (*pstmt).utilityStmt };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_DropdbStmt => {
            let statement = node.cast::<pg_sys::DropdbStmt>();
            let database_oid =
                unsafe { pg_sys::get_database_oid((*statement).dbname, true) };
            if database_oid != pg_sys::InvalidOid {
                lifecycle::request_database_drop(database_oid.to_u32());
                runtime::stop_database(database_oid.to_u32())
                    .unwrap_or_else(|error| error.report());
            }
        }
        pg_sys::NodeTag::T_CreatedbStmt => lifecycle::request_global_reconcile(),
        pg_sys::NodeTag::T_DropStmt => {
            let statement = unsafe { &*node.cast::<pg_sys::DropStmt>() };
            if statement.removeType == pg_sys::ObjectType::OBJECT_EXTENSION {
                prepare_extension_drops(statement.objects);
            }
        }
        _ => {}
    }

    if let Some(previous) = PREV_PROCESS_UTILITY.get().copied().flatten() {
        unsafe {
            previous(
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
    } else {
        unsafe {
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
}

fn prepare_extension_drops(objects: *mut pg_sys::List) {
    let count = unsafe { pg_sys::list_length(objects) };
    crate::diag::info(format_args!(
        "preparing {count} Lakebase extension drop object(s)"
    ));
    for index in 0..count {
        // PG17's `DROP drop_type_name name_list` grammar stores extension
        // names directly as String nodes, unlike `any_name_list` objects.
        let name_node =
            unsafe { pg_sys::list_nth(objects, index).cast::<pg_sys::String>() };
        if name_node.is_null() || unsafe { (*name_node).sval.is_null() } {
            continue;
        }
        let extension_name = unsafe { CStr::from_ptr((*name_node).sval) };
        let extension_oid =
            unsafe { pg_sys::get_extension_oid(extension_name.as_ptr(), true) };
        crate::diag::info(format_args!(
            "preparing Lakebase extension drop: extension={}, extension_oid={}",
            extension_name.to_string_lossy(),
            extension_oid.to_u32()
        ));
        if extension_oid != pg_sys::InvalidOid {
            prepare_extension_drop(extension_oid, extension_name);
        }
    }
}

fn prepare_extension_drop(extension_oid: pg_sys::Oid, extension_name: &CStr) {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let runtime_oid =
        unsafe { pg_sys::get_extension_oid(c"pg_lakebase_runtime".as_ptr(), true) };
    if runtime_oid == pg_sys::InvalidOid {
        return;
    }
    if extension_oid == runtime_oid {
        lifecycle::request_database_reconcile();
        runtime::stop_database(database_oid).unwrap_or_else(|error| error.report());
        let pending =
            Spi::get_one::<i64>("SELECT count(*) FROM lakebase.maintenance_queue")
                .map_err(|source| LakebaseError::MaintenanceQueueInspection {
                    source,
                })
                .unwrap_or_else(|error| error.report())
                .unwrap_or_else(|| {
                    LakebaseError::MaintenanceQueueCountMissing.report()
                });
        if pending > 0 {
            LakebaseError::MaintenanceQueueNotEmpty { pending }.report();
        }
    } else {
        let has_registrations = registry::extension_has_registrations(extension_name)
            .unwrap_or_else(|error| error.report());
        if !has_registrations {
            return;
        }
        lifecycle::request_database_reconcile();
        runtime::pause_database_reconciliation(database_oid)
            .unwrap_or_else(|error| error.report());
        runtime::stop_extension(database_oid, extension_oid.to_u32())
            .unwrap_or_else(|error| error.report());
        registry::delete_extension_registrations(extension_name)
            .unwrap_or_else(|error| error.report());
    }
}
