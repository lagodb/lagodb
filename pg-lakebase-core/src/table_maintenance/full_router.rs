//! Ordered PostgreSQL 17 `VACUUM FULL` routing.
//!
//! This is deliberately the only raw `VacuumStmt` boundary in the maintenance
//! framework. Ordinary VACUUM stays in `relation_vacuum`; native FULL runs are
//! always delegated to PostgreSQL with already-expanded OIDs.

use std::ffi::c_void;
use std::ptr;

use pgrx::pg_sys;

use crate::diag::ReportableError;
use crate::handles::{RelationHandle, VacuumParamsHandle};
use crate::hooks::utility_hook::ProcessUtilityArgs;
use crate::hooks::error::{HookError, UtilityHookPhase};

use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceRequest, TableMaintenanceRouter,
};

unsafe extern "C" {
    fn lakebase_parse_vacuum_full(
        stmt: *mut pg_sys::VacuumStmt,
        params: *mut pg_sys::VacuumParams,
    ) -> bool;
    fn lakebase_expand_vacuum_relations(
        stmt: *mut pg_sys::VacuumStmt,
        params: *mut pg_sys::VacuumParams,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::List;
    fn lakebase_relation_access_method(relid: pg_sys::Oid) -> pg_sys::Oid;
    fn lakebase_copy_node_to_context(
        node: *const c_void,
        context: pg_sys::MemoryContext,
    ) -> *mut c_void;
    fn lakebase_vacuum_provider_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: *mut pg_sys::VacuumParams,
        callback: unsafe extern "C-unwind" fn(
            pg_sys::Relation,
            *mut pg_sys::VacuumParams,
            *mut c_void,
        ) -> bool,
        context: *mut c_void,
    ) -> i32;
}

#[derive(Clone, Copy)]
struct ProviderContext {
    command_time: TableMaintenanceCommandTime,
    budget: TableMaintenanceBudget,
}

#[pg_guard]
unsafe extern "C-unwind" fn execute_provider(
    relation: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
    context: *mut c_void,
) -> bool {
    unsafe {
        let relation = RelationHandle::from_raw(relation);
        if !TableMaintenanceRouter::is_registered_am(relation.access_method_oid())
            .report_unwrap()
        {
            return false;
        }
        let context = &*(context.cast::<ProviderContext>());
        let params = VacuumParamsHandle::from_raw(params);
        let options = TableMaintenanceOptions::from_vacuum_params(&params);
        TableMaintenanceRouter::execute(TableMaintenanceRequest {
            relation: &relation,
            mode: TableMaintenanceMode::Full,
            options,
            budget: context.budget.without_soft_limit(TableMaintenanceMode::Full),
            command_time: context.command_time,
        })
        .map_err(HookError::from)
        .map_err(|error| {
            error.with_utility_context(
                "TableMaintenanceRouter",
                UtilityHookPhase::Pre,
                pg_sys::NodeTag::T_VacuumStmt,
            )
        })
        .report_unwrap();
        true
    }
}

unsafe fn option_name(option: *mut pg_sys::DefElem) -> &'static [u8] {
    unsafe { std::ffi::CStr::from_ptr((*option).defname) }.to_bytes()
}

unsafe fn append_boolean_option(
    options: *mut pg_sys::List,
    name: &'static std::ffi::CStr,
    value: bool,
) -> *mut pg_sys::List {
    let option = unsafe {
        pg_sys::makeDefElem(
            name.as_ptr().cast_mut(),
            pg_sys::makeBoolean(value).cast(),
            -1,
        )
    };
    unsafe { pg_sys::lappend(options, option.cast()) }
}

unsafe fn set_boolean_option(
    options: *mut pg_sys::List,
    name: &'static std::ffi::CStr,
    value: bool,
) -> *mut pg_sys::List {
    unsafe {
        let count = pg_sys::list_length(options);
        for index in 0..count {
            let option = pg_sys::list_nth(options, index).cast::<pg_sys::DefElem>();
            if std::ffi::CStr::from_ptr((*option).defname) == name {
                (*option).arg = pg_sys::makeBoolean(value).cast();
                return options;
            }
        }
        append_boolean_option(options, name, value)
    }
}

unsafe fn copy_stmt_in_portal(stmt: *mut pg_sys::VacuumStmt) -> *mut pg_sys::VacuumStmt {
    unsafe {
        lakebase_copy_node_to_context(stmt.cast(), pg_sys::PortalContext)
            .cast::<pg_sys::VacuumStmt>()
    }
}

unsafe fn delegate_native_run(
    args: ProcessUtilityArgs,
    original: *mut pg_sys::VacuumStmt,
    relations: *mut pg_sys::List,
) {
    unsafe {
        let stmt = copy_stmt_in_portal(original);
        (*stmt).rels = relations;
        (*stmt).options = set_boolean_option(
            (*stmt).options,
            c"skip_database_stats",
            true,
        );
        args.call_parent_with_node(stmt.cast());
    }
}

unsafe fn delegate_provider_analyze(
    args: ProcessUtilityArgs,
    original: *mut pg_sys::VacuumStmt,
    relation: *mut pg_sys::VacuumRelation,
    params: &pg_sys::VacuumParams,
) {
    unsafe {
        let stmt = copy_stmt_in_portal(original);
        let copied_relation = lakebase_copy_node_to_context(
            relation.cast(),
            pg_sys::PortalContext,
        )
        .cast::<pg_sys::VacuumRelation>();
        (*stmt).rels = pg_sys::lappend(ptr::null_mut(), copied_relation.cast());
        (*stmt).options = ptr::null_mut();
        (*stmt).is_vacuumcmd = false;
        if params.options & pg_sys::VACOPT_VERBOSE != 0 {
            (*stmt).options = append_boolean_option((*stmt).options, c"verbose", true);
        }
        if params.options & pg_sys::VACOPT_SKIP_LOCKED != 0 {
            (*stmt).options = append_boolean_option((*stmt).options, c"skip_locked", true);
        }
        let option_count = pg_sys::list_length((*original).options);
        for index in 0..option_count {
            let option = pg_sys::list_nth((*original).options, index)
                .cast::<pg_sys::DefElem>();
            if option_name(option) == b"buffer_usage_limit" {
                let copied = pg_sys::copyObjectImpl(option.cast()) as *mut pg_sys::DefElem;
                (*stmt).options = pg_sys::lappend((*stmt).options, copied.cast());
            }
        }
        args.call_parent_with_node(stmt.cast());
    }
}

/// Return true only when the FULL statement was consumed.
pub(crate) unsafe fn try_route_vacuum_full(
    stmt: *mut pg_sys::VacuumStmt,
    args: ProcessUtilityArgs,
    is_top_level: bool,
) -> bool {
    unsafe {
        let mut params = pg_sys::VacuumParams::default();
        if !lakebase_parse_vacuum_full(stmt, &mut params) {
            return false;
        }
        if !TableMaintenanceRouter::has_providers() {
            return false;
        }
        if params.options & pg_sys::VACOPT_ONLY_DATABASE_STATS != 0 {
            return false;
        }

        pg_sys::PreventInTransactionBlock(is_top_level, c"VACUUM".as_ptr());
        let command_time = TableMaintenanceCommandTime::now().report_unwrap();
        let expanded = lakebase_expand_vacuum_relations(
            stmt,
            &mut params,
            pg_sys::PortalContext,
        );
        let mut provider_context = ProviderContext {
            command_time,
            budget: TableMaintenanceBudget::configured(),
        };

        let mut native_run: *mut pg_sys::List = ptr::null_mut();
        let count = pg_sys::list_length(expanded);
        for index in 0..count {
            let relation = pg_sys::list_nth(expanded, index)
                .cast::<pg_sys::VacuumRelation>();
            let am = lakebase_relation_access_method((*relation).oid);
            let is_provider = am != pg_sys::InvalidOid
                && TableMaintenanceRouter::is_registered_am(am).report_unwrap();
            if !is_provider {
                native_run = pg_sys::lappend(native_run, relation.cast());
                continue;
            }

            if !native_run.is_null() {
                delegate_native_run(args, stmt, native_run);
                native_run = ptr::null_mut();
            }
            let result = lakebase_vacuum_provider_relation(
                relation,
                &mut params,
                execute_provider,
                (&mut provider_context as *mut ProviderContext).cast(),
            );
            if result == 0 {
                let single = pg_sys::lappend(ptr::null_mut(), relation.cast());
                delegate_native_run(args, stmt, single);
            } else if result > 0 && params.options & pg_sys::VACOPT_ANALYZE != 0 {
                delegate_provider_analyze(args, stmt, relation, &params);
            }
        }
        if !native_run.is_null() {
            delegate_native_run(args, stmt, native_run);
        }
        if params.options & pg_sys::VACOPT_SKIP_DATABASE_STATS == 0 {
            pg_sys::vac_update_datfrozenxid();
        }
        args.complete_vacuum();
        true
    }
}
