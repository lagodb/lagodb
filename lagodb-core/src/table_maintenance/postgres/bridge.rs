//! Maintenance command preparation and execution through the C bridges.
//!
//! These hand-written extern declarations do not pass through pgrx bindgen.
//! Calls that can raise ERROR or invoke a Rust callback therefore need an FFI
//! guard. Ordinary `pg_sys` calls already have that guard and are not wrapped
//! here. Each guarded closure only calls C; Rust-owned state stays outside it.

use std::ffi::{c_char, c_void};

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use super::{
    MaintenanceCommand, MaintenanceProbeCallback as VacuumProbeCallback,
    MaintenanceProviderCallback as ProviderCallback,
};

unsafe extern "C-unwind" {
    fn lagodb_vacuum_requests_full(stmt: *mut pg_sys::VacuumStmt) -> bool;
    fn lagodb_parse_vacuum_options(
        stmt: *mut pg_sys::VacuumStmt,
        query_string: *const c_char,
        query_env: *mut pg_sys::QueryEnvironment,
        params: *mut pg_sys::VacuumParams,
    );
    fn lagodb_expand_vacuum_relations(
        stmt: *mut pg_sys::VacuumStmt,
        params: *mut pg_sys::VacuumParams,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::List;
    fn lagodb_make_vacuum_stmt(
        stmt: *mut pg_sys::VacuumStmt,
        relations: *mut pg_sys::List,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::VacuumStmt;
    fn lagodb_make_analyze_stmt(
        stmt: *mut pg_sys::VacuumStmt,
        relations: *mut pg_sys::List,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::VacuumStmt;
    fn lagodb_check_maintenance_command_state();
    fn lagodb_vacuum_probe(
        stmt: *mut pg_sys::VacuumStmt,
        callback: VacuumProbeCallback,
    ) -> bool;
    fn lagodb_vacuum_relation_am(relation_oid: pg_sys::Oid) -> pg_sys::Oid;
    fn lagodb_vacuum_full_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: *mut pg_sys::VacuumParams,
        provider_callback: ProviderCallback,
        context: *mut c_void,
    ) -> bool;
}

pub(super) struct MaintenanceBridge;

impl MaintenanceBridge {
    /// # Safety
    /// The statement must belong to the live backend utility invocation.
    pub(super) unsafe fn requests_full(stmt: *mut pg_sys::VacuumStmt) -> bool {
        // SAFETY: defGetBoolean can raise ERROR on the live option list.
        unsafe { pg_guard_ffi_boundary(|| lagodb_vacuum_requests_full(stmt)) }
    }

    /// # Safety
    /// Inputs must belong to the live backend utility invocation, and
    /// `is_top_level` must describe its ProcessUtility context.
    pub(super) unsafe fn prepare_command(
        stmt: *mut pg_sys::VacuumStmt,
        query_string: *const c_char,
        query_env: *mut pg_sys::QueryEnvironment,
        is_top_level: bool,
    ) -> MaintenanceCommand {
        let mut command = MaintenanceCommand {
            params: pg_sys::VacuumParams::default(),
        };
        // SAFETY: inputs are live on the backend thread and the C parser has
        // exclusive access to the outputs. The guard contains only C calls;
        // PreventInTransactionBlock already has pgrx's generated protection.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_check_maintenance_command_state();
                lagodb_parse_vacuum_options(
                    stmt,
                    query_string,
                    query_env,
                    &mut command.params,
                );
            });
            pg_sys::PreventInTransactionBlock(is_top_level, c"VACUUM".as_ptr());
        }
        command
    }

    /// # Safety
    /// The statement and callback must be live for the synchronous backend
    /// call, after command preparation and before name resolution.
    pub(super) unsafe fn probe_vacuum(
        stmt: *mut pg_sys::VacuumStmt,
        callback: VacuumProbeCallback,
    ) -> bool {
        // SAFETY: the caller keeps the statement and guarded callback live.
        unsafe { pg_guard_ffi_boundary(|| lagodb_vacuum_probe(stmt, callback)) }
    }

    /// # Safety
    /// Must run on the backend thread inside a transaction.
    pub(super) unsafe fn relation_am(relation_oid: pg_sys::Oid) -> pg_sys::Oid {
        // SAFETY: the syscache lookup can raise ERROR; the closure only calls C.
        unsafe { pg_guard_ffi_boundary(|| lagodb_vacuum_relation_am(relation_oid)) }
    }

    /// # Safety
    /// The statement, params and allocation context must be live on the backend
    /// thread. Input names and column lists must survive maintenance execution.
    pub(super) unsafe fn expand_relations(
        stmt: *mut pg_sys::VacuumStmt,
        params: &mut pg_sys::VacuumParams,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::List {
        // SAFETY: the caller supplies live inputs on the backend thread, where
        // the maintenance plan owns the list across maintenance transactions.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_expand_vacuum_relations(stmt, params, context)
            })
        }
    }

    /// # Safety
    /// The statement and `relations` must be live, and `context` must outlive
    /// the returned statement.
    pub(super) unsafe fn vacuum_stmt(
        stmt: *mut pg_sys::VacuumStmt,
        relations: *mut pg_sys::List,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::VacuumStmt {
        // SAFETY: the supplied nodes are live; the closure only calls C.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_make_vacuum_stmt(stmt, relations, context)
            })
        }
    }

    /// # Safety
    /// The statement and `relations` must be live, and `context` must outlive
    /// the returned statement.
    pub(super) unsafe fn analyze_stmt(
        stmt: *mut pg_sys::VacuumStmt,
        relations: *mut pg_sys::List,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::VacuumStmt {
        // SAFETY: the supplied nodes are live; the closure only calls C.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_make_analyze_stmt(stmt, relations, context)
            })
        }
    }

    /// # Safety
    /// Arguments and the callback must stay live through the synchronous
    /// backend call. No transaction may be active at entry; none is at exit.
    pub(super) unsafe fn vacuum_full_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: &mut pg_sys::VacuumParams,
        provider_callback: ProviderCallback,
        context: *mut c_void,
    ) -> bool {
        // SAFETY: the caller maintains the transaction contract and keeps its
        // state outside the guard; the Rust callback has pg_guard.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_vacuum_full_relation(
                    relation,
                    params,
                    provider_callback,
                    context,
                )
            })
        }
    }
}
