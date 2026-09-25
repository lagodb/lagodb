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
    MaintenanceRouteCallback as RouteCallback,
};

unsafe extern "C-unwind" {
    fn lagodb_parse_vacuum_options(
        stmt: *mut pg_sys::VacuumStmt,
        query_string: *const c_char,
        query_env: *mut pg_sys::QueryEnvironment,
        params: *mut pg_sys::VacuumParams,
        ring_size_kb: *mut i32,
    );
    fn lagodb_expand_vacuum_relations(
        stmt: *mut pg_sys::VacuumStmt,
        params: *mut pg_sys::VacuumParams,
        context: pg_sys::MemoryContext,
    ) -> *mut pg_sys::List;
    fn lagodb_make_vacuum_buffer_strategy(
        ring_size_kb: i32,
        context: pg_sys::MemoryContext,
    ) -> pg_sys::BufferAccessStrategy;
    fn lagodb_check_maintenance_command_state(stmt: *mut pg_sys::VacuumStmt);
    fn lagodb_vacuum_probe(
        stmt: *mut pg_sys::VacuumStmt,
        options: pg_sys::bits32,
        callback: VacuumProbeCallback,
    ) -> bool;
    fn lagodb_vacuum_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: *mut pg_sys::VacuumParams,
        bstrategy: pg_sys::BufferAccessStrategy,
        route_callback: RouteCallback,
        provider_callback: ProviderCallback,
        context: *mut c_void,
    ) -> bool;
    fn lagodb_analyze_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: *mut pg_sys::VacuumParams,
        in_outer_xact: bool,
        bstrategy: pg_sys::BufferAccessStrategy,
        route_callback: RouteCallback,
    );
    fn lagodb_execute_truncate(
        stmt: *mut pg_sys::TruncateStmt,
        owns_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation) -> bool,
        truncate_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation),
    );
    fn lagodb_initialize_maintenance_costs();
    fn lagodb_finish_maintenance_costs();
}

pub(super) struct MaintenanceBridge;

impl MaintenanceBridge {
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
            ring_size_kb: -1,
        };
        // SAFETY: inputs are live on the backend thread and the C parser has
        // exclusive access to the outputs. The guard contains only C calls;
        // PreventInTransactionBlock already has pgrx's generated protection.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_check_maintenance_command_state(stmt);
                lagodb_parse_vacuum_options(
                    stmt,
                    query_string,
                    query_env,
                    &mut command.params,
                    &mut command.ring_size_kb,
                );
            });
            if command.params.options & pg_sys::VACOPT_VACUUM != 0 {
                pg_sys::PreventInTransactionBlock(is_top_level, c"VACUUM".as_ptr());
            }
        }
        command
    }

    /// # Safety
    /// The statement and callback must be live for the synchronous backend
    /// call. Options must come from command preparation before name resolution.
    pub(super) unsafe fn probe_vacuum(
        stmt: *mut pg_sys::VacuumStmt,
        options: pg_sys::bits32,
        callback: VacuumProbeCallback,
    ) -> bool {
        // SAFETY: the caller keeps the statement and guarded callback live.
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_vacuum_probe(stmt, options, callback))
        }
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
    /// The allocation context must be live on the PostgreSQL backend thread
    /// and survive all uses of the returned buffer strategy.
    pub(super) unsafe fn buffer_strategy(
        ring_size_kb: i32,
        context: pg_sys::MemoryContext,
    ) -> pg_sys::BufferAccessStrategy {
        // SAFETY: the supplied context is live; the closure only calls C.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_make_vacuum_buffer_strategy(ring_size_kb, context)
            })
        }
    }

    /// # Safety
    /// Arguments and callbacks must stay live through the synchronous backend
    /// call. The caller must follow vacuum_rel's transaction boundary contract.
    /// The runtime maintenance command scope must be active throughout the call.
    pub(super) unsafe fn vacuum_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: &mut pg_sys::VacuumParams,
        bstrategy: pg_sys::BufferAccessStrategy,
        route_callback: RouteCallback,
        provider_callback: ProviderCallback,
        context: *mut c_void,
    ) -> bool {
        // SAFETY: the caller maintains vacuum_rel's transaction contract and
        // keeps its state outside the guard; Rust callbacks have pg_guard.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_vacuum_relation(
                    relation,
                    params,
                    bstrategy,
                    route_callback,
                    provider_callback,
                    context,
                )
            })
        }
    }

    /// # Safety
    /// Arguments and the callback must be live in an active backend transaction
    /// with the ANALYZE snapshot installed by the caller.
    /// The runtime maintenance command scope must be active throughout the call.
    pub(super) unsafe fn analyze_relation(
        relation: *mut pg_sys::VacuumRelation,
        params: &mut pg_sys::VacuumParams,
        in_outer_xact: bool,
        bstrategy: pg_sys::BufferAccessStrategy,
        route_callback: RouteCallback,
    ) {
        // SAFETY: the caller owns the transaction/snapshot and live inputs;
        // the route callback's pg_guard prevents a panic escaping through C.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_analyze_relation(
                    relation,
                    params,
                    in_outer_xact,
                    bstrategy,
                    route_callback,
                )
            })
        }
    }

    /// # Safety
    /// The statement and callbacks must be live in the backend utility invocation.
    pub(super) unsafe fn truncate(
        stmt: *mut pg_sys::TruncateStmt,
        owns_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation) -> bool,
        truncate_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation),
    ) {
        // SAFETY: all inputs remain live, both Rust callbacks have pg_guard,
        // and the guarded closure only invokes the C TRUNCATE executor.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_truncate(
                    stmt,
                    owns_partitioned_table,
                    truncate_partitioned_table,
                )
            })
        }
    }

    /// # Safety
    /// Called only by the synchronous maintenance executor on the backend thread.
    pub(super) unsafe fn initialize_costs() {
        // SAFETY: this C function only assigns backend globals on the caller's
        // backend thread. It cannot raise ERROR
        // or call Rust, so it does not need an additional FFI guard.
        unsafe { lagodb_initialize_maintenance_costs() }
    }

    /// # Safety
    /// Called only by the synchronous maintenance executor on the backend thread.
    pub(super) unsafe fn finish_costs() {
        // SAFETY: like initialization, this only assigns globals on the backend thread.
        unsafe { lagodb_finish_maintenance_costs() }
    }
}
