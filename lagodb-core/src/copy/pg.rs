//! Narrow C bridge for PostgreSQL COPY states and local opaque adapters.
//!
//! The Rust-facing symbols remain version-neutral. The current C implementation
//! is audited for PostgreSQL 17; the build configuration and local
//! `LAGODB_PG17` gates reject other majors until their corresponding source
//! branches have been ported and reviewed.

use std::ffi::c_void;
use std::ptr::NonNull;

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use super::CopyEndpoint;

pub(crate) type TypedCopySourceCallback =
    unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::Datum,
        *mut bool,
        *mut u64,
        *mut u64,
        *mut *const std::ffi::c_char,
        *mut *const std::ffi::c_char,
        *mut std::ffi::c_int,
        *mut std::ffi::c_int,
    ) -> std::ffi::c_int;
pub(crate) type TypedCopyDestinationCallback =
    unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::TupleTableSlot, *mut u64);

#[repr(C)]
pub(crate) struct LagodbCopyPreparation {
    pub(crate) relation: pg_sys::Relation,
    pub(crate) where_clause: *mut pg_sys::Node,
    pub(crate) raw_query: *mut pg_sys::RawStmt,
    pub(crate) query_rel_id: pg_sys::Oid,
}

/// Opaque local encoder state; its layout and allocation are owned by C.
#[repr(C)]
pub(crate) struct LagodbCopyRowEncoder {
    _private: [u8; 0],
}

unsafe extern "C-unwind" {
    fn lagodb_prepare_copy_from(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        endpoint: CopyEndpoint,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LagodbCopyPreparation,
    );

    fn lagodb_prepare_copy_to(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        endpoint: CopyEndpoint,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LagodbCopyPreparation,
    );

    fn lagodb_dispose_copy_preparation(preparation: *mut LagodbCopyPreparation);

    fn lagodb_begin_copy_from(
        pstate: *mut pg_sys::ParseState,
        rel: pg_sys::Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_source_cb: pg_sys::copy_data_source_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyFromState;

    fn lagodb_next_copy_from(
        state: pg_sys::CopyFromState,
        econtext: *mut pg_sys::ExprContext,
        values: *mut pg_sys::Datum,
        nulls: *mut bool,
    ) -> bool;

    fn lagodb_end_copy_from(state: pg_sys::CopyFromState);

    fn lagodb_begin_routed_copy_from(
        pstate: *mut pg_sys::ParseState,
        rel: pg_sys::Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_source_cb: pg_sys::copy_data_source_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
        typed_input: bool,
    ) -> pg_sys::CopyFromState;
    fn lagodb_execute_routed_copy_from(
        state: pg_sys::CopyFromState,
        typed_source: Option<TypedCopySourceCallback>,
        typed_source_context: *mut c_void,
        provider_owned_partitioned_table: bool,
    ) -> u64;
    fn lagodb_end_routed_copy_from(state: pg_sys::CopyFromState);
    fn lagodb_routed_copy_from_tuple_desc(
        state: pg_sys::CopyFromState,
    ) -> pg_sys::TupleDesc;
    fn lagodb_routed_copy_from_attnums(
        state: pg_sys::CopyFromState,
    ) -> *mut pg_sys::List;

    fn lagodb_begin_copy_row_encoder(
        rel: pg_sys::Relation,
        options: *mut pg_sys::List,
    ) -> *mut LagodbCopyRowEncoder;

    fn lagodb_encode_copy_header(
        state: *mut LagodbCopyRowEncoder,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    );

    fn lagodb_encode_copy_row(
        state: *mut LagodbCopyRowEncoder,
        slot: *mut pg_sys::TupleTableSlot,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    );

    fn lagodb_end_copy_row_encoder(state: *mut LagodbCopyRowEncoder);

    fn lagodb_begin_routed_copy_to(
        pstate: *mut pg_sys::ParseState,
        rel: pg_sys::Relation,
        raw_query: *mut pg_sys::RawStmt,
        query_rel_id: pg_sys::Oid,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_dest_cb: pg_sys::copy_data_dest_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
        provider_owned_partitioned_table: bool,
        typed_output: bool,
    ) -> pg_sys::CopyToState;
    fn lagodb_execute_routed_copy_to(
        state: pg_sys::CopyToState,
        typed_destination: Option<TypedCopyDestinationCallback>,
        typed_destination_context: *mut c_void,
    ) -> u64;
    fn lagodb_end_routed_copy_to(state: pg_sys::CopyToState, is_error: bool);
    fn lagodb_finish_routed_copy_to(state: pg_sys::CopyToState);
    fn lagodb_update_routed_copy_to_progress(
        state: pg_sys::CopyToState,
        bytes_produced: u64,
    );
    fn lagodb_routed_copy_to_tuple_desc(
        state: pg_sys::CopyToState,
    ) -> pg_sys::TupleDesc;
    fn lagodb_routed_copy_to_attnums(state: pg_sys::CopyToState)
    -> *mut pg_sys::List;

    fn lagodb_routed_copy_to_has_header(state: pg_sys::CopyToState) -> bool;

    fn lagodb_copy_get_attnums(
        rel: pg_sys::Relation,
        attnamelist: *mut pg_sys::List,
    ) -> *mut pg_sys::List;

    fn lagodb_begin_raw_field_reader(
        data_source_cb: pg_sys::copy_data_source_cb,
        options: *mut pg_sys::List,
    ) -> *mut std::ffi::c_void;

    fn lagodb_next_raw_fields(
        reader: *mut std::ffi::c_void,
        fields: *mut *mut *mut std::ffi::c_char,
        field_count: *mut usize,
    ) -> bool;

    fn lagodb_end_raw_field_reader(reader: *mut std::ffi::c_void);

    fn lagodb_begin_text_input_validator(
        type_oid: pg_sys::Oid,
    ) -> *mut std::ffi::c_void;

    fn lagodb_text_input_accepts(
        validator: *mut std::ffi::c_void,
        value: *const std::ffi::c_char,
    ) -> bool;

    fn lagodb_end_text_input_validator(validator: *mut std::ffi::c_void);

    fn lagodb_begin_copy_datum_coercion(
        type_oid: pg_sys::Oid,
        source_typmod: i32,
        target_typmod: i32,
    ) -> *mut c_void;
    fn lagodb_coerce_copy_datum(
        state: *mut c_void,
        value: pg_sys::Datum,
    ) -> pg_sys::Datum;
    fn lagodb_end_copy_datum_coercion(state: *mut c_void);
}

/// PostgreSQL ERROR boundary for the local opaque-COPY bridge.
///
/// pgrx generates this boundary for `pg_sys` bindings, but these functions are
/// local C symbols and therefore must establish it explicitly. Every closure
/// below contains only the C call. The boundary converts a PostgreSQL longjmp
/// into a Rust panic, which the owning COPY driver catches so Rust cleanup runs
/// normally.
pub(crate) struct CopyBridge;

/// Successful shutdown runs the executor; abort only releases COPY storage.
#[derive(Clone, Copy)]
pub(crate) enum CopyToShutdown {
    Complete,
    Abort,
}

impl CopyBridge {
    pub(crate) unsafe fn begin_datum_coercion(
        type_oid: pg_sys::Oid,
        source_typmod: i32,
        target_typmod: i32,
    ) -> *mut c_void {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_begin_copy_datum_coercion(
                    type_oid,
                    source_typmod,
                    target_typmod,
                )
            })
        }
    }

    pub(crate) unsafe fn coerce_datum(
        state: *mut c_void,
        value: pg_sys::Datum,
    ) -> pg_sys::Datum {
        unsafe { pg_guard_ffi_boundary(|| lagodb_coerce_copy_datum(state, value)) }
    }

    pub(crate) unsafe fn end_datum_coercion(state: *mut c_void) {
        unsafe { pg_guard_ffi_boundary(|| lagodb_end_copy_datum_coercion(state)) }
    }

    pub(crate) unsafe fn begin_routed_from(
        pstate: *mut pg_sys::ParseState,
        relation: pg_sys::Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_source_cb: pg_sys::copy_data_source_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
        typed_input: bool,
    ) -> pg_sys::CopyFromState {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_begin_routed_copy_from(
                    pstate,
                    relation,
                    where_clause,
                    filename,
                    is_program,
                    data_source_cb,
                    attnamelist,
                    options,
                    typed_input,
                )
            })
        }
    }

    pub(crate) unsafe fn execute_routed_from_bytes(
        state: pg_sys::CopyFromState,
        provider_owned_partitioned_table: bool,
    ) -> u64 {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_routed_copy_from(
                    state,
                    None,
                    std::ptr::null_mut(),
                    provider_owned_partitioned_table,
                )
            })
        }
    }

    pub(crate) unsafe fn execute_routed_from_typed(
        state: pg_sys::CopyFromState,
        typed_source: TypedCopySourceCallback,
        typed_source_context: NonNull<c_void>,
        provider_owned_partitioned_table: bool,
    ) -> u64 {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_routed_copy_from(
                    state,
                    Some(typed_source),
                    typed_source_context.as_ptr(),
                    provider_owned_partitioned_table,
                )
            })
        }
    }

    pub(crate) unsafe fn end_routed_from(state: pg_sys::CopyFromState) {
        unsafe { pg_guard_ffi_boundary(|| lagodb_end_routed_copy_from(state)) }
    }

    pub(crate) unsafe fn routed_from_tuple_desc(
        state: pg_sys::CopyFromState,
    ) -> pg_sys::TupleDesc {
        unsafe { pg_guard_ffi_boundary(|| lagodb_routed_copy_from_tuple_desc(state)) }
    }

    pub(crate) unsafe fn routed_from_attnums(
        state: pg_sys::CopyFromState,
    ) -> *mut pg_sys::List {
        unsafe { pg_guard_ffi_boundary(|| lagodb_routed_copy_from_attnums(state)) }
    }

    pub(crate) unsafe fn begin_routed_to(
        pstate: *mut pg_sys::ParseState,
        relation: pg_sys::Relation,
        raw_query: *mut pg_sys::RawStmt,
        query_relation: pg_sys::Oid,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_dest_cb: pg_sys::copy_data_dest_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
        provider_owned_partitioned_table: bool,
        typed_output: bool,
    ) -> pg_sys::CopyToState {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_begin_routed_copy_to(
                    pstate,
                    relation,
                    raw_query,
                    query_relation,
                    filename,
                    is_program,
                    data_dest_cb,
                    attnamelist,
                    options,
                    provider_owned_partitioned_table,
                    typed_output,
                )
            })
        }
    }

    pub(crate) unsafe fn execute_routed_to_bytes(state: pg_sys::CopyToState) -> u64 {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_routed_copy_to(state, None, std::ptr::null_mut())
            })
        }
    }

    pub(crate) unsafe fn execute_routed_to_typed(
        state: pg_sys::CopyToState,
        typed_destination: TypedCopyDestinationCallback,
        typed_destination_context: NonNull<c_void>,
    ) -> u64 {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_routed_copy_to(
                    state,
                    Some(typed_destination),
                    typed_destination_context.as_ptr(),
                )
            })
        }
    }

    pub(crate) unsafe fn end_routed_to(
        state: pg_sys::CopyToState,
        shutdown: CopyToShutdown,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_end_routed_copy_to(
                    state,
                    matches!(shutdown, CopyToShutdown::Abort),
                )
            })
        }
    }

    pub(crate) unsafe fn finish_routed_to(state: pg_sys::CopyToState) {
        unsafe { pg_guard_ffi_boundary(|| lagodb_finish_routed_copy_to(state)) }
    }

    pub(crate) unsafe fn update_routed_to_progress(
        state: pg_sys::CopyToState,
        bytes_produced: u64,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_update_routed_copy_to_progress(state, bytes_produced)
            })
        }
    }

    pub(crate) unsafe fn routed_to_tuple_desc(
        state: pg_sys::CopyToState,
    ) -> pg_sys::TupleDesc {
        unsafe { pg_guard_ffi_boundary(|| lagodb_routed_copy_to_tuple_desc(state)) }
    }

    pub(crate) unsafe fn routed_to_attnums(
        state: pg_sys::CopyToState,
    ) -> *mut pg_sys::List {
        unsafe { pg_guard_ffi_boundary(|| lagodb_routed_copy_to_attnums(state)) }
    }

    pub(crate) unsafe fn routed_to_has_header(state: pg_sys::CopyToState) -> bool {
        unsafe { pg_guard_ffi_boundary(|| lagodb_routed_copy_to_has_header(state)) }
    }

    pub(crate) unsafe fn prepare_from(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        endpoint: CopyEndpoint,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LagodbCopyPreparation,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_prepare_copy_from(
                    pstate,
                    statement,
                    endpoint,
                    stmt_location,
                    stmt_len,
                    preparation,
                );
            });
        }
    }

    pub(crate) unsafe fn prepare_to(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        endpoint: CopyEndpoint,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LagodbCopyPreparation,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_prepare_copy_to(
                    pstate,
                    statement,
                    endpoint,
                    stmt_location,
                    stmt_len,
                    preparation,
                );
            });
        }
    }

    pub(crate) unsafe fn dispose_preparation(
        preparation: *mut LagodbCopyPreparation,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_dispose_copy_preparation(preparation);
            });
        }
    }

    pub(crate) unsafe fn begin_from(
        pstate: *mut pg_sys::ParseState,
        relation: pg_sys::Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_source_cb: pg_sys::copy_data_source_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyFromState {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_begin_copy_from(
                    pstate,
                    relation,
                    where_clause,
                    filename,
                    is_program,
                    data_source_cb,
                    attnamelist,
                    options,
                )
            })
        }
    }

    pub(crate) unsafe fn next_from(
        state: pg_sys::CopyFromState,
        econtext: *mut pg_sys::ExprContext,
        values: *mut pg_sys::Datum,
        nulls: *mut bool,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_next_copy_from(state, econtext, values, nulls)
            })
        }
    }

    pub(crate) unsafe fn end_from(state: pg_sys::CopyFromState) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_end_copy_from(state);
            });
        }
    }

    pub(crate) unsafe fn begin_row_encoder(
        relation: pg_sys::Relation,
        options: *mut pg_sys::List,
    ) -> *mut LagodbCopyRowEncoder {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_begin_copy_row_encoder(relation, options))
        }
    }

    pub(crate) unsafe fn encode_copy_header(
        state: *mut LagodbCopyRowEncoder,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_encode_copy_header(state, data, len));
        }
    }

    pub(crate) unsafe fn encode_copy_row(
        state: *mut LagodbCopyRowEncoder,
        slot: *mut pg_sys::TupleTableSlot,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_encode_copy_row(state, slot, data, len));
        }
    }

    pub(crate) unsafe fn end_row_encoder(state: *mut LagodbCopyRowEncoder) {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_end_copy_row_encoder(state));
        }
    }

    pub(crate) unsafe fn copy_attnums(
        relation: pg_sys::Relation,
        attnamelist: *mut pg_sys::List,
    ) -> *mut pg_sys::List {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_copy_get_attnums(relation, attnamelist))
        }
    }

    pub(crate) unsafe fn begin_raw_field_reader(
        data_source_cb: pg_sys::copy_data_source_cb,
        options: *mut pg_sys::List,
    ) -> *mut std::ffi::c_void {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_begin_raw_field_reader(data_source_cb, options)
            })
        }
    }

    pub(crate) unsafe fn next_raw_fields(
        reader: *mut std::ffi::c_void,
        fields: *mut *mut *mut std::ffi::c_char,
        field_count: *mut usize,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_next_raw_fields(reader, fields, field_count)
            })
        }
    }

    pub(crate) unsafe fn end_raw_field_reader(reader: *mut std::ffi::c_void) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_end_raw_field_reader(reader);
            });
        }
    }

    pub(crate) unsafe fn begin_text_input_validator(
        type_oid: pg_sys::Oid,
    ) -> *mut std::ffi::c_void {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_begin_text_input_validator(type_oid))
        }
    }

    pub(crate) unsafe fn text_input_accepts(
        validator: *mut std::ffi::c_void,
        value: *const std::ffi::c_char,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_text_input_accepts(validator, value))
        }
    }

    pub(crate) unsafe fn end_text_input_validator(validator: *mut std::ffi::c_void) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_end_text_input_validator(validator);
            });
        }
    }
}
