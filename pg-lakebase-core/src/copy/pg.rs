//! Narrow PostgreSQL 17 C bridge for PostgreSQL's opaque COPY states.
//!
//! The public COPY entry points are not a stable cross-major ABI. Core exposes
//! this module only for `pg17`, the build script compiles its C bridge only for
//! `pg17`, and the shared C compatibility header rejects every other major. A
//! new major must provide and review a separate adapter before its Cargo
//! feature is enabled here.

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

#[repr(C)]
pub(crate) struct LakebaseCopyPreparation {
    pub(crate) relation: pg_sys::Relation,
    pub(crate) where_clause: *mut pg_sys::Node,
    pub(crate) raw_query: *mut pg_sys::RawStmt,
    pub(crate) query_rel_id: pg_sys::Oid,
}

unsafe extern "C-unwind" {
    fn lakebase_prepare_copy_from(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LakebaseCopyPreparation,
    );

    fn lakebase_prepare_copy_to(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LakebaseCopyPreparation,
    );

    fn lakebase_dispose_copy_preparation(
        preparation: *mut LakebaseCopyPreparation,
    );

    fn lakebase_begin_copy_from(
        pstate: *mut pg_sys::ParseState,
        rel: pg_sys::Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_source_cb: pg_sys::copy_data_source_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyFromState;

    fn lakebase_next_copy_from(
        state: pg_sys::CopyFromState,
        econtext: *mut pg_sys::ExprContext,
        values: *mut pg_sys::Datum,
        nulls: *mut bool,
    ) -> bool;

    fn lakebase_copy_from(state: pg_sys::CopyFromState) -> u64;

    fn lakebase_end_copy_from(state: pg_sys::CopyFromState);

    fn lakebase_begin_copy_row_encoder(
        rel: pg_sys::Relation,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyToState;

    fn lakebase_encode_copy_header(
        state: pg_sys::CopyToState,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    );

    fn lakebase_encode_copy_row(
        state: pg_sys::CopyToState,
        slot: *mut pg_sys::TupleTableSlot,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    );

    fn lakebase_end_copy_row_encoder(
        state: pg_sys::CopyToState,
    );

    fn lakebase_begin_copy_to(
        pstate: *mut pg_sys::ParseState,
        rel: pg_sys::Relation,
        raw_query: *mut pg_sys::RawStmt,
        query_rel_id: pg_sys::Oid,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_dest_cb: pg_sys::copy_data_dest_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyToState;

    fn lakebase_copy_to(state: pg_sys::CopyToState) -> u64;

    fn lakebase_end_copy_to(state: pg_sys::CopyToState);

    fn lakebase_copy_get_attnums(
        rel: pg_sys::Relation,
        attnamelist: *mut pg_sys::List,
    ) -> *mut pg_sys::List;

    fn lakebase_copy_to_tuple_desc(
        state: pg_sys::CopyToState,
    ) -> pg_sys::TupleDesc;

    fn lakebase_copy_to_attnums(state: pg_sys::CopyToState) -> *mut pg_sys::List;

    fn lakebase_begin_raw_field_reader(
        data_source_cb: pg_sys::copy_data_source_cb,
        options: *mut pg_sys::List,
    ) -> *mut std::ffi::c_void;

    fn lakebase_next_raw_fields(
        reader: *mut std::ffi::c_void,
        fields: *mut *mut *mut std::ffi::c_char,
        field_count: *mut usize,
    ) -> bool;

    fn lakebase_end_raw_field_reader(reader: *mut std::ffi::c_void);

    fn lakebase_begin_text_input_validator(
        type_oid: pg_sys::Oid,
    ) -> *mut std::ffi::c_void;

    fn lakebase_text_input_accepts(
        validator: *mut std::ffi::c_void,
        value: *const std::ffi::c_char,
    ) -> bool;

    fn lakebase_end_text_input_validator(validator: *mut std::ffi::c_void);
}

/// PostgreSQL ERROR boundary for the local opaque-COPY bridge.
///
/// pgrx generates this boundary for `pg_sys` bindings, but these functions are
/// local C symbols and therefore must establish it explicitly. Every closure
/// below contains only the C call. The boundary converts a PostgreSQL longjmp
/// into a Rust panic, which the owning COPY driver catches so Rust cleanup runs
/// normally.
pub(crate) struct CopyBridge;

impl CopyBridge {
    pub(crate) unsafe fn prepare_from(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LakebaseCopyPreparation,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe {
                    lakebase_prepare_copy_from(
                        pstate,
                        statement,
                        stmt_location,
                        stmt_len,
                        preparation,
                    );
                }
            });
        }
    }

    pub(crate) unsafe fn prepare_to(
        pstate: *mut pg_sys::ParseState,
        statement: *const pg_sys::CopyStmt,
        stmt_location: i32,
        stmt_len: i32,
        preparation: *mut LakebaseCopyPreparation,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe {
                    lakebase_prepare_copy_to(
                        pstate,
                        statement,
                        stmt_location,
                        stmt_len,
                        preparation,
                    );
                }
            });
        }
    }

    pub(crate) unsafe fn dispose_preparation(preparation: *mut LakebaseCopyPreparation) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe { lakebase_dispose_copy_preparation(preparation) };
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
                unsafe {
                    lakebase_begin_copy_from(
                        pstate,
                        relation,
                        where_clause,
                        filename,
                        is_program,
                        data_source_cb,
                        attnamelist,
                        options,
                    )
                }
            })
        }
    }

    pub(crate) unsafe fn execute_from(state: pg_sys::CopyFromState) -> u64 {
        unsafe { pg_guard_ffi_boundary(|| unsafe { lakebase_copy_from(state) }) }
    }

    pub(crate) unsafe fn next_from(
        state: pg_sys::CopyFromState,
        econtext: *mut pg_sys::ExprContext,
        values: *mut pg_sys::Datum,
        nulls: *mut bool,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_next_copy_from(state, econtext, values, nulls)
            })
        }
    }

    pub(crate) unsafe fn end_from(state: pg_sys::CopyFromState) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe { lakebase_end_copy_from(state) };
            });
        }
    }

    pub(crate) unsafe fn begin_row_encoder(
        relation: pg_sys::Relation,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyToState {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_begin_copy_row_encoder(relation, options)
            })
        }
    }

    pub(crate) unsafe fn encode_copy_header(
        state: pg_sys::CopyToState,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_encode_copy_header(state, data, len)
            });
        }
    }

    pub(crate) unsafe fn encode_copy_row(
        state: pg_sys::CopyToState,
        slot: *mut pg_sys::TupleTableSlot,
        data: *mut *const std::ffi::c_char,
        len: *mut std::ffi::c_int,
    ) {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_encode_copy_row(state, slot, data, len)
            });
        }
    }

    pub(crate) unsafe fn end_row_encoder(state: pg_sys::CopyToState) {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_end_copy_row_encoder(state)
            });
        }
    }

    pub(crate) unsafe fn begin_to(
        pstate: *mut pg_sys::ParseState,
        relation: pg_sys::Relation,
        raw_query: *mut pg_sys::RawStmt,
        query_relation: pg_sys::Oid,
        filename: *const std::ffi::c_char,
        is_program: bool,
        data_dest_cb: pg_sys::copy_data_dest_cb,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> pg_sys::CopyToState {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe {
                    lakebase_begin_copy_to(
                        pstate,
                        relation,
                        raw_query,
                        query_relation,
                        filename,
                        is_program,
                        data_dest_cb,
                        attnamelist,
                        options,
                    )
                }
            })
        }
    }

    pub(crate) unsafe fn execute_to(state: pg_sys::CopyToState) -> u64 {
        unsafe { pg_guard_ffi_boundary(|| unsafe { lakebase_copy_to(state) }) }
    }

    pub(crate) unsafe fn end_to(state: pg_sys::CopyToState) {
        unsafe {
            pg_guard_ffi_boundary(|| {
                unsafe { lakebase_end_copy_to(state) };
            });
        }
    }

    pub(crate) unsafe fn copy_attnums(
        relation: pg_sys::Relation,
        attnamelist: *mut pg_sys::List,
    ) -> *mut pg_sys::List {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_copy_get_attnums(relation, attnamelist)
            })
        }
    }

    pub(crate) unsafe fn to_tuple_desc(state: pg_sys::CopyToState) -> pg_sys::TupleDesc {
        unsafe { pg_guard_ffi_boundary(|| unsafe { lakebase_copy_to_tuple_desc(state) }) }
    }

    pub(crate) unsafe fn to_attnums(state: pg_sys::CopyToState) -> *mut pg_sys::List {
        unsafe { pg_guard_ffi_boundary(|| unsafe { lakebase_copy_to_attnums(state) }) }
    }

    pub(crate) unsafe fn begin_raw_field_reader(
        data_source_cb: pg_sys::copy_data_source_cb,
        options: *mut pg_sys::List,
    ) -> *mut std::ffi::c_void {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_begin_raw_field_reader(data_source_cb, options)
            })
        }
    }

    pub(crate) unsafe fn next_raw_fields(
        reader: *mut std::ffi::c_void,
        fields: *mut *mut *mut std::ffi::c_char,
        field_count: *mut usize,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_next_raw_fields(reader, fields, field_count)
            })
        }
    }

    pub(crate) unsafe fn end_raw_field_reader(reader: *mut std::ffi::c_void) {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_end_raw_field_reader(reader);
            });
        }
    }

    pub(crate) unsafe fn begin_text_input_validator(
        type_oid: pg_sys::Oid,
    ) -> *mut std::ffi::c_void {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_begin_text_input_validator(type_oid)
            })
        }
    }

    pub(crate) unsafe fn text_input_accepts(
        validator: *mut std::ffi::c_void,
        value: *const std::ffi::c_char,
    ) -> bool {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_text_input_accepts(validator, value)
            })
        }
    }

    pub(crate) unsafe fn end_text_input_validator(validator: *mut std::ffi::c_void) {
        unsafe {
            pg_guard_ffi_boundary(|| unsafe {
                lakebase_end_text_input_validator(validator);
            });
        }
    }
}
