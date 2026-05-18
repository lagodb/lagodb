use super::PgWrapper;
use pgrx::{pg_sys, varlena};

impl PgWrapper {
    /// Rust wrapper for the C macro `ReleaseTupleDesc`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `tupdesc` is a valid pointer to a
    /// TupleDescData.
    #[allow(dead_code)]
    pub(crate) unsafe fn release_tuple_desc(tupdesc: pg_sys::TupleDesc) {
        unsafe {
            if (*tupdesc).tdrefcount >= 0 {
                pg_sys::DecrTupleDescRefCount(tupdesc);
            }
        }
    }

    /// Rust wrapper for the C macro `DatumGetHeapTupleHeader`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `datum` is a valid pointer to a composite
    /// type.
    #[allow(dead_code)]
    pub(crate) unsafe fn datum_get_heap_tuple_header(
        datum: pg_sys::Datum,
    ) -> pg_sys::HeapTupleHeader {
        unsafe {
            pg_sys::pg_detoast_datum(datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader
        }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetTypeId`.
    ///
    /// # Safety
    ///
    /// `header` must point to a valid heap tuple header.
    #[allow(dead_code)]
    pub(crate) unsafe fn heap_tuple_header_get_type_id(
        header: pg_sys::HeapTupleHeader,
    ) -> pg_sys::Oid {
        unsafe { (*header).t_choice.t_datum.datum_typeid }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetTypMod`.
    ///
    /// # Safety
    ///
    /// `header` must point to a valid heap tuple header.
    #[allow(dead_code)]
    pub(crate) unsafe fn heap_tuple_header_get_typmod(
        header: pg_sys::HeapTupleHeader,
    ) -> i32 {
        unsafe { (*header).t_choice.t_datum.datum_typmod }
    }

    /// Rust wrapper for the C macro `HeapTupleHeaderGetDatumLength`.
    ///
    /// # Safety
    ///
    /// `header` must point to a valid heap tuple header.
    #[allow(dead_code)]
    pub(crate) unsafe fn heap_tuple_header_get_datum_length(
        header: pg_sys::HeapTupleHeader,
    ) -> u32 {
        unsafe { varlena::varsize(header as *const pg_sys::varlena) as u32 }
    }

    /// Rust wrapper for the C macro `ScanKeyInit`.
    ///
    /// # Safety
    ///
    /// `entry` must point to a valid `ScanKeyData`.
    pub(crate) unsafe fn scan_key_init(
        entry: *mut pg_sys::ScanKeyData,
        attribute_number: pg_sys::AttrNumber,
        strategy: u16,
        procedure: pg_sys::RegProcedure,
        argument: pg_sys::Datum,
    ) {
        unsafe {
            (*entry).sk_flags = 0;
            (*entry).sk_attno = attribute_number;
            (*entry).sk_strategy = strategy;
            (*entry).sk_subtype = pg_sys::InvalidOid;
            (*entry).sk_collation = pg_sys::InvalidOid;
            (*entry).sk_argument = argument;
            pg_sys::fmgr_info(procedure, &mut (*entry).sk_func);
        }
    }
}
