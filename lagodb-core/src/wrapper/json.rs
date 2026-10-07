use super::PgWrapper;
use pgrx::{fcinfo, pg_sys};
use std::ffi::{c_char, c_int};

unsafe extern "C-unwind" {
    // common/shortest_dec.h: these write into caller-owned buffers and neither
    // allocate nor raise PG errors. They are not included in pgrx's bindings.
    fn float_to_shortest_decimal_buf(value: f32, result: *mut c_char) -> c_int;
    fn double_to_shortest_decimal_buf(value: f64, result: *mut c_char) -> c_int;
    // DirectFunctionCall needs the native C entry point, not a Rust wrapper.
    #[link_name = "numeric_in"]
    fn pg_numeric_in(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
}

impl PgWrapper {
    /// Convert a finite float to Numeric without session-dependent rounding.
    ///
    /// `float4_numeric`/`float8_numeric` round to FLT_DIG/DBL_DIG. Use PG's
    /// shortest-decimal output followed by numeric_in to retain float precision.
    /// Numeric cannot preserve a zero's sign; JsonbBuilder handles negative zero
    /// with its special-float string representation before calling this method.
    /// Keep the output text and numeric_in call frame on the stack, without
    /// temporary heap allocation for either.
    /// Errors propagate to the enclosing JSONB construction boundary.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be active on this thread with the destination row memory
    /// context current. `value` must be finite and, when `single_precision` is
    /// true, originate from a float4. The returned Numeric belongs to that
    /// context and must remain live until JSONB serialization completes.
    pub(crate) unsafe fn jsonb_numeric_from_float(
        value: f64,
        single_precision: bool,
    ) -> pg_sys::Datum {
        // Nested JSONB values can be written back, including unchanged columns
        // of an UPDATE. This is a storage conversion, not float display output:
        // extra_float_digits <= 0 would round the value before it reaches JSONB.
        // Call PG's shortest formatters directly rather than changing the GUC;
        // a GUC scope could also affect user expressions evaluated within it.
        let mut text = [0 as c_char; 32];
        // SAFETY: the shortest forms need at most 16/25 bytes including NUL.
        unsafe {
            if single_precision {
                float_to_shortest_decimal_buf(value as f32, text.as_mut_ptr());
            } else {
                double_to_shortest_decimal_buf(value, text.as_mut_ptr());
            }
        }
        // SAFETY: finite float output is valid decimal input. numeric_in needs
        // no FmgrInfo and allocates a non-NULL Numeric in the current context.
        // The stack buffer stays live through parsing; Numeric does not borrow it.
        unsafe {
            pg_sys::DirectFunctionCall3Coll(
                Some(pg_numeric_in),
                pg_sys::InvalidOid,
                pg_sys::Datum::from(text.as_ptr()),
                pg_sys::Datum::from(pg_sys::InvalidOid),
                pg_sys::Datum::from(-1_i32),
            )
        }
    }

    /// Call PostgreSQL's validating `json_in` input function for a
    /// NUL-terminated string.
    ///
    /// This parses the input. PG errors unwind to the enclosing operation or
    /// provider/FFI boundary; array/composite leaves do not catch individually.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn json_input_from_cstr(
        ptr: *const c_char,
    ) -> Option<pg_sys::Datum> {
        // SAFETY: the caller keeps the NUL-terminated input live. pgrx's PG
        // binding translates PG ERROR into unwinding through Rust frames.
        unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::json_in,
                &[Some(pg_sys::Datum::from(ptr))],
            )
        }
    }

    /// Call PostgreSQL's validating `jsonb_in` input function for a
    /// NUL-terminated string.
    /// PG errors unwind to the enclosing operation or provider/FFI boundary.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn jsonb_input_from_cstr(
        ptr: *const c_char,
    ) -> Option<pg_sys::Datum> {
        // SAFETY: the caller keeps the NUL-terminated input live. pgrx's PG
        // binding translates PG ERROR into unwinding through Rust frames.
        unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::jsonb_in,
                &[Some(pg_sys::Datum::from(ptr))],
            )
        }
    }
}
