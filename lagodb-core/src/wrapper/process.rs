use core::ffi::c_int;

use pgrx::pg_sys;

use super::PgWrapper;

impl PgWrapper {
    pub(crate) fn proc_exit(code: c_int) -> ! {
        #[cfg(any(feature = "pg16", feature = "pg17"))]
        unsafe {
            // SAFETY: PostgreSQL owns the current backend process and exposes
            // proc_exit as its normal callback-aware termination path.
            pg_sys::proc_exit(code)
        }

        #[cfg(feature = "pg18")]
        {
            // SAFETY: PostgreSQL owns the current backend process and exposes
            // proc_exit as its normal callback-aware termination path. PG18
            // still declares it pg_noreturn, although bindgen loses that fact.
            unsafe { pg_sys::proc_exit(code) };
            unreachable!("PostgreSQL proc_exit returned despite pg_noreturn")
        }
    }
}
