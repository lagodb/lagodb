//! Typed, statically named PostgreSQL injection-point call sites.

use std::ffi::CStr;

/// A statically named PostgreSQL injection point.
///
/// PostgreSQL 17 executes the point only when the target server was built with
/// injection-point support. On PostgreSQL 16 and standard PostgreSQL 17
/// builds, [`run`](Self::run) is compiled to an inline no-op.
///
/// The C compatibility boundary also records PostgreSQL 18's two-argument
/// injection-point ABI, but the workspace intentionally has no PG18 Cargo
/// feature and rejects PG18 until the complete C framework port is audited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InjectionPoint {
    name: &'static CStr,
}

impl InjectionPoint {
    /// Creates an injection point with a PostgreSQL-compatible static name.
    ///
    /// PostgreSQL stores at most 63 name bytes. Keeping construction const
    /// makes invalid or dynamically allocated names impossible at call sites.
    ///
    /// # Panics
    ///
    /// Panics when `name` is empty or exceeds PostgreSQL's 63-byte limit.
    pub const fn new(name: &'static CStr) -> Self {
        assert!(!name.to_bytes().is_empty());
        assert!(name.to_bytes().len() < 64);
        Self { name }
    }

    /// Returns the static name used to attach callbacks to this point.
    pub const fn name(self) -> &'static CStr {
        self.name
    }

    /// Returns whether the target PostgreSQL build enables native injection points.
    pub const fn is_available() -> bool {
        cfg!(lakebase_pg_injection_points)
    }

    /// Runs the attached callback, if any.
    ///
    /// The Rust call site and static name do not allocate. An enabled
    /// PostgreSQL build may still allocate while loading or caching an attached
    /// callback, so points belong at coarse lifecycle boundaries rather than
    /// per-row or per-write hot paths.
    ///
    /// The point must be reached from PostgreSQL's main thread. pgrx converts
    /// PostgreSQL errors raised by a callback into a Rust unwind at the narrow
    /// FFI boundary so Rust frames outside that boundary are dropped normally.
    ///
    /// # Panics
    ///
    /// Panics when called outside PostgreSQL's main thread. An attached
    /// callback that raises a PostgreSQL error is also rethrown through pgrx's
    /// normal Rust error boundary.
    #[inline(always)]
    pub fn run(self) {
        #[cfg(lakebase_pg_injection_points)]
        self.run_native();
    }

    #[cfg(lakebase_pg_injection_points)]
    #[inline(never)]
    fn run_native(self) {
        // SAFETY: Injection points are called only from PostgreSQL
        // backend/bgworker main threads, as required by
        // pg_guard_ffi_boundary. The closure has no values requiring drop,
        // and the name is a static, NUL-terminated C string.
        unsafe {
            pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| {
                lakebase_injection_point_run(self.name.as_ptr());
            });
        }
    }
}

#[cfg(lakebase_pg_injection_points)]
unsafe extern "C-unwind" {
    fn lakebase_injection_point_run(name: *const std::ffi::c_char);
}
