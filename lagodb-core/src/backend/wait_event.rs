//! PostgreSQL wait events reported by extensions.

use std::ffi::CStr;

use pgrx::pg_sys;

/// Wait-event identifier accepted by `pgstat_report_wait_start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtensionWaitEvent(u32);

impl ExtensionWaitEvent {
    /// Allocate a wait event visible under `name`.
    ///
    /// PostgreSQL 15 and 16 have no named extension wait events, so every name
    /// reports the generic `Extension` event there.
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    pub fn register(name: &'static CStr) -> Self {
        // SAFETY: `name` is a static NUL-terminated string.
        Self(unsafe { pg_sys::WaitEventExtensionNew(name.as_ptr()) })
    }

    /// Allocate a wait event visible under `name`.
    ///
    /// PostgreSQL 15 and 16 have no named extension wait events, so every name
    /// reports the generic `Extension` event there.
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    pub fn register(_name: &'static CStr) -> Self {
        Self(pg_sys::PG_WAIT_EXTENSION)
    }

    #[inline]
    pub const fn info(self) -> u32 {
        self.0
    }
}
