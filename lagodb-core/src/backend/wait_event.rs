//! PostgreSQL wait events reported by extensions.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::rc::Rc;

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

    /// Report this wait event until the returned guard is dropped.
    #[inline]
    pub fn enter(self) -> ExtensionWaitGuard {
        // SAFETY: `ExtensionWaitGuard` is `!Send` and used on the owning
        // PostgreSQL backend thread; `my_wait_event_info` is always valid.
        unsafe {
            pg_sys::pgstat_report_wait_start(self.0);
        }
        ExtensionWaitGuard(PhantomData)
    }
}

/// Backend-thread-scoped guard that clears `my_wait_event_info` on drop.
#[must_use = "if unused, the PostgreSQL wait event ends immediately"]
pub struct ExtensionWaitGuard(PhantomData<Rc<()>>);

impl Drop for ExtensionWaitGuard {
    fn drop(&mut self) {
        // SAFETY: paired with `pgstat_report_wait_start` on the same backend
        // thread.
        unsafe {
            pg_sys::pgstat_report_wait_end();
        }
    }
}
