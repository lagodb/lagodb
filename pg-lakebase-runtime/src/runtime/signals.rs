use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pgrx::pg_sys;

use super::bgworker::check_for_interrupts;

static SIGHUP_RECEIVED: AtomicBool = AtomicBool::new(false);

pub(super) struct BackgroundWorkerSignals;

impl BackgroundWorkerSignals {
    pub(super) fn install_launcher() {
        Self::install(true);
    }

    pub(super) fn install_dynamic_worker() {
        Self::install(true);
    }

    fn install(handle_sighup: bool) {
        // SAFETY: PostgreSQL starts background-worker entrypoints with signals
        // blocked. Install every final handler before unblocking once.
        unsafe {
            if handle_sighup {
                pg_sys::pqsignal(
                    pg_sys::SIGHUP as i32,
                    Some(background_worker_sighup),
                );
            }
            pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(background_worker_sigterm));
            pg_sys::BackgroundWorkerUnblockSignals();
        }
        // A pending SIGTERM may be delivered synchronously by the unblock.
        // Consume PostgreSQL's die flags before the entrypoint starts any work.
        check_for_interrupts();
    }

    pub(super) fn process_config_reload() -> bool {
        if !SIGHUP_RECEIVED.swap(false, Ordering::AcqRel) {
            return false;
        }
        // Match PostgreSQL's normal SIGHUP loop: consume the pending flag
        // before re-reading configuration so it describes only unprocessed
        // reload requests.
        unsafe {
            (&raw mut pg_sys::ConfigReloadPending)
                .write_volatile(0 as pg_sys::sig_atomic_t);
            pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
        }
        true
    }

    pub(super) extern "C-unwind" fn process_config_reload_callback() -> bool {
        Self::process_config_reload()
    }

    pub(super) fn wait_latch(timeout: Duration) -> bool {
        let timeout =
            libc::c_long::try_from(timeout.as_millis()).unwrap_or(libc::c_long::MAX);
        let events = (pg_sys::WL_LATCH_SET
            | pg_sys::WL_TIMEOUT
            | pg_sys::WL_POSTMASTER_DEATH) as i32;
        // SAFETY: this is called by the background-worker main thread. MyLatch
        // belongs to this process and no runtime LWLock guard crosses the
        // interrupt check.
        let events = unsafe {
            let events = pg_sys::WaitLatch(
                pg_sys::MyLatch,
                events,
                timeout,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);
            events
        };
        if events & pg_sys::WL_POSTMASTER_DEATH as i32 != 0 {
            return false;
        }
        check_for_interrupts();
        true
    }
}

unsafe extern "C-unwind" fn background_worker_sighup(_signal: i32) {
    SIGHUP_RECEIVED.store(true, Ordering::Release);
    // SAFETY: ConfigReloadPending is PostgreSQL's signal-safe reload flag and
    // SetLatch is designed for signal-handler use. pqsignal's wrapper preserves
    // errno around this callback.
    unsafe {
        (&raw mut pg_sys::ConfigReloadPending).write_volatile(1);
        pg_sys::SetLatch(pg_sys::MyLatch);
    }
}

unsafe extern "C-unwind" fn background_worker_sigterm(signal: i32) {
    // SAFETY: die is PostgreSQL's standard SIGTERM handler and this function
    // has the signal-handler ABI installed through pqsignal.
    unsafe { pg_sys::die(signal) };
}
