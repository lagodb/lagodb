//! Shared success/error shutdown for standard, byte and typed COPY FROM drivers.

use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::execution::CopyOwner;
use super::write_bridge::CopyWriteBridge;
use super::{CopyError, pg};

pub(super) struct CopyFromExecution {
    pub(super) state: pg_sys::CopyFromState,
    owner: CopyOwner,
    state_ended: bool,
}

impl CopyFromExecution {
    pub(super) fn new(state: pg_sys::CopyFromState) -> Self {
        Self {
            state,
            owner: CopyOwner::new(),
            state_ended: false,
        }
    }

    pub(super) fn bridge(&self) -> CopyWriteBridge {
        CopyWriteBridge::new(&self.owner)
    }

    fn end_state(&mut self) -> Result<(), PgError> {
        self.state_ended = true;
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_routed_from(self.state);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub(super) fn finish(
        &mut self,
        result: Result<u64, PgError>,
        finish_source: impl FnOnce() -> Result<(), CopyError>,
    ) -> Result<u64, CopyError> {
        match result {
            Ok(processed) => {
                self.end_state()?;
                finish_source()?;
                self.owner.complete()?;
                Ok(processed)
            }
            Err(error) => {
                // A close/PROGRAM error must not replace the execution error.
                let _ = self.end_state();
                Err(error.into())
            }
        }
    }
}

impl Drop for CopyFromExecution {
    fn drop(&mut self) {
        if !self.state_ended {
            let _ = self.end_state();
        }
        // Owner release follows input/executor cleanup, including early returns.
    }
}
