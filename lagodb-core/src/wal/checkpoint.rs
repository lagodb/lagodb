//! Synchronous PostgreSQL checkpoint requests for storage lifecycle operations.

use pgrx::pg_sys;

use crate::diag::PgError;
use crate::wrapper::PgWrapper;

pub struct Checkpoint;

impl Checkpoint {
    /// Force an immediate checkpoint and wait for completion, including pending
    /// relation-file unlinks. Matches PostgreSQL's DROP TABLESPACE retry path.
    /// PostgreSQL errors remain structured errors until the caller's FFI boundary.
    pub fn immediate_and_wait() -> Result<(), PgError> {
        PgWrapper::request_checkpoint(
            (pg_sys::CHECKPOINT_IMMEDIATE
                | pg_sys::CHECKPOINT_FORCE
                | pg_sys::CHECKPOINT_WAIT) as i32,
        )
    }
}
