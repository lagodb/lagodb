//! Synchronous PostgreSQL checkpoint requests for storage lifecycle operations.

use std::ffi::c_int;

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

// postmaster/bgwriter.h is not included in pgrx's generated bindings.
unsafe extern "C-unwind" {
    fn RequestCheckpoint(flags: c_int);
}

pub struct Checkpoint;

impl Checkpoint {
    /// Force an immediate checkpoint and wait for completion, including pending
    /// relation-file unlinks. Matches PostgreSQL's DROP TABLESPACE retry path.
    /// PostgreSQL errors remain structured errors until the caller's FFI boundary.
    pub fn immediate_and_wait() -> Result<(), PgError> {
        let flags = (pg_sys::CHECKPOINT_IMMEDIATE
            | pg_sys::CHECKPOINT_FORCE
            | pg_sys::CHECKPOINT_WAIT) as c_int;
        PgTryBuilder::new(move || {
            // SAFETY: called on the PostgreSQL backend thread. The guard
            // converts PG longjmp into Rust unwinding before it is caught.
            unsafe { pg_guard_ffi_boundary(|| RequestCheckpoint(flags)) };
            Ok(())
        })
        .catch_others(|error| Err(PgError::from_caught(error)))
        .execute()
    }
}
