//! PostgreSQL's in-memory decision for one hash join build input.

use std::ffi::c_int;
use std::ptr;

use pgrx::pg_sys;

unsafe extern "C-unwind" {
    // Declared in executor/nodeHash.h, which pgrx does not bind. It computes
    // from its arguments and the hash memory GUCs, and raises no ERROR.
    fn ExecChooseHashTableSize(
        ntuples: f64,
        tupwidth: c_int,
        useskew: bool,
        try_combined_hash_mem: bool,
        parallel_workers: c_int,
        space_allowed: *mut usize,
        numbuckets: *mut c_int,
        numbatches: *mut c_int,
        num_skew_mcvs: *mut c_int,
    );
}

/// A build input's planned size, in the terms PostgreSQL sizes a hash table.
#[derive(Debug, Clone, Copy)]
pub(super) struct HashBuildEstimate {
    rows: f64,
    width: i32,
}

impl HashBuildEstimate {
    pub(super) const fn new(rows: f64, width: i32) -> Self {
        Self { rows, width }
    }

    /// How many `hash_mem`s PostgreSQL sizes this input at: 1 when it keeps
    /// the input in memory, otherwise its batch count times each batch's
    /// memory. This is not the batch count PostgreSQL 18 returns, which it
    /// halves while doubling each batch's memory to bound the batch files'
    /// buffers; the product is what earlier versions return as the count.
    pub(super) fn hash_memory_units(self) -> f64 {
        let mut space_allowed = 0;
        let mut buckets = 0;
        let mut batches = 0;
        let mut skew_mcvs = 0;
        // SAFETY: runs on the backend thread, and every out pointer refers to a
        // live local.
        unsafe {
            ExecChooseHashTableSize(
                self.rows,
                self.width,
                false,
                false,
                0,
                ptr::from_mut(&mut space_allowed),
                ptr::from_mut(&mut buckets),
                ptr::from_mut(&mut batches),
                ptr::from_mut(&mut skew_mcvs),
            );
        }
        // SAFETY: PostgreSQL derives this limit from work_mem and
        // hash_mem_multiplier; both are backend-local.
        let hash_memory = unsafe { pg_sys::get_hash_memory_limit() };
        f64::from(batches) * space_allowed as f64 / hash_memory as f64
    }
}
