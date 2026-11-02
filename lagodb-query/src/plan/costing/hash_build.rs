//! PostgreSQL's in-memory decision for one hash join build input.

use std::ffi::c_int;
use std::ptr;

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

    /// Whether PostgreSQL would keep this input in one `hash_mem` rather than
    /// split it into batches.
    pub(super) fn fits_hash_memory(self) -> bool {
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
        batches == 1
    }
}
