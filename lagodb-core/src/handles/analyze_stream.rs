//! PostgreSQL 17+ ANALYZE block-sampling `ReadStream`.
//!
//! `acquire_sample_rows()` hands a table AM a `ReadStream` whose callback walks
//! PostgreSQL's private `BlockSamplerData`. Core owns the private-layout
//! bridge, so it also owns the sampler protocol: providers receive validated
//! ticket counts and never interpret `BlockSamplerData` themselves.

use pgrx::{PgSqlErrorCode, pg_sys};

use super::borrowed::PgBorrowed;
use crate::diag::PgReportError;

#[repr(C)]
#[derive(Debug, Default)]
struct RawAnalyzeSamplerState {
    population_blocks: pg_sys::BlockNumber,
    target_rows: core::ffi::c_int,
    visited_blocks: pg_sys::BlockNumber,
    selected_blocks: core::ffi::c_int,
}

unsafe extern "C" {
    fn lagodb_read_stream_analyze_sampler_state(
        stream: *mut pg_sys::ReadStream,
        state: *mut RawAnalyzeSamplerState,
    ) -> bool;
}

/// PostgreSQL's exact block-sampler state at one point of the scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AnalyzeSamplerState {
    population_blocks: u64,
    target_rows: u64,
    visited_blocks: u64,
    selected_blocks: u64,
}

/// Sampling tickets PostgreSQL's ANALYZE selected for one relation.
///
/// `target_rows` is the `targrows` passed to `acquire_sample_rows()`. During
/// inherited ANALYZE PostgreSQL has already replaced it with this relation's
/// proportional `childtargrows` before constructing the `ReadStream`.
/// PostgreSQL's `BlockSampler` selects exactly
/// `min(population_blocks, target_rows)` tickets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyzeTickets {
    population_blocks: u64,
    target_rows: u64,
    tickets: u64,
}

impl AnalyzeTickets {
    #[inline]
    pub fn population_blocks(self) -> u64 {
        self.population_blocks
    }

    #[inline]
    pub fn target_rows(self) -> u64 {
        self.target_rows
    }

    #[inline]
    pub fn tickets(self) -> u64 {
        self.tickets
    }
}

/// ANALYZE-only wrapper for PostgreSQL's block-sampling ReadStream.
///
/// Core constructs this type only for `scan_analyze_next_block`, after
/// PostgreSQL's `acquire_sample_rows()` has installed a live
/// `BlockSamplerData` callback-private pointer. Keeping that precondition in a
/// dedicated type prevents the private-layout bridge from being called on an
/// arbitrary ReadStream.
#[derive(Debug)]
pub struct AnalyzeReadStreamHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ReadStream>,
}

impl<'a> AnalyzeReadStreamHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`. It must be the PostgreSQL
    /// ReadStream created by `acquire_sample_rows()` for the active
    /// `scan_analyze_next_block` callback, with callback-private data pointing
    /// to that stack frame's live `BlockSamplerData`.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut pg_sys::ReadStream) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    /// Consume every sampling ticket of an unread stream.
    ///
    /// The sampler is copied before and after the drain, so inconsistent
    /// sampler fields cannot silently change the sample size. This validates
    /// field values only: the versioned C bridge reads the build-time PG17 or
    /// PG18 private layout, which must be audited when PostgreSQL is upgraded.
    pub fn drain_tickets(&self) -> Result<AnalyzeTickets, PgReportError> {
        let initial = self.sampler_state()?;
        if initial.visited_blocks != 0 || initial.selected_blocks != 0 {
            return Err(Self::invalid_sampler(
                "ANALYZE ReadStream was consumed before the table access method drained it",
            ));
        }
        let mut tickets = 0_u64;
        while self.next_block().is_some() {
            tickets += 1;
        }
        let completed = self.sampler_state()?;
        if completed.population_blocks != initial.population_blocks
            || completed.target_rows != initial.target_rows
            || completed.selected_blocks != tickets
            || tickets != initial.population_blocks.min(initial.target_rows)
        {
            return Err(Self::invalid_sampler(
                "ANALYZE BlockSampler state is inconsistent with the consumed tickets",
            ));
        }
        Ok(AnalyzeTickets {
            population_blocks: initial.population_blocks,
            target_rows: initial.target_rows,
            tickets,
        })
    }

    fn sampler_state(&self) -> Result<AnalyzeSamplerState, PgReportError> {
        let mut raw = RawAnalyzeSamplerState::default();
        // SAFETY: the handle owns a non-null PostgreSQL ReadStream borrow for
        // this callback, and `raw` is writable for the duration of the call.
        // The version-gated C bridge owns the private-layout dereference and
        // selects the supported major/minor layout epoch. It returns false
        // unless its copied BlockSamplerData fields are valid.
        let valid = unsafe {
            lagodb_read_stream_analyze_sampler_state(self.inner.as_ptr(), &mut raw)
        };
        if !valid {
            return Err(Self::invalid_sampler(
                "ANALYZE ReadStream has no valid PostgreSQL BlockSampler state",
            ));
        }
        Ok(AnalyzeSamplerState {
            population_blocks: u64::from(raw.population_blocks),
            target_rows: u64::try_from(raw.target_rows)
                .expect("the sampler bridge rejects non-positive target rows"),
            visited_blocks: u64::from(raw.visited_blocks),
            selected_blocks: u64::try_from(raw.selected_blocks)
                .expect("the sampler bridge rejects negative selected blocks"),
        })
    }

    /// Consume the next block chosen by PostgreSQL's sampling stream.
    ///
    /// The block identity is opaque to non-block storage engines; `Some`
    /// means one sampling ticket was selected. On PG18 this calls
    /// `read_stream_next_block()` directly, so the stream's batching flag does
    /// not enter relation-I/O look-ahead or change ticket consumption.
    fn next_block(&self) -> Option<pg_sys::BlockNumber> {
        let mut strategy: pg_sys::BufferAccessStrategy = std::ptr::null_mut();
        // SAFETY: `inner` is a live PostgreSQL-owned ReadStream for this
        // callback and `strategy` is writable out-parameter storage. The AM
        // deliberately discards the reported buffer strategy because it does
        // not read PostgreSQL relation buffers.
        let block = unsafe {
            pg_sys::read_stream_next_block(self.inner.as_ptr(), &mut strategy)
        };
        (block != pg_sys::InvalidBlockNumber).then_some(block)
    }

    fn invalid_sampler(message: &'static str) -> PgReportError {
        PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
    }
}
