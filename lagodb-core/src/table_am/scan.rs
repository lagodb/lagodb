//! Scan, sample, parallel-scan, and ANALYZE contracts.

use pgrx::pg_sys;

use super::{AmResult, BitmapScanOps, TidRangeOps, unsupported_callback};
#[cfg(any(feature = "pg17", feature = "pg18"))]
use crate::handles::AnalyzeReadStreamHandle;
use crate::handles::{
    ItemPointer, OwnedScanKeys, ParallelTableScanDescHandle, RelationHandle,
    SampleScanStateHandle, ScanDirection, SnapshotHandle,
};
use crate::tuple::{Row, SlotDatumWriter};

/// Type-safe view of PostgreSQL's `ScanOptions` bitmask.
///
/// Keeping the raw value private prevents access methods from scattering
/// bit arithmetic and PostgreSQL constants through their scan state machines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanFlags(u32);

impl ScanFlags {
    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn is_analyze(self) -> bool {
        self.0 & pg_sys::ScanOptions::SO_TYPE_ANALYZE != 0
    }

    #[inline]
    pub const fn is_seqscan(self) -> bool {
        self.0 & pg_sys::ScanOptions::SO_TYPE_SEQSCAN != 0
    }
}

/// Result of inspecting one candidate row during `ANALYZE`.
///
/// `live_delta` and `dead_delta` are contributions to PostgreSQL-owned
/// running totals, not replacement totals.  A visible row normally uses
/// `visible(1.0)`; storage engines using unequal scan units may provide a
/// statistically justified weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnalyzeTupleOutcome {
    pub found: bool,
    pub live_delta: f64,
    pub dead_delta: f64,
}

impl AnalyzeTupleOutcome {
    #[inline]
    pub const fn visible(live_delta: f64) -> Self {
        Self {
            found: true,
            live_delta,
            dead_delta: 0.0,
        }
    }

    #[inline]
    pub const fn end_of_block() -> Self {
        Self {
            found: false,
            live_delta: 0.0,
            dead_delta: 0.0,
        }
    }

    /// End a scan unit that contributes `live_delta` without emitting a row.
    ///
    /// PostgreSQL divides its live-row total by the blocks it visited, so a
    /// store whose rows are not tied to a block reports each unit's share here.
    #[inline]
    pub const fn end_of_block_with_live_delta(live_delta: f64) -> Self {
        Self {
            found: false,
            live_delta,
            dead_delta: 0.0,
        }
    }
}

pub trait AmScan {
    fn slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        unsafe { &pg_sys::TTSOpsVirtual }
    }

    /// Slot operations used after an ANALYZE callback has produced a sample.
    ///
    /// PostgreSQL allocates the slot using [`Self::slot_callbacks`], so an
    /// override must use the same slot layout and may only customize behavior
    /// that does not affect allocation or initialization.
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        Self::slot_callbacks()
    }

    fn parallelscan_estimate(rel: &RelationHandle) -> AmResult<pg_sys::Size>
    where
        Self: Sized,
    {
        let _ = rel;
        unsupported_callback("parallel table scan")
    }

    fn parallelscan_initialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> AmResult<pg_sys::Size>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        unsupported_callback("parallel table scan")
    }

    fn parallelscan_reinitialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        unsupported_callback("parallel table scan")
    }
}

pub trait AmScanSession {
    /// TID-range support and its complete callback implementation.
    const TID_RANGE: Option<TidRangeOps<Self>> = None;

    /// Bitmap table-scan support and its complete callback implementation.
    const BITMAP: Option<BitmapScanOps<Self>> = None;

    /// Begin the scan and construct the complete, row-ready session.
    ///
    /// Like `heap_beginscan`, this is the single construction step for a
    /// PostgreSQL-initiated scan; [`Self::scan_rescan`] restarts the
    /// traversal. A session the framework installs directly, such as a
    /// ModifyTable target scan, is likewise complete before its descriptor is
    /// created. `rel`, `snapshot` and `pscan` are borrowed only for this call;
    /// the AM captures what it needs into owned fields.
    ///
    /// `keys` is the dispatcher-copied set of effective scan keys for the
    /// *initial* scan. The reference is only valid for the duration of this
    /// call; the AM must consume the keys (e.g. translate them into its native
    /// predicate language, after resolving its schema) here rather than
    /// retaining the borrow. After construction the dispatcher moves the owned
    /// buffer into the scan descriptor, and later [`Self::scan_rescan`] calls
    /// see that buffer updated in place.
    fn scan_begin(
        rel: &RelationHandle,
        snapshot: Option<&SnapshotHandle>,
        pscan: Option<&ParallelTableScanDescHandle>,
        flags: ScanFlags,
        keys: &OwnedScanKeys,
    ) -> AmResult<Self>
    where
        Self: Sized;

    /// Fetch the next tuple in `direction` into `out`, like PostgreSQL's
    /// `scan_getnextslot`.
    ///
    /// The framework clears the slot, resets the per-fetch memory context and
    /// selects it before this call, and stores the virtual tuple only when
    /// this returns `true`. Pass-by-reference datums written through `out`
    /// must be allocated in the current memory context. The framework has
    /// already set every dropped attribute in `out` to NULL.
    ///
    /// A session that cannot fetch in `direction` must return an error rather
    /// than treat it as [`ScanDirection::Forward`].
    fn scan_getnextslot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<bool>;

    /// Restart the scan.
    ///
    /// `keys` reflects the dispatcher's PostgreSQL-aligned semantics: a
    /// non-null `key` argument from `scan_rescan` *replaces* the previously
    /// stored keys (PostgreSQL's heap AM does the same with `memcpy` in
    /// `initscan`); a null `key` keeps the prior keys unchanged. The
    /// dispatcher has already applied that rule before calling this method,
    /// so `keys` is always the *effective* key set for the upcoming scan.
    /// Like in `scan_begin`, the reference is only valid for this call.
    ///
    /// `set_params` and the `allow_*` flags are heap-AM scan strategy
    /// hints (BufferAccessStrategy, sync scan, page mode). AMs that do not
    /// implement those strategies may safely ignore them.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> AmResult<()>;

    fn scan_end(&mut self) -> AmResult<()>;

    fn scan_sample_next_block(
        &mut self,
        scanstate: &SampleScanStateHandle,
    ) -> AmResult<bool> {
        let _ = scanstate;
        unsupported_callback("scan_sample_next_block")
    }

    fn scan_sample_next_tuple(
        &mut self,
        scanstate: &SampleScanStateHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        let _ = (scanstate, row);
        unsupported_callback("scan_sample_next_tuple")
    }

    /// PostgreSQL 15 and 16 choose each block themselves and keep the sampler
    /// private.
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    fn scan_analyze_next_block(
        &mut self,
        block: pg_sys::BlockNumber,
    ) -> AmResult<bool> {
        let _ = block;
        unsupported_callback("scan_analyze_next_block")
    }

    #[cfg(any(feature = "pg17", feature = "pg18"))]
    fn scan_analyze_next_block(
        &mut self,
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        let _ = stream;
        unsupported_callback("scan_analyze_next_block")
    }

    fn scan_analyze_next_tuple(
        &mut self,
        oldest_xmin: pg_sys::TransactionId,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        let _ = (oldest_xmin, out);
        unsupported_callback("scan_analyze_next_tuple")
    }

    fn tuple_tid_valid(&mut self, tid: &ItemPointer) -> AmResult<bool> {
        let _ = tid;
        Ok(false)
    }

    fn tuple_get_latest_tid(&mut self, tid: &mut ItemPointer) -> AmResult<()> {
        let _ = tid;
        Ok(())
    }
}
