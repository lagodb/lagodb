//! Iceberg Table Scan Implementation.
//!
//! A scan's state is split in two:
//!
//! - [`ScanSpec`] is the immutable scan description (table, scan columns, and
//!   optional predicate). Built once in [`AmScanSession::scan_begin`] and
//!   preserved across `scan_rescan`, so the visible snapshot is frozen for the
//!   scan's duration. This matches the Read Committed contract: every
//!   `scan_rescan` comes from the same statement that issued `scan_begin`.
//! - [`IcebergBatchCursor`] is the per-cursor mutable state; `scan_rescan`
//!   rebuilds only the cursor from the spec.
//!
//! `scan_rescan` re-translates the dispatcher-supplied keys (PostgreSQL's
//! "non-null replaces, null keeps" rule is applied by the dispatcher first).
//! The borrow is consumed within each callback, never retained.
//!
//! Scan-key predicate translation currently returns `None`, so predicates
//! remain handled by the executor (`ExecQual`).

mod cursor;
mod spec;

pub use cursor::IcebergBatchCursor;
pub(crate) use cursor::{
    BatchMetadataColumns, IcebergArrowBatchSource, IcebergArrowBatches,
};
pub(crate) use spec::{PlannedScanTasks, ScanSpec};

use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::column_mapping::RelationShape;
use crate::error::IcebergError;

/// PostgreSQL-facing scan session for the Iceberg table AM. Thin bookkeeping
/// (`rel_oid` / `spc_oid` / `shape`) over the lazily-built [`ScanSpec`] and
/// current [`IcebergBatchCursor`].
pub struct IcebergScan {
    rel_oid: pg_sys::Oid,
    spc_oid: pg_sys::Oid,
    /// Relation shape captured in [`AmScanSession::new`] (the one place the
    /// `RelationHandle` is in scope), threaded into `ScanSpec::build`.
    shape: RelationShape,
    state: IcebergScanState,
}
enum ScanPurpose {
    Query,
    Analyze {
        #[cfg(not(feature = "pg17"))]
        statistics_target: i32,
    },
}

// Query state stays inline intentionally: boxing it would add an allocation
// per ordinary scan and an indirection on every scan_getnextslot call merely
// to shrink this once-per-scan state object.
#[allow(clippy::large_enum_variant)]
enum IcebergScanState {
    Pending(ScanPurpose),
    Query {
        spec: ScanSpec,
        cursor: IcebergBatchCursor,
    },
    Analyze(Box<crate::access::analyze::AnalyzeScanState>),
    Ended,
}

impl AmScan for IcebergTableAm {
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        pg_lakebase_core::access::scan::virtual_slot_callbacks_with_tid()
    }
}

impl AmScanSession for IcebergScan {
    type BatchDriver = IcebergBatchCursor;

    fn new(
        rel: &RelationHandle,
        _snapshot: Option<&SnapshotHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        flags: ScanFlags,
    ) -> AmResult<Self> {
        // No metadata IO yet: defer schema-dependent work to `scan_begin`. The
        // relation shape is captured here, where the `RelationHandle` is in scope.
        Ok(IcebergScan {
            rel_oid: rel.oid(),
            spc_oid: rel.tablespace_oid(),
            shape: RelationShape::from_relation(rel),
            state: IcebergScanState::Pending(if flags.is_analyze() {
                ScanPurpose::Analyze {
                    #[cfg(not(feature = "pg17"))]
                    statistics_target: rel.max_statistics_target()?,
                }
            } else {
                ScanPurpose::Query
            }),
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let purpose =
            match std::mem::replace(&mut self.state, IcebergScanState::Ended) {
                IcebergScanState::Pending(purpose) => purpose,
                state => {
                    self.state = state;
                    return Err(IcebergError::InvariantViolated(
                        "scan_begin called more than once for one Iceberg scan",
                    )
                    .into());
                }
            };
        self.state = match purpose {
            ScanPurpose::Query => {
                let mut spec =
                    ScanSpec::build(self.rel_oid, self.spc_oid, keys, &self.shape)?;
                let cursor = spec.open_batch_cursor()?;
                IcebergScanState::Query { spec, cursor }
            }
            ScanPurpose::Analyze {
                #[cfg(not(feature = "pg17"))]
                statistics_target,
            } => {
                let spec = ScanSpec::build_for_analyze(
                    self.rel_oid,
                    self.spc_oid,
                    &self.shape,
                )?;
                let preparation = spec.prepare_analyze(
                    #[cfg(not(feature = "pg17"))]
                    statistics_target,
                )?;
                IcebergScanState::Analyze(Box::new(
                    crate::access::analyze::AnalyzeScanState::pending(preparation),
                ))
            }
        };
        Ok(())
    }

    /// Slot-first scan driver: the Arrow batch cursor that decodes the current
    /// batch straight into the slot. The framework drives every scan through
    /// this one path; there is no row variant for a columnar AM.
    fn scan_driver(&mut self) -> &mut Self::BatchDriver {
        // `scan_begin` builds the cursor before the executor fetches any row,
        // so it is always present by the time the framework calls this.
        match &mut self.state {
            IcebergScanState::Query { cursor, .. } => cursor,
            _ => panic!("scan_driver called outside a query scan"),
        }
    }

    /// Restart the scan, re-translating the current effective scan keys.
    ///
    /// The dispatcher has already applied the "non-null replaces, null keeps"
    /// rule, so `keys` is the effective set. `set_params` and the `allow_*`
    /// flags only affect heap-AM strategy choices and are ignored. Metadata is
    /// not re-read: a single statement drives every `scan_rescan` and must see
    /// a consistent snapshot.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> AmResult<()> {
        match &mut self.state {
            IcebergScanState::Query { spec, cursor } => {
                spec.refresh_filter(keys)?;
                *cursor = spec.open_batch_cursor()?;
                Ok(())
            }
            IcebergScanState::Pending(_) => Ok(()),
            IcebergScanState::Analyze(_) => Err(IcebergError::InvariantViolated(
                "PostgreSQL attempted to rescan an ANALYZE session",
            )
            .into()),
            IcebergScanState::Ended => Ok(()),
        }
    }

    fn scan_end(&mut self) -> AmResult<()> {
        self.state = IcebergScanState::Ended;
        Ok(())
    }

    fn scan_analyze_next_block(
        &mut self,
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        match &mut self.state {
            IcebergScanState::Analyze(state) => state.next_block(stream),
            _ => Err(IcebergError::InvariantViolated(
                "ANALYZE block callback used a non-ANALYZE scan",
            )
            .into()),
        }
    }

    fn scan_analyze_next_tuple(
        &mut self,
        _oldest_xmin: pg_sys::TransactionId,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.state {
            IcebergScanState::Analyze(state) => state.next_tuple(out),
            _ => Err(IcebergError::InvariantViolated(
                "ANALYZE tuple callback used a non-ANALYZE scan",
            )
            .into()),
        }
    }
}
