//! PostgreSQL TableAM scan lifecycle for Iceberg tables.
//!
//! Query scans keep two lifecycle layers:
//!
//! - [`ScanSpec`] owns the statement snapshot, bound columns, predicates, and
//!   planned-task caches. It is built once in [`AmScanSession::scan_begin`] and
//!   preserved across `scan_rescan`, so the visible snapshot is frozen for the
//!   scan's duration. This matches the Read Committed contract: every
//!   `scan_rescan` comes from the same statement that issued `scan_begin`.
//! - [`IcebergBatchCursor`] owns one traversal over the planned tasks;
//!   `scan_rescan` rebuilds only this traversal from the existing spec.
//!
//! ANALYZE has its own state after the shared statement metadata and decoder
//! have been captured. Batch adaptation and row-location binding live in
//! [`batch`], below both query and ANALYZE cursors.

use std::mem;

mod cursor;
mod spec;

pub(crate) use crate::engine::scan::ScanSpec;
pub use cursor::IcebergBatchCursor;
pub(crate) use spec::LoadedScanMetadata;

use lagodb_core::access::scan::virtual_slot_callbacks_with_tid;
use lagodb_core::handles::RelationHandle;
use lagodb_core::prelude::*;
use pgrx::pg_sys;

use crate::engine::schema::relation::RelationShape;
use crate::error::IcebergError;
use crate::managed_table::IcebergTableAm;
use crate::managed_table::access::analyze::AnalyzeScanState;

/// PostgreSQL-facing scan session for the Iceberg table AM.
pub struct IcebergScan {
    relation: ScanRelation,
    state: IcebergScanState,
}

/// Descriptor-derived relation facts retained after the `RelationHandle`
/// borrow ends.
struct ScanRelation {
    oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
    shape: RelationShape,
}

impl ScanRelation {
    fn from_relation(relation: &RelationHandle) -> Result<Self, IcebergError> {
        Ok(Self {
            oid: relation.oid(),
            tablespace_oid: relation.tablespace_oid(),
            shape: RelationShape::from_relation(relation)?,
        })
    }
}

enum ScanPurpose {
    Query,
    Analyze,
}

impl ScanPurpose {
    fn begin(
        self,
        relation: &ScanRelation,
        keys: &OwnedScanKeys,
    ) -> AmResult<IcebergScanState> {
        match self {
            Self::Query => Ok(IcebergScanState::Query(QueryScanState::begin(
                relation, keys,
            )?)),
            Self::Analyze => {
                let spec = ScanSpec::build_for_analyze(
                    relation.oid,
                    relation.tablespace_oid,
                    &relation.shape,
                )?;
                let preparation = spec.prepare_analyze()?;
                Ok(IcebergScanState::Analyze(Box::new(
                    AnalyzeScanState::pending(preparation),
                )))
            }
        }
    }
}

struct QueryScanState {
    spec: ScanSpec,
    cursor: IcebergBatchCursor,
}

impl QueryScanState {
    fn begin(relation: &ScanRelation, keys: &OwnedScanKeys) -> AmResult<Self> {
        let mut spec = ScanSpec::build(
            relation.oid,
            relation.tablespace_oid,
            keys,
            &relation.shape,
        )?;
        let cursor = spec.open_batch_cursor()?;
        Ok(Self { spec, cursor })
    }

    fn rescan(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        self.spec.refresh_filter(keys)?;
        self.cursor = self.spec.open_batch_cursor()?;
        Ok(())
    }
}

// Query state stays inline intentionally: boxing it would add an allocation
// per ordinary scan and an indirection on every scan_getnextslot call merely
// to shrink this once-per-scan state object.
#[allow(clippy::large_enum_variant)]
enum IcebergScanState {
    Pending(ScanPurpose),
    Query(QueryScanState),
    Analyze(Box<AnalyzeScanState>),
    Ended,
}

impl AmScan for IcebergTableAm {
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        virtual_slot_callbacks_with_tid()
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
        // No metadata IO yet: defer schema-dependent work to `scan_begin`.
        Ok(IcebergScan {
            relation: ScanRelation::from_relation(rel)?,
            state: IcebergScanState::Pending(if flags.is_analyze() {
                ScanPurpose::Analyze
            } else {
                ScanPurpose::Query
            }),
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let purpose = match mem::replace(&mut self.state, IcebergScanState::Ended) {
            IcebergScanState::Pending(purpose) => purpose,
            state => {
                self.state = state;
                return Err(IcebergError::InvariantViolated(
                    "scan_begin called more than once for one Iceberg scan",
                )
                .into());
            }
        };
        self.state = purpose.begin(&self.relation, keys)?;
        Ok(())
    }

    /// Slot-first scan driver: the Arrow batch cursor that decodes the current
    /// batch straight into the slot. The framework drives every scan through
    /// this one path; there is no row variant for a columnar AM.
    fn scan_driver(&mut self) -> &mut Self::BatchDriver {
        // `scan_begin` builds the cursor before the executor fetches any row,
        // so it is always present by the time the framework calls this.
        match &mut self.state {
            IcebergScanState::Query(state) => &mut state.cursor,
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
            IcebergScanState::Query(state) => state.rescan(keys),
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
