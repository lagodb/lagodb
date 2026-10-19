//! PostgreSQL TableAM scan lifecycle for Iceberg tables.
//!
//! Regular scans keep two lifecycle layers:
//!
//! - [`PreparedRowScan`] owns the statement snapshot, projection, predicates,
//!   decoder, and scan task cache. Metadata is captured in `new` while the
//!   PG snapshot is borrowed; `scan_begin` prepares the cursor. Both are
//!   preserved across `scan_rescan`, so the visible snapshot is frozen for the
//!   scan's duration. This matches the Read Committed contract: every
//!   `scan_rescan` comes from the same statement that issued `scan_begin`.
//! - [`PgRowCursor`] owns one traversal over the planned tasks;
//!   `scan_rescan` rebuilds only this traversal from the prepared read.
//!
//! ANALYZE has its own state after the shared statement metadata and decoder
//! have been captured. Managed mutation cursors and task ownership live in the
//! sibling `mutation` module and are shared with the CustomScan adapter.

mod mutation;

use std::mem;

use lagodb_core::access::scan::virtual_slot_callbacks_with_tid;
use lagodb_core::api::unsupported_callback;
use lagodb_core::handles::RelationHandle;
use lagodb_core::prelude::*;
use pgrx::pg_sys;

use crate::error::IcebergError;
use crate::managed_table::access::analyze::{AnalyzePreparation, AnalyzeScanState};
use crate::managed_table::{IcebergTableAm, ManagedTableReadView};
use crate::scan::{
    IcebergReadSnapshot, PgRowCursor, PreparedRowScan, ScanPredicates,
};
use crate::schema::relation::RelationLayout;

use super::mutation::{ManagedMutationScan, PreparedManagedMutationScan};

/// PostgreSQL-facing scan session for the Iceberg table AM.
pub struct IcebergScan {
    relation: ScanRelation,
    state: IcebergScanState,
}

/// Descriptor-derived relation facts retained after the `RelationHandle`
/// borrow ends.
struct ScanRelation {
    oid: pg_sys::Oid,
    layout: RelationLayout,
}

impl ScanRelation {
    fn from_relation(relation: &RelationHandle) -> Result<Self, IcebergError> {
        Ok(Self {
            oid: relation.oid(),
            layout: RelationLayout::from_relation(relation)?,
        })
    }
}

enum ScanKind {
    Regular(IcebergReadSnapshot),
    Analyze,
}

impl ScanKind {
    fn begin(
        self,
        relation: &ScanRelation,
        keys: &OwnedScanKeys,
    ) -> AmResult<IcebergScanState> {
        match self {
            Self::Regular(snapshot) => Ok(IcebergScanState::Regular(
                RegularScanState::begin(relation, keys, snapshot)?,
            )),
            Self::Analyze => {
                let view = ManagedTableReadView::load(relation.oid)?;
                let storage_bytes = view.storage_bytes()?;
                let snapshot = view.into_read_snapshot()?;
                let prepared = PreparedRowScan::full(
                    snapshot,
                    ScanPredicates::unfiltered(),
                    &relation.layout,
                )?;
                let preparation = AnalyzePreparation::try_new(
                    prepared.analyze_input(storage_bytes)?,
                )?;
                Ok(IcebergScanState::Analyze(Box::new(
                    AnalyzeScanState::pending(preparation),
                )))
            }
        }
    }
}

struct RegularScanState {
    prepared: PreparedRowScan,
    cursor: PgRowCursor,
}

impl RegularScanState {
    fn begin(
        relation: &ScanRelation,
        _keys: &OwnedScanKeys,
        snapshot: IcebergReadSnapshot,
    ) -> AmResult<Self> {
        let mut prepared = PreparedRowScan::full(
            snapshot,
            ScanPredicates::unfiltered(),
            &relation.layout,
        )?;
        let cursor = prepared.open_row_cursor()?;
        Ok(Self { prepared, cursor })
    }

    fn rescan(&mut self, _keys: &OwnedScanKeys) -> AmResult<()> {
        // Iceberg advertises no scan-key path. PostgreSQL remains responsible
        // for SeqScan qualification, matching the previous empty translation.
        self.prepared
            .replace_predicates(ScanPredicates::unfiltered());
        self.cursor = self.prepared.open_row_cursor()?;
        Ok(())
    }
}

// Row-producing states stay inline intentionally: boxing them would add an
// allocation per scan and an indirection on every scan_getnextslot call merely
// to shrink this once-per-scan state object.
#[allow(clippy::large_enum_variant)]
enum IcebergScanState {
    Pending(ScanKind),
    Regular(RegularScanState),
    MutationPending(PreparedManagedMutationScan),
    Mutation(ManagedMutationScan),
    Analyze(Box<AnalyzeScanState>),
    Ended,
}

impl AmScan for IcebergTableAm {
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        virtual_slot_callbacks_with_tid()
    }
}

impl AmScanSession for IcebergScan {
    type BatchDriver = Self;

    fn new(
        rel: &RelationHandle,
        snapshot: Option<&SnapshotHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        flags: ScanFlags,
    ) -> AmResult<Self> {
        let relation = ScanRelation::from_relation(rel)?;
        // Bind while the callback borrows PG's registered snapshot. Retain
        // only owned Iceberg state; no SnapshotHandle escapes this callback.
        let kind = if flags.is_analyze() {
            ScanKind::Analyze
        } else {
            let snapshot = snapshot.expect("regular AM scans have a PG snapshot");
            ScanKind::Regular(
                ManagedTableReadView::for_snapshot(relation.oid, snapshot)?
                    .into_read_snapshot()?,
            )
        };
        Ok(IcebergScan {
            relation,
            state: IcebergScanState::Pending(kind),
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let kind = match mem::replace(&mut self.state, IcebergScanState::Ended) {
            IcebergScanState::Pending(kind) => kind,
            state => {
                self.state = state;
                return Err(IcebergError::InvariantViolated(
                    "scan_begin called more than once for one Iceberg scan",
                )
                .into());
            }
        };
        self.state = kind.begin(&self.relation, keys)?;
        Ok(())
    }

    #[inline]
    fn scan_driver(&mut self) -> &mut Self::BatchDriver {
        self
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
            IcebergScanState::Regular(state) => state.rescan(keys),
            IcebergScanState::Mutation(state) => {
                state.rescan(None)?;
                Ok(())
            }
            IcebergScanState::MutationPending(_) => Ok(()),
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
        out: &mut SlotDatumWriter<'_>,
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

impl ScanBatchDriver for IcebergScan {
    /// One mode dispatch per row, matching the previous scan_driver dispatch.
    #[inline]
    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<bool> {
        match &mut self.state {
            IcebergScanState::Regular(state) => {
                if direction != ScanDirection::Forward {
                    return unsupported_callback("non-forward Iceberg scan");
                }
                state.cursor.next_into_slot(out)
            }
            IcebergScanState::Mutation(state) => {
                // ModifyTable target scans run forward. RETURNING cursor
                // movement reads the portal's tuplestore instead.
                state.next_into_slot(out)
            }
            _ => panic!("scan_driver called before scan initialization completed"),
        }
    }
}
