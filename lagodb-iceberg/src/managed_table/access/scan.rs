//! PostgreSQL TableAM scan lifecycle for Iceberg tables.
//!
//! Regular scans keep two lifecycle layers:
//!
//! - [`PreparedRowScan`] owns the statement snapshot, projection, predicates,
//!   decoder, and scan task cache. `scan_begin` captures metadata while the
//!   PG snapshot is borrowed and opens the first cursor. The prepared read is
//!   preserved across `scan_rescan`, so the visible snapshot is frozen for the
//!   scan's duration. This matches the Read Committed contract: every
//!   `scan_rescan` comes from the same statement that issued `scan_begin`.
//! - [`PgRowCursor`] owns one traversal over the planned tasks;
//!   `scan_rescan` rebuilds only this traversal from the prepared read.
//!
//! ANALYZE has its own state after the shared statement metadata and decoder
//! have been captured. Managed mutation cursors and task ownership live in the
//! sibling `mutation` module and are shared with the CustomScan adapter; the
//! native SeqScan mutation target is constructed by [`PreparedModifySeqScan`].

mod mutation;

use lagodb_core::prelude::*;
use pgrx::{PgSqlErrorCode, pg_sys};

use crate::error::IcebergError;
use crate::managed_table::access::analyze::{AnalyzePreparation, AnalyzeScanState};
use crate::managed_table::{IcebergTableAm, ManagedTableReadView};
use crate::scan::{PgRowCursor, PreparedRowScan, ScanPredicates};
use crate::schema::relation::RelationLayout;

use super::mutation::ManagedMutationScan;

pub(crate) use mutation::PreparedModifySeqScan;

/// PostgreSQL-facing scan session for the Iceberg table AM.
pub struct IcebergScan {
    state: IcebergScanState,
}

/// Descriptor-derived relation facts used while a scan session is constructed.
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

struct RegularScanState {
    prepared: PreparedRowScan,
    cursor: PgRowCursor,
}

impl RegularScanState {
    fn begin(
        relation: &ScanRelation,
        snapshot: &SnapshotHandle,
        _keys: &OwnedScanKeys,
    ) -> AmResult<Self> {
        // Bind while the callback borrows PG's registered snapshot. Retain
        // only owned Iceberg state; no SnapshotHandle escapes this callback.
        let snapshot = ManagedTableReadView::for_snapshot(relation.oid, snapshot)?
            .into_read_snapshot()?;
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
        // for SeqScan qualification, so rescan only reopens the traversal over
        // the unchanged prepared read.
        self.cursor = self.prepared.open_row_cursor()?;
        Ok(())
    }

    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<bool> {
        // ExecSupportsBackwardScan treats every SeqScan as backward-capable,
        // so PostgreSQL adds no Material node for a SCROLL cursor over this
        // scan. A Sort above the scan fetches backward itself and reads this
        // scan forward.
        if direction == ScanDirection::Backward {
            return Err(PgReportError::from_parts(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "Iceberg table scans do not support backward fetches",
                None,
                Some(
                    "Add ORDER BY to the cursor query so PostgreSQL sorts the \
                     rows before fetching them backward."
                        .to_owned(),
                ),
            ));
        }
        // SAFETY: PostgreSQL passes a slot created from this relation's
        // descriptor, and `begin` bound the decoder to that relation's full
        // layout. The scan trampoline selected its reset per-fetch context.
        Ok(unsafe { self.cursor.next_into_slot(out) }?)
    }
}

// Row-producing states stay inline intentionally: boxing them would add an
// allocation per scan and an indirection on every scan_getnextslot call merely
// to shrink this once-per-scan state object.
#[allow(clippy::large_enum_variant)]
enum IcebergScanState {
    Regular(RegularScanState),
    Mutation(ManagedMutationScan),
    Analyze(Box<AnalyzeScanState>),
}

impl IcebergScanState {
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    fn analyze(_rel: &RelationHandle, relation: &ScanRelation) -> AmResult<Self> {
        Ok(Self::Analyze(Box::new(AnalyzeScanState::pending(
            Self::prepare_analyze(relation)?,
        ))))
    }

    /// PostgreSQL 15 and 16 never pass the invocation's selected columns, `targrows`,
    /// or inherited `childtargrows` to the callbacks, so this captures the
    /// relation-local approximation while the relation is still borrowed. It
    /// covers built-in non-inherited scans, but can overproduce for a column
    /// list or underfill a child allocation made from its parent's targets.
    /// Every admitted column type uses a built-in analyzer unless a superuser
    /// replaces it with `ALTER TYPE ... SET (ANALYZE = ...)`.
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    fn analyze(rel: &RelationHandle, relation: &ScanRelation) -> AmResult<Self> {
        let sample_budget = rel.analyze_sample_budget()?;
        Ok(Self::Analyze(Box::new(AnalyzeScanState::pending(
            Self::prepare_analyze(relation)?,
            sample_budget,
        ))))
    }

    fn prepare_analyze(relation: &ScanRelation) -> AmResult<AnalyzePreparation> {
        let view = ManagedTableReadView::for_active_snapshot(relation.oid)?;
        let storage_bytes = view.storage_bytes()?;
        let snapshot = view.into_read_snapshot()?;
        let prepared = PreparedRowScan::full(
            snapshot,
            ScanPredicates::unfiltered(),
            &relation.layout,
        )?;
        Ok(AnalyzePreparation::try_new(
            prepared.analyze_input(storage_bytes)?,
        )?)
    }
}

impl AmScan for IcebergTableAm {
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        virtual_slot_callbacks_with_tid()
    }
}

impl AmScanSession for IcebergScan {
    fn scan_begin(
        rel: &RelationHandle,
        snapshot: Option<&SnapshotHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        flags: ScanFlags,
        keys: &OwnedScanKeys,
    ) -> AmResult<Self> {
        let relation = ScanRelation::from_relation(rel)?;
        let state = if flags.is_analyze() {
            IcebergScanState::analyze(rel, &relation)?
        } else {
            let snapshot = snapshot.expect("regular AM scans have a PG snapshot");
            IcebergScanState::Regular(RegularScanState::begin(
                &relation, snapshot, keys,
            )?)
        };
        Ok(Self { state })
    }

    /// One mode dispatch per row.
    #[inline]
    fn scan_getnextslot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<bool> {
        match &mut self.state {
            IcebergScanState::Regular(state) => state.next_into_slot(direction, out),
            IcebergScanState::Mutation(state) => {
                // ModifyTable target scans run forward. RETURNING cursor
                // movement reads the portal's tuplestore instead.
                state.next_into_slot(out)
            }
            // ANALYZE samples through its own callbacks, never scan_getnextslot.
            IcebergScanState::Analyze(_) => {
                unreachable!("PostgreSQL ANALYZE never calls scan_getnextslot")
            }
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
            IcebergScanState::Regular(state) => state.rescan(keys),
            IcebergScanState::Mutation(state) => {
                state.rescan(None)?;
                Ok(())
            }
            IcebergScanState::Analyze(_) => {
                unreachable!("PostgreSQL never rescans an ANALYZE scan")
            }
        }
    }

    /// The scan descriptor drops this session right after `scan_end`, through
    /// the same lifecycle-context callback that releases it when ERROR skips
    /// `table_endscan`.
    fn scan_end(&mut self) -> AmResult<()> {
        Ok(())
    }

    #[cfg(any(feature = "pg15", feature = "pg16"))]
    fn scan_analyze_next_block(
        &mut self,
        block: pg_sys::BlockNumber,
    ) -> AmResult<bool> {
        match &mut self.state {
            IcebergScanState::Analyze(state) => state.next_block(block),
            IcebergScanState::Regular(_) | IcebergScanState::Mutation(_) => {
                unreachable!("PostgreSQL samples blocks only from an ANALYZE scan")
            }
        }
    }

    #[cfg(any(feature = "pg17", feature = "pg18"))]
    fn scan_analyze_next_block(
        &mut self,
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        match &mut self.state {
            IcebergScanState::Analyze(state) => state.next_block(stream),
            IcebergScanState::Regular(_) | IcebergScanState::Mutation(_) => {
                unreachable!("PostgreSQL samples blocks only from an ANALYZE scan")
            }
        }
    }

    fn scan_analyze_next_tuple(
        &mut self,
        _oldest_xmin: pg_sys::TransactionId,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.state {
            IcebergScanState::Analyze(state) => state.next_tuple(out),
            IcebergScanState::Regular(_) | IcebergScanState::Mutation(_) => {
                unreachable!("PostgreSQL samples tuples only from an ANALYZE scan")
            }
        }
    }
}
