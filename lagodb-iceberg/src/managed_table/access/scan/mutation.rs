//! Explicit mutation preparation and binding for the native SeqScan adapter.

use iceberg_lite::expr::Predicate;
use lagodb_core::customscan::provider::NeededColumns;
use lagodb_core::expr::pushdown::BoundFilterSet;
use lagodb_core::handles::{RelationHandle, SnapshotHandle};
use lagodb_core::table_am::ModifyScanBinding;
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::managed_table::ManagedTableReadView;
use crate::managed_table::access::mutation::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationScan,
    PreparedManagedMutationScan,
};
use crate::predicate::{BoundIcebergPredicate, IcebergFilterError};
use crate::scan::{
    PreparedRowScan, ReaderPredicate, ScanPredicates, StablePruningPredicate,
};
use crate::schema::projection::{ProjectedAttribute, SlotProjection};

use super::{IcebergScan, IcebergScanState, ScanRelation};

/// Native SeqScan mutation target prepared before ModifyTable binds it.
///
/// This is not a scan session: [`Self::open`] consumes it with the binding and
/// returns the row-ready [`IcebergScan`] that the descriptor will own.
pub(crate) struct PreparedModifySeqScan {
    relation_oid: pg_sys::Oid,
    prepared: PreparedManagedMutationScan,
}

impl PreparedModifySeqScan {
    pub(crate) fn prepare(
        relation: &RelationHandle<'_>,
        snapshot: &SnapshotHandle<'_>,
        filters: BoundFilterSet<'_, BoundIcebergPredicate>,
        needed_columns: NeededColumns<'_>,
    ) -> Result<Self, IcebergFilterError> {
        let relation = ScanRelation::from_relation(relation)?;
        let snapshot = ManagedTableReadView::for_snapshot(relation.oid, snapshot)?
            .into_read_snapshot()?;
        let schema_id = snapshot.schema().schema_id();
        // PostgreSQL SeqScan supplies no quals to TableAM. ModifyTable's explicit
        // channel binds constants and external parameters for this execution.
        // Both remain valid across rescans and serve pruning and reader filtering,
        // except a set, which only prunes files.
        // PG intentionally retains even exact quals for native SeqScan's EPQ
        // contract (see LagodbCustomModifyProvider::prepare_modify_seq_scan).
        // Reader filtering reduces rows sent through Datum conversion and PG
        // qualification; repeated evaluation on surviving rows alone is not
        // evidence that disabling reader filtering would improve performance.
        let predicates = ScanPredicates::new(
            StablePruningPredicate::new(BoundIcebergPredicate::conjoin(
                filters.rescan_stable(),
            )),
            ReaderPredicate::new(BoundIcebergPredicate::conjoin_row_filter(
                filters.rescan_stable(),
            )),
        );
        let prepared = match needed_columns {
            NeededColumns::All => {
                PreparedRowScan::full(snapshot, predicates, &relation.layout)?
            }
            NeededColumns::Subset(attnos) => {
                // Native SeqScan keeps the full relation descriptor. Prune
                // storage columns without compacting their slot destinations.
                let projection = SlotProjection::from_outputs(
                    attnos
                        .iter()
                        .map(|&attno| {
                            ProjectedAttribute::new(attno, (attno - 1) as usize)
                        })
                        .collect(),
                );
                PreparedRowScan::projected(
                    snapshot,
                    projection,
                    predicates,
                    &relation.layout,
                    relation.layout.attr_types(),
                )?
            }
        };
        // Validate after binding the read plan so column-binding errors keep
        // their own SQLSTATE ahead of a planned-predicate schema mismatch.
        BoundIcebergPredicate::validate_schema(filters.iter(), schema_id)?;
        // Match CustomScan's conflict policy: external parameters may narrow
        // the scan, but only static predicates narrow conflict validation.
        let conflict_filter = BoundIcebergPredicate::conjoin(filters.static_values())
            .unwrap_or(Predicate::AlwaysTrue);
        Ok(Self {
            relation_oid: relation.oid,
            prepared: PreparedManagedMutationScan::prepare(
                prepared,
                conflict_filter,
            )?,
        })
    }

    /// Storage context the outer ModifyTable binds before this scan opens.
    pub(crate) fn context(&self) -> IcebergModifyScanContext {
        self.prepared.context()
    }

    /// Open the bound mutation cursor and return the complete scan session.
    pub(crate) fn open(
        self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
    ) -> IcebergResult<IcebergScan> {
        Ok(IcebergScan {
            state: IcebergScanState::Mutation(ManagedMutationScan::new(
                self.prepared,
                binding,
                self.relation_oid,
            )?),
        })
    }
}
