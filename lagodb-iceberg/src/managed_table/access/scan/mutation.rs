//! Explicit mutation preparation and binding for the native SeqScan adapter.

use std::mem;

use iceberg_lite::expr::Predicate;
use lagodb_core::access::mutation::ModifyScanBinding;
use lagodb_core::customscan::provider::NeededColumns;
use lagodb_core::expr::pushdown::BoundFilterSet;
use lagodb_core::handles::{RelationHandle, SnapshotHandle};

use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::ManagedTableReadView;
use crate::managed_table::access::mutation::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationScan,
    PreparedManagedMutationScan,
};
use crate::predicate::{BoundIcebergPredicate, IcebergFilterError};
use crate::scan::{PreparedRowScan, ScanPredicates};
use crate::schema::projection::{ProjectedAttribute, SlotProjection};

use super::{IcebergScan, IcebergScanState, ScanRelation};

impl IcebergScan {
    pub(crate) fn prepare_modify(
        relation: &RelationHandle<'_>,
        snapshot: &SnapshotHandle<'_>,
        filters: BoundFilterSet<'_, BoundIcebergPredicate>,
        needed_columns: NeededColumns<'_>,
    ) -> Result<(Self, IcebergModifyScanContext), IcebergFilterError> {
        let relation = ScanRelation::from_relation(relation)?;
        let snapshot = ManagedTableReadView::for_snapshot(relation.oid, snapshot)?
            .into_read_snapshot()?;
        let mut prepared = match needed_columns {
            NeededColumns::All => PreparedRowScan::full(
                snapshot,
                ScanPredicates::unfiltered(),
                &relation.layout,
            )?,
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
                    ScanPredicates::unfiltered(),
                    &relation.layout,
                    relation.layout.attr_types(),
                )?
            }
        };
        BoundIcebergPredicate::validate_schema(filters.iter(), prepared.schema_id())?;
        // PG17 SeqScan supplies no quals to TableAM. ModifyTable's explicit
        // channel binds constants and external parameters for this execution.
        // Both remain valid across rescans and serve pruning and reader filtering.
        // PG intentionally retains even exact quals for native SeqScan's EPQ
        // contract (see LagodbCustomModifyProvider::prepare_modify_seq_scan).
        // Reader filtering reduces rows sent through Datum conversion and PG
        // qualification; repeated evaluation on surviving rows alone is not
        // evidence that disabling reader filtering would improve performance.
        let scan_filter = BoundIcebergPredicate::conjoin(filters.rescan_stable());
        prepared.replace_predicates(ScanPredicates::same(scan_filter));
        // Match CustomScan's conflict policy: external parameters may narrow
        // the scan, but only static predicates narrow conflict validation.
        let conflict_filter = BoundIcebergPredicate::conjoin(filters.static_values())
            .unwrap_or(Predicate::AlwaysTrue);
        let prepared =
            PreparedManagedMutationScan::prepare(prepared, conflict_filter)?;
        let context = prepared.context();
        Ok((
            Self {
                relation,
                state: IcebergScanState::MutationPending(prepared),
            },
            context,
        ))
    }

    pub(crate) fn bind_modify(
        &mut self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
    ) -> IcebergResult<()> {
        let IcebergScanState::MutationPending(prepared) =
            mem::replace(&mut self.state, IcebergScanState::Ended)
        else {
            return Err(IcebergError::InvariantViolated(
                "AM mutation scan was not awaiting binding",
            ));
        };
        self.state = IcebergScanState::Mutation(ManagedMutationScan::new(
            prepared,
            binding,
            self.relation.oid,
        )?);
        Ok(())
    }
}
