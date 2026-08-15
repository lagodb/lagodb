//! Iceberg-specific runtime state and scan lifecycle.

use crate::access::mutation::{IcebergModifyQueryState, IcebergModifyScanContext};
use crate::access::scan::{IcebergBatchCursor, ScanSpec};
use crate::error::IcebergError;
use crate::predicate::BoundIcebergPredicate;
use crate::relation_binding::RelationShape;
use iceberg_lite::expr::Predicate;
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::customscan::modify::ModifyBindContext;
use pg_lakebase_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, ReScanContext,
    ScanPurpose,
};

use super::IcebergCustomScanProvider;
use super::projection::ProjectionResolver;

/// Per-scan runtime state inside the framework's `CustomScanStateWrapper`.
pub(super) struct IcebergScanState {
    active_scan: Option<ScanSpec>,
    cursor: Option<IcebergBatchCursor>,
    conflict_filter: Predicate,
    purpose: ScanPurpose,
    modify_binding: Option<ModifyScanBinding<IcebergModifyQueryState>>,
}

impl Default for IcebergScanState {
    fn default() -> Self {
        Self {
            active_scan: None,
            cursor: None,
            conflict_filter: Predicate::AlwaysTrue,
            purpose: ScanPurpose::Query,
            modify_binding: None,
        }
    }
}

impl IcebergScanState {
    /// Build [`ScanSpec`]/[`IcebergBatchCursor`] and install already-bound
    /// planned predicates.
    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let rel_oid = ctx.relation.oid();
        let spc_oid = ctx.relation.tablespace_oid();
        let scan_tuple = ctx.scan_tuple();
        let projection =
            ProjectionResolver.resolve(ctx.required_columns(), scan_tuple)?;
        let shape = RelationShape::from_relation(&ctx.relation)?;

        let mut spec = match projection {
            None => {
                ScanSpec::build_for_custom_scan(rel_oid, spc_oid, None, None, &shape)?
            }
            Some(proj) => {
                let scan_attr_types = scan_tuple.attr_types();
                ScanSpec::build_with_projection(
                    rel_oid,
                    spc_oid,
                    proj,
                    None,
                    None,
                    &shape,
                    &scan_attr_types,
                )?
            }
        };

        BoundIcebergPredicate::validate_schema(ctx.filters.iter(), spec.schema_id())
            .map_err(CustomScanError::provider)?;
        let row_filter = BoundIcebergPredicate::conjoin(ctx.filters.iter());
        let planning_filter =
            BoundIcebergPredicate::conjoin(ctx.filters.rescan_stable());
        let conflict_filter = if ctx.purpose.is_modify() {
            BoundIcebergPredicate::conjoin(ctx.filters.static_values())
                .unwrap_or(Predicate::AlwaysTrue)
        } else {
            Predicate::AlwaysTrue
        };
        spec.set_predicates(planning_filter, row_filter);

        let purpose = ctx.purpose;
        let state = ctx.state;
        state.purpose = purpose;
        state.conflict_filter = conflict_filter;
        if purpose.is_modify() {
            spec.prepare_mutation_tasks()?;
        }

        let cursor = if purpose.is_modify() {
            None
        } else {
            Some(spec.open_batch_cursor()?)
        };
        state.active_scan = Some(spec);
        state.cursor = cursor;
        state.modify_binding = None;

        Ok(())
    }

    pub(super) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        self.active_scan.as_ref().and_then(|scan| {
            let scan_tasks = scan.prepared_mutation_tasks()?;
            Some(IcebergModifyScanContext::new(
                scan.starting_snapshot_id(),
                self.conflict_filter.clone(),
                scan_tasks,
            ))
        })
    }

    pub(super) fn bind_modify(
        ctx: ModifyBindContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let binding = ctx.binding;
        let state = ctx.state;
        match state.modify_binding.as_ref() {
            Some(existing) if existing == &binding => return Ok(()),
            Some(_) => {
                return Err(CustomScanError::provider(
                    IcebergError::InvariantViolated(
                        "Modify scan was bound to two relation states",
                    ),
                ));
            }
            None => {}
        }
        state.active_scan.as_ref().ok_or_else(|| {
            CustomScanError::provider(IcebergError::InvariantViolated(
                "Modify scan binding has no scan specification",
            ))
        })?;
        state.modify_binding = Some(binding);
        Ok(())
    }

    /// Drive the slot-first cursor straight into the scan slot via
    /// [`NextSlotContext::emit_columns`]. Returns `Ok(false)` at end-of-scan
    /// without touching the slot.
    pub(super) fn next_slot(
        mut ctx: NextSlotContext<'_, IcebergCustomScanProvider>,
    ) -> Result<bool, CustomScanError> {
        let purpose = ctx.state.purpose;
        let mut cursor = match ctx.state.cursor.take() {
            Some(cursor) => cursor,
            None if purpose.is_modify() => {
                let binding = ctx.state.modify_binding.clone().ok_or_else(|| {
                    CustomScanError::provider(IcebergError::InvariantViolated(
                        "Modify scan executed before outer binding",
                    ))
                })?;
                ctx.state
                    .active_scan
                    .as_mut()
                    .ok_or_else(|| {
                        CustomScanError::provider(IcebergError::InvariantViolated(
                            "Modify scan has no scan specification",
                        ))
                    })?
                    .open_mutation_batch_cursor(binding, ctx.relation.oid())?
            }
            None => return Ok(false),
        };

        let result = ctx.emit_columns(&mut cursor);
        ctx.state.cursor = Some(cursor);
        result
    }

    /// Replace the complete row filter when values changed; always reopen the
    /// cursor without replanning stable file tasks.
    pub(super) fn rescan(
        ctx: ReScanContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let relation_oid = ctx.relation.oid();
        let replacement = ctx
            .filters_changed
            .then(|| BoundIcebergPredicate::conjoin(ctx.filters.iter()));

        let state = ctx.state;
        let Some(spec) = state.active_scan.as_mut() else {
            return Ok(());
        };
        if let Some(predicate) = replacement {
            spec.set_row_filter(predicate);
        }

        state.cursor = Some(if state.purpose.is_modify() {
            let binding = state.modify_binding.clone().ok_or_else(|| {
                CustomScanError::provider(IcebergError::InvariantViolated(
                    "Modify rescan occurred before outer binding",
                ))
            })?;
            spec.open_mutation_batch_cursor(binding, relation_oid)?
        } else {
            spec.open_batch_cursor()?
        });
        Ok(())
    }

    pub(super) fn end(
        ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let state = ctx.state;
        // Drop cursor before the active scan so IO closes before
        // metadata/predicate teardown.
        let _ = state.cursor.take();
        let _ = state.active_scan.take();
        state.modify_binding = None;
        Ok(())
    }
}
