//! Iceberg-specific runtime state and scan lifecycle.

use std::rc::Rc;

use crate::access::mutation::{IcebergModifyQueryState, IcebergModifyScanContext};
use crate::access::scan::{IcebergBatchCursor, ScanSpec};
use crate::error::IcebergError;
use crate::predicate::IcebergPredicateTranslator;
use crate::relation_binding::{RelationFieldIndex, RelationShape};
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
    active_scan: Option<ActiveScan>,
    cursor: Option<IcebergBatchCursor>,
    conflict_filter: Predicate,
    purpose: ScanPurpose,
    modify_binding: Option<ModifyScanBinding<IcebergModifyQueryState>>,
}

/// The scan specification and predicate lookup originate from the same
/// relation binding pass and remain coupled for the CustomScan lifetime.
struct ActiveScan {
    spec: ScanSpec,
    field_index: Rc<RelationFieldIndex>,
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

enum RowFilterUpdate {
    Unchanged,
    Replace(Option<Predicate>),
}

impl RowFilterUpdate {
    fn from_changed_params(
        ctx: &ReScanContext<'_, IcebergCustomScanProvider>,
        field_index: &Rc<RelationFieldIndex>,
    ) -> Result<Self, CustomScanError> {
        let translated = ctx.pushed_predicates.translate(|_| {
            IcebergPredicateTranslator::with_field_index(Rc::clone(field_index))
        })?;
        Ok(Self::Replace(IcebergPredicateTranslator::conjoin(
            translated,
        )))
    }

    fn apply_to(self, spec: &mut ScanSpec) {
        match self {
            Self::Unchanged => {}
            // Replace(None) is intentional: PARAM_EXEC changed, but no pushed
            // predicate survived translation for the current value. Keeping the
            // old row filter would reuse stale parameter values and could drop
            // valid rows before PostgreSQL's residual qual sees them.
            Self::Replace(predicate) => spec.set_row_filter(predicate),
        }
    }
}

impl IcebergScanState {
    /// Translate pushed quals, build [`ScanSpec`]/[`IcebergBatchCursor`] against
    /// `estate.es_snapshot`, and capture the tuple width.
    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let rel_oid = ctx.relation.oid();
        let spc_oid = ctx.relation.tablespace_oid();
        let scan_tuple = ctx.scan_tuple();
        let projection = ProjectionResolver
            .resolve(ctx.pushed_predicates.required_columns(), scan_tuple)?;
        let shape = RelationShape::from_relation(&ctx.relation);

        let (mut spec, field_index) = match projection {
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

        let has_pushed_predicates = ctx.pushed_predicates.has_pushed_predicates();
        let row_filter = if !has_pushed_predicates {
            None
        } else {
            Self::translate_predicates(&ctx, &field_index)?
        };
        let planning_filter = if !has_pushed_predicates {
            None
        } else {
            Self::translate_rescan_stable_predicates(&ctx, &field_index)?
        };
        let conflict_filter = if ctx.purpose.is_modify() {
            IcebergPredicateTranslator::conjoin(
                ctx.pushed_predicates.translate_static(|_| {
                    IcebergPredicateTranslator::with_field_index(Rc::clone(
                        &field_index,
                    ))
                })?,
            )
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
        state.active_scan = Some(ActiveScan { spec, field_index });
        state.cursor = cursor;
        state.modify_binding = None;

        Ok(())
    }

    pub(super) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        self.active_scan.as_ref().and_then(|scan| {
            let scan_tasks = scan.spec.prepared_mutation_tasks()?;
            Some(IcebergModifyScanContext::new(
                scan.spec.starting_snapshot_id(),
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
                    .spec
                    .open_mutation_batch_cursor(binding, ctx.relation.oid())?
            }
            None => return Ok(false),
        };

        let result = ctx.emit_columns(&mut cursor);
        ctx.state.cursor = Some(cursor);
        result
    }

    /// Re-translate and replace the filter when `params_changed`; always reopen
    /// the cursor.
    pub(super) fn rescan(
        ctx: ReScanContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let relation_oid = ctx.relation.oid();
        let row_filter_update =
            if ctx.params_changed && ctx.pushed_predicates.has_pushed_predicates() {
                let Some(field_index) = ctx
                    .state
                    .active_scan
                    .as_ref()
                    .map(|scan| Rc::clone(&scan.field_index))
                else {
                    return Ok(());
                };
                RowFilterUpdate::from_changed_params(&ctx, &field_index)?
            } else {
                RowFilterUpdate::Unchanged
            };

        let state = ctx.state;
        let Some(scan) = state.active_scan.as_mut() else {
            return Ok(());
        };
        let spec = &mut scan.spec;

        row_filter_update.apply_to(spec);

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

    /// Translate pushed expressions and AND the survivors into one predicate.
    fn translate_predicates(
        ctx: &BeginContext<'_, IcebergCustomScanProvider>,
        field_index: &Rc<RelationFieldIndex>,
    ) -> Result<Option<Predicate>, CustomScanError> {
        let translated = ctx.pushed_predicates.translate(|_| {
            IcebergPredicateTranslator::with_field_index(Rc::clone(field_index))
        })?;
        Ok(IcebergPredicateTranslator::conjoin(translated))
    }

    /// Translate pushed predicates that can safely participate in stable file
    /// planning. `PARAM_EXEC` predicates are deliberately excluded because
    /// PostgreSQL may change those values across rescans of a parameterized
    /// inner path.
    fn translate_rescan_stable_predicates(
        ctx: &BeginContext<'_, IcebergCustomScanProvider>,
        field_index: &Rc<RelationFieldIndex>,
    ) -> Result<Option<Predicate>, CustomScanError> {
        let translated = ctx.pushed_predicates.translate_rescan_stable(|_| {
            IcebergPredicateTranslator::with_field_index(Rc::clone(field_index))
        })?;
        Ok(IcebergPredicateTranslator::conjoin(translated))
    }
}
