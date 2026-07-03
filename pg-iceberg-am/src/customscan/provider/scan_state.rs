//! Iceberg-specific runtime state and scan lifecycle.

use crate::access::column_mapping::RelationShape;
use crate::access::mutation::{IcebergModifyQueryState, IcebergModifyScanContext};
use crate::access::scan::{IcebergBatchCursor, ScanSpec};
use crate::predicate::IcebergPredicateTranslator;
use iceberg_lite::expr::Predicate;
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, ModifyBindContext, NextSlotContext,
    ReScanContext, ScanPurpose,
};

use super::IcebergCustomScanProvider;
use super::projection::ProjectionResolver;

/// Per-scan runtime state inside the framework's `CustomScanStateWrapper`.
pub struct IcebergScanState {
    pub(crate) spec: Option<ScanSpec>,
    pub(crate) cursor: Option<IcebergBatchCursor>,
    conflict_filter: Predicate,
    purpose: ScanPurpose,
    modify_binding: Option<ModifyScanBinding<IcebergModifyQueryState>>,
}

impl Default for IcebergScanState {
    fn default() -> Self {
        Self {
            spec: None,
            cursor: None,
            conflict_filter: Predicate::AlwaysTrue,
            purpose: ScanPurpose::Query,
            modify_binding: None,
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

        let predicate = if !ctx.has_pushed_predicates() {
            None
        } else {
            Self::translate_predicates(&ctx)?
        };
        let conflict_filter = if ctx.purpose.is_modify() {
            IcebergPredicateTranslator::conjoin(
                ctx.translate_static_pushed_predicates(|_| {
                    IcebergPredicateTranslator::new()
                })?,
            )
            .unwrap_or(Predicate::AlwaysTrue)
        } else {
            Predicate::AlwaysTrue
        };

        let scan_tuple = ctx.scan_tuple();
        let projection = ProjectionResolver::new(rel_oid)
            .resolve(ctx.required_columns(), scan_tuple)?;
        let shape = RelationShape::from_relation(&ctx.relation);

        let spec = match projection {
            None => {
                ScanSpec::build_with_predicate(rel_oid, spc_oid, predicate, &shape)?
            }
            Some(proj) => {
                let scan_attr_types = scan_tuple.attr_types();
                ScanSpec::build_with_projection(
                    rel_oid,
                    spc_oid,
                    proj,
                    predicate,
                    &scan_attr_types,
                )?
            }
        };

        let purpose = ctx.purpose;
        let state = ctx.state;
        state.purpose = purpose;
        state.conflict_filter = conflict_filter;
        state.spec = Some(spec);
        state.cursor = if purpose.is_modify() {
            None
        } else {
            Some(
                state
                    .spec
                    .as_ref()
                    .expect("scan spec was just installed")
                    .open_batch_cursor()?,
            )
        };
        state.modify_binding = None;

        Ok(())
    }

    pub(crate) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        self.spec.as_ref().map(|spec| {
            IcebergModifyScanContext::new(
                spec.starting_snapshot_id(),
                self.conflict_filter.clone(),
            )
        })
    }

    pub(super) fn bind_modify(
        ctx: ModifyBindContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let binding = ctx.binding;
        let state = ctx.state;
        if state.purpose != ScanPurpose::Modify {
            return Err(CustomScanError::provider(
                crate::error::IcebergError::InvariantViolated(
                    "Modify binding reached a query scan state",
                ),
            ));
        }
        match state.modify_binding.as_ref() {
            Some(existing) if existing == &binding => return Ok(()),
            Some(_) => {
                return Err(CustomScanError::provider(
                    crate::error::IcebergError::InvariantViolated(
                        "Modify scan was bound to two relation states",
                    ),
                ));
            }
            None => {}
        }
        state.spec.as_ref().ok_or_else(|| {
            CustomScanError::provider(crate::error::IcebergError::InvariantViolated(
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
        ctx.check_for_interrupts();

        let purpose = ctx.state.purpose;
        let mut cursor = match ctx.state.cursor.take() {
            Some(cursor) => cursor,
            None if purpose.is_modify() => {
                let binding = ctx.state.modify_binding.clone().ok_or_else(|| {
                    CustomScanError::provider(
                        crate::error::IcebergError::InvariantViolated(
                            "Modify scan executed before outer binding",
                        ),
                    )
                })?;
                ctx.state
                    .spec
                    .as_ref()
                    .ok_or_else(|| {
                        CustomScanError::provider(
                            crate::error::IcebergError::InvariantViolated(
                                "Modify scan has no scan specification",
                            ),
                        )
                    })?
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
        let translated = if ctx.params_changed {
            Some(IcebergPredicateTranslator::conjoin(
                ctx.translate_pushed_predicates(|_| {
                    IcebergPredicateTranslator::new()
                })?,
            ))
        } else {
            None
        };

        let relation_oid = ctx.relation.oid();
        let state = ctx.state;
        let Some(spec) = state.spec.as_mut() else {
            return Ok(());
        };

        if let Some(filter) = translated {
            spec.set_filter(filter);
        }

        state.cursor = Some(if state.purpose.is_modify() {
            let binding = state.modify_binding.clone().ok_or_else(|| {
                CustomScanError::provider(
                    crate::error::IcebergError::InvariantViolated(
                        "Modify rescan occurred before outer binding",
                    ),
                )
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
        // Drop cursor before spec so IO closes before metadata/predicate teardown.
        let _ = state.cursor.take();
        let _ = state.spec.take();
        state.modify_binding = None;
        Ok(())
    }

    /// Translate pushed expressions and AND the survivors into one predicate.
    fn translate_predicates(
        ctx: &BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<Option<Predicate>, CustomScanError> {
        let translated =
            ctx.translate_pushed_predicates(|_| IcebergPredicateTranslator::new())?;
        Ok(IcebergPredicateTranslator::conjoin(translated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_lite::expr::Reference;
    use iceberg_lite::spec::Datum;

    #[test]
    fn conjoin_empty_is_none() {
        assert_eq!(IcebergPredicateTranslator::conjoin(vec![]), None);
    }

    #[test]
    fn conjoin_single_is_unchanged() {
        let p = Reference::new("a").equal_to(Datum::int(1));
        let combined = IcebergPredicateTranslator::conjoin(vec![p.clone()]);
        assert_eq!(combined, Some(p));
    }

    #[test]
    fn conjoin_many_is_left_assoc_conjunction() {
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));
        let combined = IcebergPredicateTranslator::conjoin(vec![
            a.clone(),
            b.clone(),
            c.clone(),
        ]);
        let expected = a.and(b).and(c);
        assert_eq!(combined, Some(expected));
    }
}
