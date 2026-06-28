//! Runtime scan lifecycle for the Iceberg CustomScan provider.

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, ReScanContext,
};

use crate::access::column_mapping::RelationShape;
use crate::access::scan::{IcebergBatchCursor, ScanSpec};
use crate::customscan::IcebergPredicateTranslator;
use crate::customscan::predicate_translator::fold_left;

use super::provider::IcebergCustomScanProvider;
use super::provider_projection::ProjectionResolver;

/// Per-scan runtime state inside the framework's `CustomScanStateWrapper`.
#[derive(Default)]
pub struct IcebergScanState {
    pub(crate) spec: Option<ScanSpec>,
    pub(crate) cursor: Option<IcebergBatchCursor>,
}

impl IcebergScanState {
    /// Translate pushed quals, build [`ScanSpec`]/[`IcebergBatchCursor`] against
    /// `estate.es_snapshot`, and capture the tuple width.
    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let rel_oid = ctx.relation.oid();
        let spc_oid = ctx.private_data.tablespace_oid;
        debug_assert_eq!(
            ctx.relation.tablespace_oid(),
            spc_oid,
            "IcebergCustomScanProvider::begin: tablespace_oid drifted between \
             plan-stage capture and execute-time relation",
        );

        let predicate = if !ctx.has_pushed_predicates() {
            None
        } else {
            translate_predicates(&ctx)?
        };

        let scan_tuple = ctx.scan_tuple();
        let projection = ProjectionResolver::new(rel_oid)
            .resolve(ctx.required_columns(), scan_tuple)?;
        let scan_attr_types = scan_tuple.attr_types();
        let shape = RelationShape::from_relation(&ctx.relation);

        let spec = match projection {
            None => {
                ScanSpec::build_with_predicate(rel_oid, spc_oid, predicate, &shape)?
            }
            Some(proj) => ScanSpec::build_with_projection(
                rel_oid,
                spc_oid,
                proj,
                predicate,
                &scan_attr_types,
            )?,
        };

        let cursor = spec.open_batch_cursor(None)?;

        let state = ctx.state;
        state.spec = Some(spec);
        state.cursor = Some(cursor);

        Ok(())
    }

    /// Drive the slot-first cursor straight into the scan slot via
    /// [`NextSlotContext::emit_columns`]. Returns `Ok(false)` at end-of-scan
    /// without touching the slot.
    pub(super) fn next_slot(
        mut ctx: NextSlotContext<'_, IcebergCustomScanProvider>,
    ) -> Result<bool, CustomScanError> {
        ctx.check_for_interrupts();

        // Take the cursor out so `ctx` is not borrowed through `state` across
        // the `&mut self` call to `emit_columns`.
        let Some(mut cursor) = ctx.state.cursor.take() else {
            return Ok(false);
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
            Some(combine_with_and(ctx.translate_pushed_predicates(|_| {
                IcebergPredicateTranslator::new()
            })?))
        } else {
            None
        };

        let state = ctx.state;
        let Some(spec) = state.spec.as_mut() else {
            return Ok(());
        };

        if let Some(filter) = translated {
            spec.set_filter(filter);
        }

        state.cursor = Some(spec.open_batch_cursor(None)?);
        Ok(())
    }

    pub(super) fn end(
        ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let state = ctx.state;
        // Drop cursor before spec so IO closes before metadata/predicate teardown.
        let _ = state.cursor.take();
        let _ = state.spec.take();
        Ok(())
    }
}

/// Translate pushed expressions and AND the survivors into one predicate.
fn translate_predicates(
    ctx: &BeginContext<'_, IcebergCustomScanProvider>,
) -> Result<Option<Predicate>, CustomScanError> {
    let translated =
        ctx.translate_pushed_predicates(|_| IcebergPredicateTranslator::new())?;
    Ok(combine_with_and(translated))
}

fn combine_with_and(items: Vec<Predicate>) -> Option<Predicate> {
    fold_left(items, Predicate::and)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_lite::expr::Reference;
    use iceberg_lite::spec::Datum;

    #[test]
    fn combine_with_and_empty_is_none() {
        assert_eq!(combine_with_and(vec![]), None);
    }

    #[test]
    fn combine_with_and_single_is_unchanged() {
        let p = Reference::new("a").equal_to(Datum::int(1));
        let combined = combine_with_and(vec![p.clone()]);
        assert_eq!(combined, Some(p));
    }

    #[test]
    fn combine_with_and_many_is_left_assoc_conjunction() {
        let a = Reference::new("a").equal_to(Datum::int(1));
        let b = Reference::new("b").equal_to(Datum::int(2));
        let c = Reference::new("c").equal_to(Datum::int(3));
        let combined = combine_with_and(vec![a.clone(), b.clone(), c.clone()]);
        let expected = a.and(b).and(c);
        assert_eq!(combined, Some(expected));
    }
}
