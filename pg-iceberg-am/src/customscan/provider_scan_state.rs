//! Runtime scan lifecycle for the Iceberg CustomScan provider.

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, ReScanContext,
};
use pg_lakebase_core::tuple::Row;

use crate::access::scan::{RelationShape, ScanCursor, ScanSpec};
use crate::customscan::IcebergPredicateTranslator;
use crate::customscan::predicate_translator::fold_left;

use super::provider::IcebergCustomScanProvider;
use super::provider_projection::ProjectionResolver;

/// Per-scan runtime state inside the framework's `CustomScanStateWrapper`.
#[derive(Default)]
pub struct IcebergScanState {
    pub(crate) spec: Option<ScanSpec>,
    pub(crate) cursor: Option<ScanCursor>,
    /// Reusable buffer sized in `begin` to match the scan relation's `TupleDesc`.
    pub(crate) row: Row,
}

impl IcebergScanState {
    /// Translate pushed quals, build [`ScanSpec`]/[`ScanCursor`] against
    /// `estate.es_snapshot`, and size the row buffer.
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

        let projection = ProjectionResolver::new(rel_oid, &ctx.relation)
            .resolve(ctx.referenced_attnos())?;
        let shape = RelationShape::from_relation(&ctx.relation);

        let spec = match projection {
            None => {
                ScanSpec::build_with_predicate(rel_oid, spc_oid, predicate, &shape)?
            }
            Some(proj) => ScanSpec::build_with_projection(
                rel_oid, spc_oid, proj, predicate, &shape,
            )?,
        };

        let cursor = spec.open_cursor()?;
        let natts = ctx.relation.natts();

        let state = ctx.state;
        state.spec = Some(spec);
        state.cursor = Some(cursor);
        state.row = Row::with_capacity(natts);

        Ok(())
    }

    /// Advance the cursor, then materialize the row via
    /// [`NextSlotContext::emit_row`]. Returns `Ok(false)` at end-of-scan
    /// without touching the slot.
    pub(super) fn next_slot(
        mut ctx: NextSlotContext<'_, IcebergCustomScanProvider>,
    ) -> Result<bool, CustomScanError> {
        ctx.check_for_interrupts();

        let produced = {
            let state = &mut *ctx.state;
            let (Some(spec), Some(cursor)) =
                (state.spec.as_ref(), state.cursor.as_mut())
            else {
                return Ok(false);
            };

            cursor.next_row(spec.row_reader(), &mut state.row)?
        };
        if !produced {
            return Ok(false);
        }

        // Move row out so `ctx` is not borrowed across `emit_row`.
        let mut row = core::mem::take(&mut ctx.state.row);
        let result = ctx.emit_row(&mut row);
        ctx.state.row = row;
        result?;

        Ok(true)
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

        state.cursor = Some(spec.open_cursor()?);
        Ok(())
    }

    pub(super) fn end(
        ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let state = ctx.state;
        // Drop cursor before spec so IO closes before metadata/predicate teardown.
        let _ = state.cursor.take();
        let _ = state.spec.take();
        state.row.clear();
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
