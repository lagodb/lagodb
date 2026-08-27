//! Projection policy for Iceberg CustomScan tuple production.

use lagodb_core::customscan::provider::{
    CustomScanError, NeededColumns, ScanTupleDescriptor,
};
use pgrx::pg_sys;

use crate::engine::scan::projection::{ProjectedField, Projection};

/// Resolves core's referenced-column set into the Iceberg scan projection.
pub(super) struct ProjectionResolver;

impl ProjectionResolver {
    pub(super) fn resolve(
        &self,
        needed: NeededColumns<'_>,
        scan_tuple: ScanTupleDescriptor<'_>,
    ) -> Result<Option<Projection>, CustomScanError> {
        self.resolve_with(needed, |attno| scan_tuple.destination_for_attno(attno))
            .map_err(Into::into)
    }

    /// Pure policy seam used by host tests: `All` maps to select-all; projected
    /// layouts map each base attribute to the supplied destination.
    fn resolve_with<D>(
        &self,
        needed: NeededColumns<'_>,
        resolve_destination: D,
    ) -> Result<Option<Projection>, ProjectionError>
    where
        D: Fn(pg_sys::AttrNumber) -> Option<usize>,
    {
        let attnos = match needed {
            NeededColumns::All => return Ok(None),
            NeededColumns::Subset(attnos) => attnos,
        };

        let mut columns = Vec::with_capacity(attnos.len());
        for &attno in attnos {
            let destination = resolve_destination(attno)
                .ok_or(ProjectionError::UnmappedAttno(attno))?;
            columns.push(ProjectedField::new(attno, destination));
        }
        // Keep the storage request in base-schema order. Destination remains
        // independent, so the compact custom tuple can still follow targetlist
        // order while Arrow/Parquet sees a stable physical-field order.
        columns.sort_unstable_by_key(|column| column.attno);
        Ok(Some(Projection::new(columns)))
    }
}

#[derive(Debug, thiserror::Error)]
enum ProjectionError {
    #[error("projected attno {0} has no destination in the scan tuple layout")]
    UnmappedAttno(pg_sys::AttrNumber),
}

impl From<ProjectionError> for CustomScanError {
    fn from(err: ProjectionError) -> Self {
        CustomScanError::internal(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureRelation {
        destinations: Vec<(pg_sys::AttrNumber, usize)>,
    }

    impl FixtureRelation {
        fn run(
            &self,
            needed: NeededColumns<'_>,
        ) -> Result<Option<Projection>, ProjectionError> {
            ProjectionResolver.resolve_with(needed, |attno| {
                self.destinations
                    .iter()
                    .find_map(|(source, dest)| (*source == attno).then_some(*dest))
            })
        }
    }

    fn pairs(proj: &Projection) -> Vec<(pg_sys::AttrNumber, usize)> {
        proj.columns()
            .iter()
            .map(|c| (c.attno, c.destination))
            .collect()
    }

    fn rel_no_dropped() -> FixtureRelation {
        FixtureRelation {
            destinations: vec![(1, 0), (2, 1), (3, 2)],
        }
    }

    fn rel_with_dropped() -> FixtureRelation {
        FixtureRelation {
            destinations: vec![(2, 0), (3, 1)],
        }
    }

    fn rel_zero_live() -> FixtureRelation {
        FixtureRelation {
            destinations: Vec::new(),
        }
    }

    #[test]
    fn policy_all_maps_to_none_select_all() {
        for rel in [rel_no_dropped(), rel_with_dropped(), rel_zero_live()] {
            let got = rel.run(NeededColumns::All).expect("All never errors");
            assert!(got.is_none(), "All must map to None (select-all)");
        }
    }

    #[test]
    fn policy_nonempty_subset_uses_source_order_and_preserves_destination() {
        let rel = rel_no_dropped();
        let proj = rel
            .run(NeededColumns::Subset(&[3, 1]))
            .expect("all attnos resolve")
            .expect("non-empty subset is Some");
        assert_eq!(pairs(&proj), vec![(1, 0), (3, 2)],);

        let rel = rel_with_dropped();
        let proj = rel
            .run(NeededColumns::Subset(&[2, 3]))
            .expect("live attnos resolve")
            .expect("non-empty subset is Some");
        assert_eq!(pairs(&proj), vec![(2, 0), (3, 1)],);
    }

    #[test]
    fn policy_empty_subset_builds_metadata_only_projection() {
        let rel = rel_no_dropped();
        let projection = rel
            .run(NeededColumns::Subset(&[]))
            .expect("empty storage projection is valid")
            .expect("subset always produces a projection");
        assert!(projection.columns().is_empty());
    }

    #[test]
    fn policy_unresolved_attno_in_subset_is_error() {
        let rel = rel_with_dropped();
        let err = rel
            .run(NeededColumns::Subset(&[1, 2]))
            .expect_err("dropped attno must not resolve");
        assert!(matches!(err, ProjectionError::UnmappedAttno(1)));
    }
}
