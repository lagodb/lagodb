//! Provider-side column projection policy for Iceberg scans.

use pg_lakebase_core::customscan::provider::{
    CustomScanError, NeededColumns, ScanTupleDescriptor,
};
use pg_lakebase_core::expr::ColumnNameResolver;
use pgrx::pg_sys;

use crate::access::projection::{ProjectedName, Projection};

/// Resolves core's referenced-column set into the Iceberg scan projection.
pub(super) struct ProjectionResolver {
    rel_oid: pg_sys::Oid,
    policy: ProjectionPolicy,
}

impl ProjectionResolver {
    pub(super) fn new(rel_oid: pg_sys::Oid) -> Self {
        Self {
            rel_oid,
            policy: ProjectionPolicy,
        }
    }

    pub(super) fn resolve(
        &self,
        needed: NeededColumns<'_>,
        scan_tuple: ScanTupleDescriptor<'_>,
    ) -> Result<Option<Projection>, CustomScanError> {
        self.policy
            .resolve(
                needed,
                |attno| scan_tuple.destination_for_attno(attno),
                |attno| ColumnNameResolver::new(self.rel_oid).resolve(attno),
            )
            .map_err(Into::into)
    }
}

/// Pure projection policy: `All` maps to select-all; projected layouts map
/// each base attribute to the destination supplied by Core's tuple contract.
#[derive(Clone, Copy, Debug, Default)]
struct ProjectionPolicy;

impl ProjectionPolicy {
    fn resolve<D, R>(
        &self,
        needed: NeededColumns<'_>,
        resolve_destination: D,
        resolve_name: R,
    ) -> Result<Option<Projection>, ProjectionError>
    where
        D: Fn(pg_sys::AttrNumber) -> Option<usize>,
        R: Fn(pg_sys::AttrNumber) -> Option<String>,
    {
        let attnos = match needed {
            NeededColumns::All => return Ok(None),
            NeededColumns::Subset(attnos) => attnos,
        };

        if attnos.is_empty() {
            return Err(ProjectionError::EmptyProjectedLayout);
        }

        let mut columns = Vec::with_capacity(attnos.len());
        for &attno in attnos {
            let destination = resolve_destination(attno)
                .ok_or(ProjectionError::UnmappedAttno(attno))?;
            let name =
                resolve_name(attno).ok_or(ProjectionError::UnresolvedAttno(attno))?;
            columns.push(ProjectedName::new(attno, destination, name));
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
    #[error("projected scan tuple layout contains no source columns")]
    EmptyProjectedLayout,
    #[error("projected attno {0} has no destination in the scan tuple layout")]
    UnmappedAttno(pg_sys::AttrNumber),
    #[error(
        "projected attno {0} could not be resolved to a live column name \
         (stale plan or dropped attribute)"
    )]
    UnresolvedAttno(pg_sys::AttrNumber),
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
        columns: Vec<(pg_sys::AttrNumber, Option<&'static str>)>,
        destinations: Vec<(pg_sys::AttrNumber, usize)>,
    }

    impl FixtureRelation {
        fn resolve_name(&self, attno: pg_sys::AttrNumber) -> Option<String> {
            self.columns.iter().find_map(|(a, name)| {
                if *a == attno {
                    name.map(|n| n.to_string())
                } else {
                    None
                }
            })
        }

        fn run(
            &self,
            needed: NeededColumns<'_>,
        ) -> Result<Option<Projection>, ProjectionError> {
            ProjectionPolicy.resolve(
                needed,
                |attno| {
                    self.destinations.iter().find_map(|(source, dest)| {
                        (*source == attno).then_some(*dest)
                    })
                },
                |attno| self.resolve_name(attno),
            )
        }
    }

    fn pairs(proj: &Projection) -> Vec<(pg_sys::AttrNumber, usize, String)> {
        proj.columns()
            .iter()
            .map(|c| (c.attno, c.destination, c.name.clone()))
            .collect()
    }

    fn rel_no_dropped() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, Some("a")), (2, Some("b")), (3, Some("c"))],
            destinations: vec![(1, 0), (2, 1), (3, 2)],
        }
    }

    fn rel_with_dropped() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, None), (2, Some("b")), (3, Some("c"))],
            destinations: vec![(2, 0), (3, 1)],
        }
    }

    fn rel_zero_live() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, None), (2, None)],
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
        assert_eq!(
            pairs(&proj),
            vec![(1, 0, "a".to_string()), (3, 2, "c".to_string())],
        );
        let names: Vec<&str> = proj.names().collect();
        assert_eq!(names, vec!["a", "c"]);

        let rel = rel_with_dropped();
        let proj = rel
            .run(NeededColumns::Subset(&[2, 3]))
            .expect("live attnos resolve")
            .expect("non-empty subset is Some");
        assert_eq!(
            pairs(&proj),
            vec![(2, 0, "b".to_string()), (3, 1, "c".to_string())],
        );
    }

    #[test]
    fn policy_empty_subset_is_contract_error() {
        let rel = rel_no_dropped();
        assert!(matches!(
            rel.run(NeededColumns::Subset(&[])),
            Err(ProjectionError::EmptyProjectedLayout)
        ));
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
