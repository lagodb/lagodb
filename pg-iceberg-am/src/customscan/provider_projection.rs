//! Provider-side column projection policy for Iceberg scans.

use pg_lakebase_core::customscan::provider::{CustomScanError, NeededColumns};
use pg_lakebase_core::expr::ColumnNameResolver;
use pg_lakebase_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::access::projection::{ProjectedName, Projection};

/// Resolves core's referenced-column set into the Iceberg scan projection.
pub(super) struct ProjectionResolver<'relation, 'handle> {
    rel_oid: pg_sys::Oid,
    relation: &'relation RelationHandle<'handle>,
    policy: ProjectionPolicy,
}

impl<'relation, 'handle> ProjectionResolver<'relation, 'handle> {
    pub(super) fn new(
        rel_oid: pg_sys::Oid,
        relation: &'relation RelationHandle<'handle>,
    ) -> Self {
        Self {
            rel_oid,
            relation,
            policy: ProjectionPolicy,
        }
    }

    pub(super) fn resolve(
        &self,
        needed: NeededColumns,
    ) -> Result<Option<Projection>, CustomScanError> {
        self.policy
            .resolve(
                needed,
                || self.first_live_user_column(),
                |attno| ColumnNameResolver::new(self.rel_oid).resolve(attno),
            )
            .map_err(Into::into)
    }

    /// Smallest-attno non-dropped user column, for the empty-subset `count(*)`
    /// policy.
    fn first_live_user_column(&self) -> Option<(pg_sys::AttrNumber, String)> {
        self.relation.live_columns().into_iter().next()
    }
}

/// Pure projection policy: `All` maps to select-all; non-empty subsets map to
/// named columns; empty subsets read one live column for `count(*)`, or
/// select-all when the relation has no live user columns.
#[derive(Clone, Copy, Debug, Default)]
struct ProjectionPolicy;

impl ProjectionPolicy {
    fn resolve<F, R>(
        &self,
        needed: NeededColumns,
        first_live: F,
        resolve_name: R,
    ) -> Result<Option<Projection>, ProjectionError>
    where
        F: FnOnce() -> Option<(pg_sys::AttrNumber, String)>,
        R: Fn(pg_sys::AttrNumber) -> Option<String>,
    {
        let attnos = match needed {
            NeededColumns::All => return Ok(None),
            NeededColumns::Subset(attnos) => attnos,
        };

        if attnos.is_empty() {
            return Ok(first_live().map(|(attno, name)| {
                Projection::new(vec![ProjectedName::new(attno, name)])
            }));
        }

        let mut columns = Vec::with_capacity(attnos.len());
        for attno in attnos {
            let name =
                resolve_name(attno).ok_or(ProjectionError::UnresolvedAttno(attno))?;
            columns.push(ProjectedName::new(attno, name));
        }
        Ok(Some(Projection::new(columns)))
    }
}

#[derive(Debug, thiserror::Error)]
enum ProjectionError {
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
    }

    impl FixtureRelation {
        fn first_live(&self) -> Option<(pg_sys::AttrNumber, String)> {
            self.columns
                .iter()
                .find_map(|(attno, name)| name.map(|n| (*attno, n.to_string())))
        }

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
            needed: NeededColumns,
        ) -> Result<Option<Projection>, ProjectionError> {
            ProjectionPolicy.resolve(
                needed,
                || self.first_live(),
                |attno| self.resolve_name(attno),
            )
        }
    }

    fn pairs(proj: &Projection) -> Vec<(pg_sys::AttrNumber, String)> {
        proj.columns()
            .iter()
            .map(|c| (c.attno, c.name.clone()))
            .collect()
    }

    fn rel_no_dropped() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, Some("a")), (2, Some("b")), (3, Some("c"))],
        }
    }

    fn rel_with_dropped() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, None), (2, Some("b")), (3, Some("c"))],
        }
    }

    fn rel_zero_live() -> FixtureRelation {
        FixtureRelation {
            columns: vec![(1, None), (2, None)],
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
    fn policy_nonempty_subset_maps_to_ascending_named_projection() {
        let rel = rel_no_dropped();
        let proj = rel
            .run(NeededColumns::Subset(vec![1, 3]))
            .expect("all attnos resolve")
            .expect("non-empty subset is Some");
        assert_eq!(
            pairs(&proj),
            vec![(1, "a".to_string()), (3, "c".to_string())],
        );
        let names: Vec<&str> = proj.names().collect();
        assert_eq!(names, vec!["a", "c"]);

        let rel = rel_with_dropped();
        let proj = rel
            .run(NeededColumns::Subset(vec![2, 3]))
            .expect("live attnos resolve")
            .expect("non-empty subset is Some");
        assert_eq!(
            pairs(&proj),
            vec![(2, "b".to_string()), (3, "c".to_string())],
        );
    }

    #[test]
    fn policy_empty_subset_maps_to_first_live_single_column() {
        let rel = rel_no_dropped();
        let proj = rel
            .run(NeededColumns::Subset(vec![]))
            .expect("first live exists")
            .expect("empty subset with a live column is Some");
        assert_eq!(pairs(&proj), vec![(1, "a".to_string())]);

        let rel = rel_with_dropped();
        let proj = rel
            .run(NeededColumns::Subset(vec![]))
            .expect("first live exists")
            .expect("empty subset with a live column is Some");
        assert_eq!(pairs(&proj), vec![(2, "b".to_string())]);
    }

    #[test]
    fn policy_empty_subset_zero_live_columns_falls_back_to_none() {
        let rel = rel_zero_live();
        let got = rel
            .run(NeededColumns::Subset(vec![]))
            .expect("zero-live fallback never errors");
        assert!(
            got.is_none(),
            "empty subset with no live column must fall back to select-all (None)",
        );
    }

    #[test]
    fn policy_unresolved_attno_in_subset_is_error() {
        let rel = rel_with_dropped();
        let err = rel
            .run(NeededColumns::Subset(vec![1, 2]))
            .expect_err("dropped attno must not resolve");
        match err {
            ProjectionError::UnresolvedAttno(attno) => assert_eq!(attno, 1),
        }
    }
}
