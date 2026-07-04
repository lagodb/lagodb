//! Backend tests for [`super::mutation`].

#[pgrx::pg_schema]
mod tests {
    use pg_lakebase_core::prelude::*;
    use pgrx::pg_sys;

    use crate::access::mutation::{IcebergFileSource, IcebergModifyQueryState};
    use crate::catalog::row_mutations::RelationRowRegistry;

    #[pgrx::pg_test(schema = "tests")]
    fn independent_query_states_share_transaction_file_ids_despite_scan_order() {
        let registry = RelationRowRegistry::default();
        let mut first_query = IcebergModifyQueryState::default();
        let mut second_query = IcebergModifyQueryState::default();
        let relation_oid: pg_sys::Oid = 41.into();
        first_query.relations.insert(relation_oid, registry.clone());
        second_query.relations.insert(relation_oid, registry);

        let first_a = first_query
            .register_scan_identity_source(
                relation_oid,
                &IcebergFileSource::new("data/a.parquet"),
            )
            .unwrap();
        let first_b = first_query
            .register_scan_identity_source(
                relation_oid,
                &IcebergFileSource::new("data/b.parquet"),
            )
            .unwrap();
        let second_b = second_query
            .register_scan_identity_source(
                relation_oid,
                &IcebergFileSource::new("data/b.parquet"),
            )
            .unwrap();
        let second_a = second_query
            .register_scan_identity_source(
                relation_oid,
                &IcebergFileSource::new("data/a.parquet"),
            )
            .unwrap();

        assert_eq!(first_a, second_a);
        assert_eq!(first_b, second_b);
        assert_ne!(first_a, first_b);
    }
}
