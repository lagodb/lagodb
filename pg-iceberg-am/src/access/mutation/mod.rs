//! Iceberg mutation operations.
//!
//! The module facade exposes the provider's modify/query state. Runtime
//! callbacks live in [`state`], immutable command decisions in [`plan`], and
//! row-delete backends in [`row_delete`].

mod data_file_sink;
mod plan;
mod row_delete;
mod row_identity;
mod state;

pub use row_identity::{
    IcebergFileSource, IcebergModifyQueryState, IcebergModifyScanContext,
};
pub use state::IcebergModifyState;

#[cfg(test)]
mod mutation_state_tests {
    use std::collections::HashMap;
    use std::rc::Rc;

    use super::IcebergModifyScanContext;
    use super::plan::{ModifyCommand, TargetDependency};
    use crate::access::scan::PlannedMutationTasks;
    use crate::catalog::row_mutations::RelationRowRegistry;
    use iceberg_lite::expr::Predicate;
    use iceberg_lite::spec::TableProperties;
    use iceberg_lite::transaction::IsolationLevel;

    #[test]
    fn relation_registry_interns_each_path_once() {
        let registry = RelationRowRegistry::default();
        let first_file = registry.register_file("data/a.parquet").unwrap();
        let second_file = registry.register_file("data/a.parquet").unwrap();
        let other_file = registry.register_file("data/b.parquet").unwrap();
        assert_eq!(first_file, second_file);
        assert_ne!(first_file, other_file);
        assert_eq!(
            registry.file_path(other_file).unwrap().as_ref(),
            "data/b.parquet"
        );
    }

    #[test]
    fn commands_read_their_own_isolation_property() {
        let cases = [
            (
                TableProperties::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL,
                ModifyCommand::Delete,
            ),
            (
                TableProperties::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL,
                ModifyCommand::Update,
            ),
            (
                TableProperties::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL,
                ModifyCommand::Merge,
            ),
        ];

        for (snapshot_property, snapshot_command) in cases {
            let properties = TableProperties::try_from(&HashMap::from([(
                snapshot_property.to_owned(),
                "snapshot".to_owned(),
            )]))
            .unwrap();

            for command in [
                ModifyCommand::Delete,
                ModifyCommand::Update,
                ModifyCommand::Merge,
            ] {
                let expected = if command == snapshot_command {
                    IsolationLevel::Snapshot
                } else {
                    IsolationLevel::Serializable
                };
                assert_eq!(
                    command.table_isolation_level(&properties),
                    Some(expected)
                );
            }
        }
    }

    #[test]
    fn target_dependency_separates_independent_and_required_reads() {
        let empty_scan = IcebergModifyScanContext::new(
            None,
            Predicate::AlwaysTrue,
            Rc::new(PlannedMutationTasks::new(Vec::new())),
        );
        let empty_table_read = TargetDependency::from_context(Some(&empty_scan));
        assert_eq!(empty_table_read.required_snapshot().unwrap(), None);

        let independent = TargetDependency::from_context(None);
        assert_eq!(independent, TargetDependency::Independent);
    }
}
