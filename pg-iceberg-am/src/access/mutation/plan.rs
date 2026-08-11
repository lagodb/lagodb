//! Immutable command and validation decisions for one mutation session.

use iceberg_lite::expr::Predicate;
use iceberg_lite::spec::TableProperties;
use iceberg_lite::transaction::{IsolationLevel, RowLevelCommand};
use pgrx::pg_sys;

use super::row_identity::IcebergModifyScanContext;
use crate::access::isolation::PgTransactionIsolation;
use crate::error::{IcebergError, IcebergResult};

#[derive(Debug)]
pub(super) enum ConflictValidationScope {
    StaticTarget(Predicate),
    WholeTable,
}

impl ConflictValidationScope {
    fn from_predicate(predicate: Predicate) -> Self {
        if predicate == Predicate::AlwaysTrue {
            Self::WholeTable
        } else {
            Self::StaticTarget(predicate)
        }
    }

    pub(super) fn into_predicate(self) -> Predicate {
        match self {
            Self::StaticTarget(predicate) => predicate,
            Self::WholeTable => Predicate::AlwaysTrue,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ModifyCommand {
    Insert,
    Delete,
    Update,
    Merge,
}

impl ModifyCommand {
    pub(super) fn from_pg(cmd_type: pg_sys::CmdType::Type) -> IcebergResult<Self> {
        match cmd_type {
            pg_sys::CmdType::CMD_INSERT => Ok(Self::Insert),
            pg_sys::CmdType::CMD_DELETE => Ok(Self::Delete),
            pg_sys::CmdType::CMD_UPDATE => Ok(Self::Update),
            pg_sys::CmdType::CMD_MERGE => Ok(Self::Merge),
            _ => Err(IcebergError::NotImplemented(
                "unsupported PostgreSQL mutation command for Iceberg table",
            )),
        }
    }

    pub(super) fn validation_command(self) -> Option<RowLevelCommand> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(RowLevelCommand::Delete),
            Self::Update => Some(RowLevelCommand::Update),
            Self::Merge => Some(RowLevelCommand::Merge),
        }
    }

    pub(super) fn table_isolation_level(
        self,
        table_properties: &TableProperties,
    ) -> Option<IsolationLevel> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(table_properties.write_delete_isolation_level),
            Self::Update => Some(table_properties.write_update_isolation_level),
            Self::Merge => Some(table_properties.write_merge_isolation_level),
        }
    }

    fn effective_isolation_level(
        self,
        table_properties: &TableProperties,
        transaction_isolation: PgTransactionIsolation,
    ) -> Option<IsolationLevel> {
        self.table_isolation_level(table_properties)
            .map(|table_isolation| {
                transaction_isolation.effective_iceberg(table_isolation)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetDependency {
    Independent,
    ReadRequired(Option<i64>),
}

impl TargetDependency {
    pub(super) fn from_context(
        scan_context: Option<&IcebergModifyScanContext>,
    ) -> Self {
        scan_context.map_or(Self::Independent, |context| {
            Self::ReadRequired(context.starting_snapshot_id)
        })
    }

    pub(super) fn required_snapshot(self) -> IcebergResult<Option<i64>> {
        match self {
            Self::ReadRequired(snapshot_id) => Ok(snapshot_id),
            Self::Independent => Err(IcebergError::InvariantViolated(
                "independent mutation has no target snapshot",
            )),
        }
    }
}

/// Immutable decisions used by validation and by the final statement output.
#[derive(Debug)]
pub(super) struct ValidationPlan {
    pub(super) command: ModifyCommand,
    pub(super) target_dependency: TargetDependency,
    pub(super) isolation_level: Option<IsolationLevel>,
    pub(super) conflict_scope: Option<ConflictValidationScope>,
}

impl ValidationPlan {
    pub(super) fn new(
        command: ModifyCommand,
        target_dependency: TargetDependency,
        table_properties: &TableProperties,
        transaction_isolation: PgTransactionIsolation,
        scan_context: Option<&IcebergModifyScanContext>,
    ) -> Self {
        let isolation_level = command
            .effective_isolation_level(table_properties, transaction_isolation);
        let conflict_scope = if command.validation_command().is_some() {
            scan_context.map(|context| {
                ConflictValidationScope::from_predicate(
                    context.conflict_filter.clone(),
                )
            })
        } else {
            None
        };
        Self {
            command,
            target_dependency,
            isolation_level,
            conflict_scope,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;

    use iceberg_lite::expr::Predicate;
    use iceberg_lite::spec::TableProperties;
    use iceberg_lite::transaction::IsolationLevel;

    use super::super::IcebergModifyScanContext;
    use super::{ModifyCommand, TargetDependency};
    use crate::access::scan::PlannedMutationTasks;

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
