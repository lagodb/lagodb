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
