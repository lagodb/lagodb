//! Managed schema-evolution operations prepared against a loaded table.

use std::collections::HashMap;

use iceberg_lite::table::Table;
use iceberg_lite::transaction::{AddColumn, PreparedSchemaUpdate, Transaction};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;

use super::super::schema_builder::column_type_to_iceberg_type;
use crate::error::{IcebergError, IcebergResult};

#[derive(Debug, Clone)]
enum SchemaEvolutionOp {
    AddNullableColumn { name: String },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    DropNotNull { name: String },
}

#[derive(Debug, Default, Clone)]
pub(in crate::managed_table) struct SchemaEvolutionUpdate {
    ops: Vec<SchemaEvolutionOp>,
}

impl SchemaEvolutionUpdate {
    pub(in crate::managed_table) fn new() -> Self {
        Self { ops: Vec::new() }
    }

    #[must_use]
    pub(in crate::managed_table) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(in crate::managed_table) fn add_nullable_column(
        &mut self,
        name: impl Into<String>,
    ) {
        self.ops
            .push(SchemaEvolutionOp::AddNullableColumn { name: name.into() });
    }

    pub(in crate::managed_table) fn drop_column(&mut self, name: impl Into<String>) {
        self.ops
            .push(SchemaEvolutionOp::DropColumn { name: name.into() });
    }

    pub(in crate::managed_table) fn rename_column(
        &mut self,
        old_name: impl Into<String>,
        new_name: impl Into<String>,
    ) {
        self.ops.push(SchemaEvolutionOp::RenameColumn {
            old_name: old_name.into(),
            new_name: new_name.into(),
        });
    }

    pub(in crate::managed_table) fn drop_not_null(
        &mut self,
        name: impl Into<String>,
    ) {
        self.ops
            .push(SchemaEvolutionOp::DropNotNull { name: name.into() });
    }

    pub(super) fn preflight(&self, table: &Table) -> IcebergResult<()> {
        if !self.requires_existing_schema_preflight() {
            return Ok(());
        }
        let mut action = Transaction::new(table).update_schema();
        for op in &self.ops {
            action = match op {
                SchemaEvolutionOp::AddNullableColumn { .. } => action,
                SchemaEvolutionOp::DropColumn { name } => {
                    action.delete_column(name.as_str())
                }
                SchemaEvolutionOp::RenameColumn { old_name, new_name } => {
                    action.rename_column(old_name.as_str(), new_name.as_str())
                }
                SchemaEvolutionOp::DropNotNull { name } => {
                    action.make_column_optional(name.as_str())
                }
            };
        }
        action.prepare(table)?;
        Ok(())
    }

    pub(super) fn prepare_for_relation(
        &self,
        relation: &RelationHandle<'_>,
        table: &Table,
    ) -> IcebergResult<PreparedSchemaUpdate> {
        let columns = self
            .has_add_column()
            .then(|| RelationColumns::capture(relation))
            .transpose()?;
        let mut action = Transaction::new(table).update_schema();
        for op in &self.ops {
            action = match op {
                SchemaEvolutionOp::AddNullableColumn { name } => {
                    let columns = columns
                        .as_ref()
                        .expect("ADD COLUMN captures relation columns above");
                    let pg_type = columns.column_type(name)?;
                    action.add_column(AddColumn::optional(
                        name.as_str(),
                        column_type_to_iceberg_type(
                            name,
                            pg_type.oid,
                            pg_type.typmod,
                        )?,
                    ))
                }
                SchemaEvolutionOp::DropColumn { name } => {
                    action.delete_column(name.as_str())
                }
                SchemaEvolutionOp::RenameColumn { old_name, new_name } => {
                    action.rename_column(old_name.as_str(), new_name.as_str())
                }
                SchemaEvolutionOp::DropNotNull { name } => {
                    action.make_column_optional(name.as_str())
                }
            };
        }
        Ok(action.prepare(table)?)
    }

    fn has_add_column(&self) -> bool {
        self.ops
            .iter()
            .any(|op| matches!(op, SchemaEvolutionOp::AddNullableColumn { .. }))
    }

    pub(in crate::managed_table) fn drop_column_names(
        &self,
    ) -> impl Iterator<Item = &str> {
        self.ops.iter().filter_map(|op| match op {
            SchemaEvolutionOp::DropColumn { name } => Some(name.as_str()),
            SchemaEvolutionOp::AddNullableColumn { .. }
            | SchemaEvolutionOp::RenameColumn { .. }
            | SchemaEvolutionOp::DropNotNull { .. } => None,
        })
    }

    pub(in crate::managed_table) fn has_drop_column(&self) -> bool {
        self.ops
            .iter()
            .any(|op| matches!(op, SchemaEvolutionOp::DropColumn { .. }))
    }

    pub(super) fn requires_existing_schema_preflight(&self) -> bool {
        self.ops
            .iter()
            .any(|op| !matches!(op, SchemaEvolutionOp::AddNullableColumn { .. }))
    }
}

#[derive(Debug)]
struct RelationColumns {
    by_name: HashMap<String, PgType>,
}

impl RelationColumns {
    fn capture(relation: &RelationHandle<'_>) -> IcebergResult<Self> {
        let mut by_name = HashMap::new();
        for column in relation.live_columns().iter() {
            let name = column.name().to_str().map_err(|_| {
                IcebergError::SchemaBuildError(
                    "PostgreSQL column names must be valid UTF-8 for Iceberg"
                        .to_owned(),
                )
            })?;
            by_name.insert(
                name.to_owned(),
                PgType::new(column.type_oid(), column.type_mod()),
            );
        }
        Ok(Self { by_name })
    }

    fn column_type(&self, name: &str) -> IcebergResult<PgType> {
        self.by_name
            .get(name)
            .copied()
            .ok_or_else(|| IcebergError::ColumnNotFound(name.to_owned()))
    }
}
