//! Iceberg schema evolution planning for PostgreSQL DDL.
//!
//! The hook layer owns PostgreSQL parse-tree decoding. This module owns the
//! catalog-facing operation object: it reads the post-DDL relation descriptor,
//! prepares an Iceberg schema update against the transaction-local metadata
//! overlay, and stages the prepared action in [`TxMetadata`].

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{AddColumn, Transaction};
use pg_lakebase_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::catalog::schema_builder::column_type_to_iceberg_type;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

#[derive(Debug, Clone)]
enum SchemaEvolutionOp {
    AddNullableColumn { name: String },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    DropNotNull { name: String },
}

/// Ordered schema-evolution operations decoded from one PostgreSQL DDL
/// statement.
#[derive(Debug, Default, Clone)]
pub(crate) struct SchemaEvolutionUpdate {
    ops: Vec<SchemaEvolutionOp>,
}

impl SchemaEvolutionUpdate {
    pub(crate) fn new() -> Self {
        Self { ops: Vec::new() }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn add_nullable_column(&mut self, name: impl Into<String>) {
        self.ops
            .push(SchemaEvolutionOp::AddNullableColumn { name: name.into() });
    }

    pub(crate) fn drop_column(&mut self, name: impl Into<String>) {
        self.ops
            .push(SchemaEvolutionOp::DropColumn { name: name.into() });
    }

    pub(crate) fn rename_column(
        &mut self,
        old_name: impl Into<String>,
        new_name: impl Into<String>,
    ) {
        self.ops.push(SchemaEvolutionOp::RenameColumn {
            old_name: old_name.into(),
            new_name: new_name.into(),
        });
    }

    pub(crate) fn drop_not_null(&mut self, name: impl Into<String>) {
        self.ops
            .push(SchemaEvolutionOp::DropNotNull { name: name.into() });
    }

    /// Dry-run existing-schema operations against the current transaction view.
    ///
    /// ADD COLUMN is intentionally skipped: PostgreSQL's post-DDL relation
    /// descriptor is the source of truth for the added column's type.
    pub(crate) fn preflight_existing_schema_for_relation(
        &self,
        rel: &RelationHandle<'_>,
    ) -> IcebergResult<()> {
        if !self.has_existing_schema_op() {
            return Ok(());
        }

        let (_, table) = Self::table_for_relation(rel, false)?;
        let mut action = Transaction::new(&table).update_schema();
        for op in &self.ops {
            match op {
                SchemaEvolutionOp::AddNullableColumn { .. } => {}
                SchemaEvolutionOp::DropColumn { name } => {
                    action = action.delete_column(name.as_str());
                }
                SchemaEvolutionOp::RenameColumn { old_name, new_name } => {
                    action =
                        action.rename_column(old_name.as_str(), new_name.as_str());
                }
                SchemaEvolutionOp::DropNotNull { name } => {
                    action = action.make_column_optional(name.as_str());
                }
            }
        }
        action.prepare(&table)?;
        Ok(())
    }

    /// Prepare and stage this update for commit with the current PostgreSQL
    /// transaction.
    pub(crate) fn stage_for_relation(
        &self,
        rel: &RelationHandle<'_>,
    ) -> IcebergResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        let (file_io, table) = Self::table_for_relation(rel, true)?;
        let columns = self.has_add_column().then(|| RelationColumns::capture(rel));
        let mut action = Transaction::new(&table).update_schema();
        for op in &self.ops {
            match op {
                SchemaEvolutionOp::AddNullableColumn { name } => {
                    let Some(columns) = columns.as_ref() else {
                        return Err(IcebergError::InvariantViolated(
                            "schema evolution ADD COLUMN missing captured relation columns",
                        ));
                    };
                    let column_type = columns.column_type(name)?;
                    let iceberg_type = column_type_to_iceberg_type(
                        name,
                        column_type.oid,
                        column_type.typmod,
                    )?;
                    action = action
                        .add_column(AddColumn::optional(name.as_str(), iceberg_type));
                }
                SchemaEvolutionOp::DropColumn { name } => {
                    action = action.delete_column(name.as_str());
                }
                SchemaEvolutionOp::RenameColumn { old_name, new_name } => {
                    action =
                        action.rename_column(old_name.as_str(), new_name.as_str());
                }
                SchemaEvolutionOp::DropNotNull { name } => {
                    action = action.make_column_optional(name.as_str());
                }
            }
        }

        let prepared = action.prepare(&table)?;
        TxMetadata::current().stage_schema_update(rel.oid(), prepared, &file_io)
    }

    fn has_add_column(&self) -> bool {
        self.ops
            .iter()
            .any(|op| matches!(op, SchemaEvolutionOp::AddNullableColumn { .. }))
    }

    fn has_existing_schema_op(&self) -> bool {
        self.ops
            .iter()
            .any(|op| !matches!(op, SchemaEvolutionOp::AddNullableColumn { .. }))
    }

    fn table_for_relation(
        rel: &RelationHandle<'_>,
        register_modify: bool,
    ) -> IcebergResult<(FileIO, Table)> {
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?;
        let file_io = ctx.into_file_io();
        let tx_metadata = TxMetadata::current();
        let loaded = if register_modify {
            tx_metadata.begin_table_modify(rel.oid(), &file_io)?
        } else {
            tx_metadata.current_table_metadata(rel.oid(), &file_io)?
        };
        let table = Table::builder()
            .metadata_location(loaded.location)
            .metadata(Arc::new(loaded.metadata))
            .identifier(IcebergTableId::for_relation(rel.oid()).into_table_ident())
            .file_io(file_io.clone())
            .build()?;
        Ok((file_io, table))
    }
}

#[derive(Debug, Clone, Copy)]
struct ColumnType {
    oid: pg_sys::Oid,
    typmod: i32,
}

#[derive(Debug)]
struct RelationColumns {
    by_name: HashMap<String, ColumnType>,
}

impl RelationColumns {
    fn capture(rel: &RelationHandle<'_>) -> Self {
        let attr_types = rel.attr_types();
        let by_name = rel
            .live_columns()
            .into_iter()
            .filter_map(|(attno, name)| {
                let index = usize::try_from(attno - 1).ok()?;
                let (oid, typmod) = *attr_types.get(index)?;
                Some((name, ColumnType { oid, typmod }))
            })
            .collect();
        Self { by_name }
    }

    fn column_type(&self, name: &str) -> IcebergResult<ColumnType> {
        self.by_name
            .get(name)
            .copied()
            .ok_or_else(|| IcebergError::ColumnNotFound(name.to_owned()))
    }
}
