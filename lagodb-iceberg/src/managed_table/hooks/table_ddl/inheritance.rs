//! Partitioned Iceberg tables stay out of regular inheritance.
//!
//! PostgreSQL enforces this from relkind, which cannot identify a managed
//! partitioned table: its partition spec lives only in Iceberg metadata. The
//! spec is fixed at CREATE, so validating each new edge is sufficient once the
//! metadata exists. Until the CREATE post-hook writes it, a ddl_command_end
//! event trigger can still add edges, which that post-hook rejects. These checks
//! run after PostgreSQL's own permission and column checks, whose errors
//! therefore take precedence, unlike for a native partitioned table.

use lagodb_core::catalog::{has_inheritance_children, has_inheritance_parents};
use lagodb_core::handles::{RelationGuard, RelationHandle};
use lagodb_core::hooks::HookError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::create_command::CreateCommandStack;
use crate::error::IcebergResult;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::storage::StorageContext;

pub(in crate::managed_table) struct PartitionedInheritance;

impl PartitionedInheritance {
    /// Mirrors PostgreSQL's order: ATPrepAddInherit() rejects the child before
    /// ATExecAddInherit() opens the parent.
    pub(in crate::managed_table) fn validate(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<(), HookError> {
        // PostgreSQL holds the lock on both relations of a new edge.
        let child_guard =
            RelationGuard::open_table(child, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        if Self::is_partitioned(&child_guard.as_handle())? {
            return Err(Self::partitioned_child());
        }

        let parent_guard =
            RelationGuard::open_table(parent, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        let parent = parent_guard.as_handle();
        if Self::is_partitioned(&parent)? {
            return Err(Self::partitioned_parent(&parent));
        }
        Ok(())
    }

    /// Rejects the edges a new partitioned table gained before its metadata
    /// existed. The event trigger's final CommandCounterIncrement() makes its
    /// edges visible here, and an edge it removed again is not rejected.
    pub(in crate::managed_table) fn validate_created(
        relation: &RelationHandle<'_>,
    ) -> Result<(), HookError> {
        if has_inheritance_parents(relation.oid())? {
            return Err(Self::partitioned_child());
        }
        if has_inheritance_children(relation.oid())? {
            return Err(Self::partitioned_parent(relation));
        }
        Ok(())
    }

    /// Reads the definition directly, as schema evolution does.
    /// ManagedTableReadView would reject REPEATABLE READ, which only data reads
    /// cannot honor. A relation mid-CREATE has no metadata yet: the pre-hook
    /// rejected PARTITION BY with INHERITS, and validate_created() covers the
    /// edges added afterwards.
    fn is_partitioned(relation: &RelationHandle<'_>) -> IcebergResult<bool> {
        if CreateCommandStack::is_creating(relation.oid()) {
            return Ok(false);
        }
        let context = StorageContext::for_read(relation)?;
        let loaded = TxMetadata::current()
            .current_table_metadata(relation.oid(), context.file_io())?;
        Ok(loaded.is_partitioned())
    }

    fn partitioned_child() -> HookError {
        HookError::with_code(
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            "cannot change inheritance of partitioned table",
        )
    }

    fn partitioned_parent(parent: &RelationHandle<'_>) -> HookError {
        HookError::with_code(
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!(
                "cannot inherit from partitioned table \"{}\"",
                parent.relation_name().as_c_str().to_string_lossy()
            ),
        )
    }
}
