//! Prepare foreign-table definitions before PG transforms CREATE, then stage
//! only the relation whose creation PostgreSQL reports through OAT_POST_CREATE.
//!
//! Provider finalization deliberately remains in the utility post-hook.
//! PostgreSQL fires `ddl_command_end` before its `ProcessUtility` call returns,
//! so SQL run by that event trigger cannot access a missing Iceberg table being
//! provisioned by `create_remote_if_missing`. The supported visibility boundary
//! starts with the next statement after CREATE returns. Moving staging earlier
//! would precede PostgreSQL's native transformation and validation and would make
//! this provider own their rollback and reentrancy cases. Table resolution must
//! not compensate by staging on a remote lookup failure: the DDL hook and foreign
//! transaction tracker remain the single owners of staged creation.
//!
//! Accepted restriction: a `ddl_command_end` trigger must not drop the foreign
//! relation whose CREATE is still being finalized here. In one backend and one
//! transaction, OAT_POST_CREATE records its OID, the event trigger drops it,
//! and this post-hook then fails to open that OID. The DROP hook has no staged
//! creation to cancel yet, and the command stack records the creation event,
//! not the relation's continued existence. AccessExclusiveLock excludes other
//! transactions; it does not prevent this same-transaction DROP. The resulting
//! ERROR aborts the CREATE instead of silently treating it as a canceled create.
//!
//! This behavior is an accepted design tradeoff, not an outstanding lifecycle
//! fix. Retaining post-utility finalization lets PostgreSQL finish native DDL
//! validation before provider staging. Supporting trigger-driven cancellation
//! would require command lifecycle tracking across nested DDL and subtransaction
//! rollback solely for this currently unsupported use case. Do not add that
//! machinery or suppress the relation-open error without an explicit requirement
//! to support it. DROP after CREATE returns retains its normal staged-cancel
//! behavior.
//!
//! Reference: pg_lake at 44befc06, pg_lake_table/src/ddl/create_table.c,
//! ProcessCreateIcebergTableFromForeignTableStmt, also resolves the relation
//! after PgLakeCommonParentProcessUtility returns. Its external-catalog
//! read-only path errors on name lookup if an end trigger dropped the relation.
//! This comparison documents the shared timing limitation; the acceptance above
//! is our own supported-behavior decision.

use std::{ffi::CStr, panic::AssertUnwindSafe};

use lagodb_core::catalog::{CatalogSnapshot, RelationCatalogEntry};
use lagodb_core::diag::PgError;
use lagodb_core::handles::RelationGuard;
use lagodb_core::hooks::{
    CreateForeignTableStmtNode, OBJECT_ACCESS_POST_CREATE, ObjectAccessEvent,
    ObjectAccessFilter, ObjectAccessHook, ObjectAccessHookError, PostUtilityContext,
    UtilityHook, UtilityHookError, UtilityNode,
};
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::{PgTryBuilder, pg_sys};

use super::super::catalog::{CatalogAccessIdentity, RemoteTableKey};
use super::super::error::IcebergFdwError;
use super::super::options::{ForeignIdentifier, ForeignTableCreateOptions};
use super::super::provider::LagodbIceberg;
use super::super::relation::RemoteTableTarget;
use super::super::schema::{
    ForeignSchemaBinding, ForeignTableCreation, ForeignTableSchema,
};
use super::super::transaction::ForeignTransaction;
use crate::create_command::CreateCommandStack;

pub(super) struct IcebergForeignTableDdl;

impl UtilityHook for IcebergForeignTableDdl {
    fn name(&self) -> &'static str {
        "lagodb-iceberg foreign-table DDL"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let create = statement
            .cast_mut::<CreateForeignTableStmtNode>()
            .expect("registered for CreateForeignTableStmt");
        PgTryBuilder::new(AssertUnwindSafe(|| Self::prepare(create)))
            .catch_others(|error| Err(PgError::from(error).into()))
            .execute()
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let Some(oid) = CreateCommandStack::finish() else {
            return Ok(());
        };
        Self::stage_after_create(oid)
    }
}

impl ObjectAccessHook for IcebergForeignTableDdl {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_CREATE)
            .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let ObjectAccessEvent::PostCreate {
            object_id,
            sub_id: 0,
            arg,
            ..
        } = event
        else {
            return Ok(());
        };
        if arg.is_some_and(|arg| arg.is_internal)
            || !CreateCommandStack::has_pending()
        {
            return Ok(());
        }
        // heap_create_with_catalog fires this event before CreateForeignTable
        // inserts pg_foreign_table. SnapshotSelf identifies the new relation;
        // the utility post-hook performs provider work after native validation.
        let relation =
            RelationCatalogEntry::find(*object_id, CatalogSnapshot::SelfVisible)?
                .expect("PostgreSQL post-create event identifies a live relation");
        if relation.relkind() == pg_sys::RELKIND_FOREIGN_TABLE as i8 {
            CreateCommandStack::record_created(*object_id);
        }
        Ok(())
    }
}

impl IcebergForeignTableDdl {
    fn prepare(
        statement: &mut pg_sys::CreateForeignTableStmt,
    ) -> Result<(), UtilityHookError> {
        CreateCommandStack::begin();
        let server_name = unsafe { CStr::from_ptr(statement.servername) };
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid
            || !LagodbIceberg::handles_server(server_oid)
        {
            return Ok(());
        }
        if !statement.base.tableElts.is_null()
            || !statement.base.inhRelations.is_null()
        {
            // PostgreSQL must finish definition, privilege and option checks
            // before staged-state policy runs in the utility post-hook.
            return Ok(());
        }
        let mut existing = pg_sys::InvalidOid;
        unsafe {
            pg_sys::RangeVarGetAndCheckCreationNamespace(
                statement.base.relation,
                pg_sys::NoLock as _,
                &mut existing,
            );
        }
        if existing != pg_sys::InvalidOid {
            // PostgreSQL owns duplicate errors and IF NOT EXISTS notices. In
            // particular, an existing object must not trigger remote I/O.
            return Ok(());
        }
        let user = unsafe { pg_sys::GetUserId() };
        // Empty definitions require remote schema inference before native
        // CREATE. Admit access before inspecting provider options or state.
        Self::check_server_usage(server_oid, user, server_name)?;
        let options = ForeignTableCreateOptions::from_view(unsafe {
            ForeignOptionView::from_raw(statement.options)
        })?;
        let key = RemoteTableKey::new(
            CatalogAccessIdentity {
                server_oid,
                effective_user: user,
            },
            options.identity().table().clone(),
        );
        ForeignTransaction::ensure_staged_create_available(&key)?;
        let create_remote_if_missing = options.create_remote_if_missing();
        let remote =
            RemoteTableTarget::connect(server_oid, user, options.into_identity())?;
        let Some(table) = remote.load_published()? else {
            if create_remote_if_missing {
                return Err(IcebergFdwError::invalid_option(
                    "create_remote_if_missing",
                    "requires an explicit or inherited column definition",
                )
                .into());
            }
            return Err(IcebergFdwError::TableNotFound {
                table: ForeignIdentifier::table_path(remote.identity().table()),
            }
            .into());
        };
        let schema = remote.identity().definition_schema(table.metadata())?;
        let definition = ForeignTableSchema::from_iceberg(
            schema,
            table.metadata().current_schema(),
            false,
        )?;
        if definition.is_empty() {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail:
                    "Iceberg table has no columns with a supported PostgreSQL mapping"
                        .to_owned(),
            }
            .into());
        }
        statement.base.tableElts = definition.into_pg_list();
        Ok(())
    }

    fn stage_after_create(oid: pg_sys::Oid) -> Result<(), UtilityHookError> {
        // heap_create_with_catalog acquired AccessExclusiveLock for this OID
        // and PostgreSQL retains it through the utility post-hook. It does not
        // prevent a same-transaction event-trigger DROP; the module-level
        // contract deliberately accepts the relation-open ERROR in that case.
        let guard =
            RelationGuard::open_table(oid, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        let relation = guard.as_handle();
        let foreign = unsafe { &*pg_sys::GetForeignTable(oid) };
        if !LagodbIceberg::handles_server(foreign.serverid) {
            return Ok(());
        }
        let options = ForeignTableCreateOptions::from_view(unsafe {
            ForeignOptionView::from_raw(foreign.options)
        })?;
        let user = unsafe { pg_sys::GetUserId() };
        let key = RemoteTableKey::new(
            CatalogAccessIdentity {
                server_oid: foreign.serverid,
                effective_user: user,
            },
            options.identity().table().clone(),
        );
        // Native CREATE has succeeded, but the statement is not committed.
        // Reject another staged owner before catalog access or schema conversion.
        ForeignTransaction::ensure_staged_create_available(&key)?;
        let remote = RemoteTableTarget::connect(
            foreign.serverid,
            user,
            options.identity().clone(),
        )?;
        if let Some(table) = remote.load_published()? {
            return ForeignSchemaBinding::new(&relation)?
                .validate_definition(table.metadata(), remote.identity())
                .map_err(Into::into);
        }
        if !options.create_remote_if_missing() {
            return Err(IcebergFdwError::TableNotFound {
                table: ForeignIdentifier::table_path(remote.identity().table()),
            }
            .into());
        }
        if relation.live_columns().is_empty() {
            return Err(IcebergFdwError::invalid_option(
                "create_remote_if_missing",
                "requires an explicit or inherited column definition",
            )
            .into());
        }
        let schema = ForeignTableCreation::from_relation(&relation)?;
        let table = remote.stage_create(oid, schema)?;
        ForeignSchemaBinding::new(&relation)?
            .validate_definition(table.metadata(), remote.identity())?;
        Ok(())
    }

    fn check_server_usage(
        server_oid: pg_sys::Oid,
        user: pg_sys::Oid,
        server_name: &CStr,
    ) -> Result<(), IcebergFdwError> {
        let result = unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                user,
                pg_sys::ACL_USAGE.into(),
            )
        };
        if result != pg_sys::AclResult::ACLCHECK_OK {
            return Err(IcebergFdwError::ServerUsageDenied {
                server: server_name.to_string_lossy().into_owned(),
            });
        }
        Ok(())
    }
}
