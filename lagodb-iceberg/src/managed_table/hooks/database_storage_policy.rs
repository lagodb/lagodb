//! Database-command policy for managed Iceberg storage ownership.
//!
//! PostgreSQL database cloning copies catalog rows without transferring the
//! storage identity represented by LagoDB's local directories and shared
//! tablespace dependencies. Database relocation ignores extension-owned
//! subdirectories before recursively deleting the old default database
//! directory. These two commands therefore require provider-level guards.
//!
//! CREATE DATABASE validation remains entirely PostgreSQL-owned. The utility
//! hook only retains the live statement until PostgreSQL's database
//! OAT_POST_CREATE event, which runs after native validation and before the
//! first physical file copy. ALTER DATABASE SET TABLESPACE still requires a
//! pre-execution guard because PostgreSQL exposes no equivalent object-access
//! event before it moves database files.

use std::cell::Cell;
use std::ffi::CStr;
use std::ptr::NonNull;

use lagodb_core::catalog::{LockedDatabase, get_tablespace_oid};
use lagodb_core::hooks::{
    AlterDatabaseStmtNode, CreatedbStmtNode, HookError, OBJECT_ACCESS_POST_CREATE,
    ObjectAccessEvent, ObjectAccessFilter, ObjectAccessHook, ObjectAccessHookError,
    PostUtilityContext, UtilityHook, UtilityHookError, UtilityNode,
    register_object_access_hook, register_utility_hook,
};
use lagodb_core::resource::{ResourceHandle, forget_resource, remember_resource};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::database_storage_inventory::ManagedDatabaseInventory;

#[derive(Clone, Copy)]
struct PendingCreateDatabase {
    stmt: NonNull<pg_sys::CreatedbStmt>,
    cleanup: ResourceHandle,
}

thread_local! {
    static PENDING_CREATE_DATABASE: Cell<Option<PendingCreateDatabase>> =
        const { Cell::new(None) };
}

struct DatabaseCloneContext;

impl DatabaseCloneContext {
    fn capture(stmt: &pg_sys::CreatedbStmt) {
        let stmt = NonNull::from(stmt);
        PENDING_CREATE_DATABASE.with(|slot| {
            // A valid CREATE DATABASE cannot be nested. Preserve an enclosing
            // command if another hook issues an invalid nested invocation;
            // PostgreSQL's PreventInTransactionBlock will reject the nested one.
            if slot.get().is_some() {
                return;
            }

            let cleanup = remember_resource(move || Self::release(stmt));
            slot.set(Some(PendingCreateDatabase { stmt, cleanup }));
        });
    }

    fn take() -> Option<NonNull<pg_sys::CreatedbStmt>> {
        let pending = PENDING_CREATE_DATABASE.with(Cell::take)?;
        let _ = forget_resource(pending.cleanup);
        Some(pending.stmt)
    }

    fn release(stmt: NonNull<pg_sys::CreatedbStmt>) {
        PENDING_CREATE_DATABASE.with(|slot| {
            if slot.get().is_some_and(|pending| pending.stmt == stmt) {
                slot.set(None);
            }
        });
    }

    fn is_pending() -> bool {
        PENDING_CREATE_DATABASE.with(|slot| slot.get().is_some())
    }
}

struct ManagedDatabaseClonePolicy;

impl UtilityHook for ManagedDatabaseClonePolicy {
    fn name(&self) -> &'static str {
        "managed Iceberg database clone policy"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<CreatedbStmtNode>()
            .expect("hook registered for T_CreatedbStmt");
        DatabaseCloneContext::capture(stmt);
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        debug_assert!(
            !DatabaseCloneContext::is_pending(),
            "database OAT_POST_CREATE did not consume its CREATE DATABASE context",
        );
        Ok(())
    }
}

impl ObjectAccessHook for ManagedDatabaseClonePolicy {
    fn name(&self) -> &'static str {
        "managed Iceberg database clone policy"
    }

    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_CREATE)
            .for_class(pg_sys::DatabaseRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let ObjectAccessEvent::PostCreate {
            class_id,
            sub_id: 0,
            ..
        } = event
        else {
            return Ok(());
        };
        if *class_id != pg_sys::DatabaseRelationId {
            return Ok(());
        }

        let Some(stmt) = DatabaseCloneContext::take() else {
            return Ok(());
        };
        // SAFETY: the utility pre-hook captured the live statement passed to
        // this synchronous ProcessUtility invocation. PostgreSQL fires this
        // event inside createdb(), before ProcessUtility returns or releases
        // the parse tree's memory context.
        let stmt = unsafe { stmt.as_ref() };
        // SAFETY: PostgreSQL has parsed every option, rejected duplicates, and
        // successfully resolved and ShareLocked this template before firing
        // the database OAT_POST_CREATE event.
        let source_name = unsafe { Self::source_database(stmt) };
        let source_oid =
            unsafe { pg_sys::get_database_oid(source_name.as_ptr(), false) };

        if ManagedDatabaseInventory::new(source_oid).contains_any_managed_storage()? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot CREATE DATABASE using template \"{}\" because it contains managed Iceberg storage",
                    source_name.to_string_lossy(),
                ),
            ));
        }
        Ok(())
    }
}

impl ManagedDatabaseClonePolicy {
    /// Read the source selected by PostgreSQL after native CREATE DATABASE
    /// validation has established that the option list is well formed.
    ///
    /// # Safety
    ///
    /// `stmt` must be the live statement for a database OAT_POST_CREATE event
    /// reached through PostgreSQL's `createdb()` path.
    unsafe fn source_database(stmt: &pg_sys::CreatedbStmt) -> &CStr {
        let count = unsafe { pg_sys::list_length(stmt.options) };
        for index in 0..count {
            let option = unsafe {
                pg_sys::list_nth(stmt.options, index).cast::<pg_sys::DefElem>()
            };
            if unsafe { CStr::from_ptr((*option).defname) }.to_bytes() != b"template"
            {
                continue;
            }
            if unsafe { (*option).arg.is_null() } {
                return c"template1";
            }
            let value = unsafe { pg_sys::defGetString(option) };
            return unsafe { CStr::from_ptr(value) };
        }
        c"template1"
    }
}

struct ManagedDatabaseRelocationGuard;

impl UtilityHook for ManagedDatabaseRelocationGuard {
    fn name(&self) -> &'static str {
        "managed Iceberg database relocation guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<AlterDatabaseStmtNode>()
            .expect("hook registered for T_AlterDatabaseStmt");
        let Some(destination_name) = Self::tablespace_destination(stmt) else {
            return Ok(());
        };
        // SAFETY: PostgreSQL owns this parsed identifier for the utility
        // statement's lifetime.
        let database_name = unsafe { CStr::from_ptr(stmt.dbname) };
        let Some(source) =
            LockedDatabase::resolve(database_name, pg_sys::AccessExclusiveLock as _)?
        else {
            return Ok(());
        };

        if !source.is_owned_by_current_user()
            || source.oid() == unsafe { pg_sys::MyDatabaseId }
        {
            return Ok(());
        }

        let destination_oid = get_tablespace_oid(destination_name, true)?;
        if destination_oid == pg_sys::InvalidOid
            || !Self::current_user_can_create_in(destination_oid)
            || destination_oid == pg_sys::GLOBALTABLESPACE_OID
        {
            return Ok(());
        }

        // PostgreSQL treats relocation to the current default tablespace as a
        // successful no-op before checking for other backends or moving files.
        if destination_oid == source.tablespace_oid() {
            return Ok(());
        }

        if source.has_other_backends() {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE,
                format!(
                    "database \"{}\" is being accessed by other users",
                    database_name.to_string_lossy(),
                ),
            ));
        }

        // movedb() only copies the source default tablespace's database
        // directory, ignores extension-owned subdirectories, and then removes
        // that source directory recursively. Explicit non-default and object
        // placements are not affected by this command.
        if ManagedDatabaseInventory::new(source.oid())
            .contains_local_storage_in(source.tablespace_oid())?
        {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot ALTER DATABASE SET TABLESPACE while its current default tablespace contains managed Iceberg storage",
            ));
        }
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

impl ManagedDatabaseRelocationGuard {
    fn tablespace_destination(stmt: &pg_sys::AlterDatabaseStmt) -> Option<&CStr> {
        if unsafe { pg_sys::list_length(stmt.options) } != 1 {
            return None;
        }
        let option =
            unsafe { pg_sys::list_nth(stmt.options, 0).cast::<pg_sys::DefElem>() };
        if unsafe { CStr::from_ptr((*option).defname) }.to_bytes() != b"tablespace" {
            return None;
        }
        let value = unsafe { pg_sys::defGetString(option) };
        Some(unsafe { CStr::from_ptr(value) })
    }

    fn current_user_can_create_in(tablespace_oid: pg_sys::Oid) -> bool {
        unsafe {
            pg_sys::object_aclcheck(
                pg_sys::TableSpaceRelationId,
                tablespace_oid,
                pg_sys::GetUserId(),
                pg_sys::ACL_CREATE.into(),
            ) == pg_sys::AclResult::ACLCHECK_OK
        }
    }
}

pub(super) fn init_hook() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreatedbStmt,
        Box::new(ManagedDatabaseClonePolicy),
    );
    register_object_access_hook(Box::new(ManagedDatabaseClonePolicy));
    register_utility_hook(
        pg_sys::NodeTag::T_AlterDatabaseStmt,
        Box::new(ManagedDatabaseRelocationGuard),
    );
}
