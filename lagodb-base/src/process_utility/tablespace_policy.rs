//! PostgreSQL database-directory placement restrictions.

use std::ffi::CStr;

use lagodb_core::catalog::{LockedDatabase, get_tablespace_oid};
use lagodb_core::diag::{PgError, SqlStateError};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::storage::volume_config::{
    DatabaseDirectoryError, DatabaseDirectoryPolicy,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum TablespacePolicyError {
    #[error(transparent)]
    Catalog(#[from] PgError),
    #[error(transparent)]
    DatabaseDirectory(#[from] DatabaseDirectoryError),
}

impl SqlStateError for TablespacePolicyError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Catalog(error) => error.sql_error_code(),
            Self::DatabaseDirectory(error) => error.sql_error_code(),
        }
    }
}

pub(super) struct StorageVolumeTablespacePolicy;

impl StorageVolumeTablespacePolicy {
    pub(super) fn for_tag(tag: pg_sys::NodeTag) -> Option<Self> {
        (tag == pg_sys::NodeTag::T_AlterDatabaseStmt).then_some(Self)
    }

    /// # Safety
    /// `node` is the live AlterDatabaseStmt selected by `for_tag`.
    pub(super) unsafe fn prepare(
        &mut self,
        node: *mut pg_sys::Node,
        is_top_level: bool,
    ) -> Result<(), TablespacePolicyError> {
        unsafe { Self::guard_move_database(node.cast(), is_top_level) }
    }

    /// # Safety
    ///
    /// `stmt` must point to the live `AlterDatabaseStmt` for this utility
    /// invocation.
    unsafe fn guard_move_database(
        stmt: *const pg_sys::AlterDatabaseStmt,
        is_top_level: bool,
    ) -> Result<(), TablespacePolicyError> {
        let Some(destination_name) =
            (unsafe { Self::alter_database_tablespace(&*stmt) })
        else {
            return Ok(());
        };
        // ALTER DATABASE invokes this only for SET TABLESPACE, after rejecting
        // malformed option combinations and before resolving the database.
        unsafe {
            pg_sys::PreventInTransactionBlock(
                is_top_level,
                c"ALTER DATABASE SET TABLESPACE".as_ptr(),
            )
        };
        let database_name = unsafe { CStr::from_ptr((*stmt).dbname) };
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
        let destination_tablespace = get_tablespace_oid(destination_name, true)?;
        if destination_tablespace == pg_sys::InvalidOid
            || !unsafe { Self::current_user_can_create_in(destination_tablespace) }
        {
            return Ok(());
        }
        DatabaseDirectoryPolicy::check_tablespace(destination_tablespace)?;
        Ok(())
    }

    /// # Safety
    ///
    /// `stmt` must reference a live PostgreSQL `AlterDatabaseStmt` parse tree.
    unsafe fn alter_database_tablespace(
        stmt: &pg_sys::AlterDatabaseStmt,
    ) -> Option<&CStr> {
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

    /// # Safety
    ///
    /// PostgreSQL must have initialized the current backend user and catalog
    /// access state.
    unsafe fn current_user_can_create_in(tablespace: pg_sys::Oid) -> bool {
        unsafe {
            pg_sys::object_aclcheck(
                pg_sys::TableSpaceRelationId,
                tablespace,
                pg_sys::GetUserId(),
                pg_sys::ACL_CREATE.into(),
            ) == pg_sys::AclResult::ACLCHECK_OK
        }
    }
}
