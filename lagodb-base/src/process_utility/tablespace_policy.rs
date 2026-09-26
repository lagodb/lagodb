//! ProcessUtility policy for LagoDB storage-volume tablespaces.
//!
//! The policy owns the utility-statement lifecycle for creating and protecting
//! storage-volume tablespaces, including ALTER DATABASE SET TABLESPACE.
//! CREATE DATABASE directory storage is checked later by the runtime
//! object-access policy so PostgreSQL completes its native validation first.
//!
//! Runtime and provider hooks registered for these utility tags are limited to
//! inspection and validation: after this policy prepares a statement, they must
//! preserve its tag and target identity.

use std::ffi::{CStr, CString};

use lagodb_core::catalog::{LockedDatabase, get_tablespace_oid};
use lagodb_core::diag::{PgError, SqlStateError};
use lagodb_core::options::{
    CreateTablespaceStorageOptions, TablespaceBinding, TablespaceCacheError,
    TablespaceError, is_distributed_tablespace,
};
use lagodb_core::storage::volume::StorageVolumeId;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::storage::volume_config::{
    DatabaseDirectoryError, DatabaseDirectoryPolicy, StorageVolumeControl,
    StorageVolumeError,
};
use crate::{RuntimeNotPreloaded, ensure_runtime_preloaded};

const VOLUME_BINDING_LOCK_CLASS: u16 = 0x4c56;

#[derive(Debug, thiserror::Error)]
pub(super) enum TablespacePolicyError {
    #[error(transparent)]
    Tablespace(#[from] TablespaceError),
    #[error(transparent)]
    TablespaceCache(#[from] TablespaceCacheError),
    #[error(transparent)]
    Volume(#[from] StorageVolumeError),
    #[error(transparent)]
    Runtime(#[from] RuntimeNotPreloaded),
    #[error("failed to resolve created tablespace: {0}")]
    Catalog(#[from] PgError),
    #[error("cannot alter options of a LagoDB tablespace")]
    AlterOptions,
    #[error(transparent)]
    DatabaseDirectory(#[from] DatabaseDirectoryError),
}

impl SqlStateError for TablespacePolicyError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Tablespace(error) => error.sql_error_code(),
            Self::TablespaceCache(error) => error.sql_error_code(),
            Self::Volume(error) => error.sql_error_code(),
            Self::Runtime(error) => error.sql_error_code(),
            Self::Catalog(error) => error.sql_error_code(),
            Self::AlterOptions => PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            Self::DatabaseDirectory(error) => error.sql_error_code(),
        }
    }
}

#[derive(Clone, Copy)]
enum TablespaceUtility {
    CreateTablespace,
    AlterTablespaceOptions,
    AlterDatabase,
}

struct PendingTablespaceBinding {
    tablespace_name: CString,
    binding: TablespaceBinding,
}

pub(super) struct StorageVolumeTablespacePolicy {
    utility: TablespaceUtility,
    pending_binding: Option<PendingTablespaceBinding>,
}

impl StorageVolumeTablespacePolicy {
    pub(super) fn for_tag(tag: pg_sys::NodeTag) -> Option<Self> {
        let utility = match tag {
            pg_sys::NodeTag::T_CreateTableSpaceStmt => {
                TablespaceUtility::CreateTablespace
            }
            pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt => {
                TablespaceUtility::AlterTablespaceOptions
            }
            pg_sys::NodeTag::T_AlterDatabaseStmt => TablespaceUtility::AlterDatabase,
            _ => return None,
        };
        Some(Self {
            utility,
            pending_binding: None,
        })
    }

    /// Prepare and validate the runtime-owned policy before extension utility
    /// hooks run.
    ///
    /// # Safety
    ///
    /// `node` must point to the live parse node represented by the tag passed
    /// to [`Self::for_tag`].
    pub(super) unsafe fn prepare(
        &mut self,
        node: *mut pg_sys::Node,
        is_top_level: bool,
    ) -> Result<(), TablespacePolicyError> {
        unsafe {
            match self.utility {
                TablespaceUtility::CreateTablespace => {
                    self.prepare_create_tablespace(node.cast(), is_top_level)
                }
                TablespaceUtility::AlterTablespaceOptions => {
                    Self::guard_alter_tablespace(node.cast())
                }
                TablespaceUtility::AlterDatabase => {
                    Self::guard_move_database(node.cast(), is_top_level)
                }
            }
        }
    }

    /// Complete the runtime-owned policy after successful utility execution
    /// and all extension post hooks.
    pub(super) fn complete(&mut self) -> Result<(), TablespacePolicyError> {
        let Some(pending) = self.pending_binding.take() else {
            return Ok(());
        };
        let oid = get_tablespace_oid(pending.tablespace_name.as_c_str(), false)?;
        pending.binding.persist_to_catalog(oid)?;
        StorageVolumeControl::current().bind(pending.binding.volume_id(), oid)?;
        Ok(())
    }

    /// # Safety
    ///
    /// `stmt` must point to the live `CreateTableSpaceStmt` for this utility
    /// invocation.
    unsafe fn prepare_create_tablespace(
        &mut self,
        stmt: *mut pg_sys::CreateTableSpaceStmt,
        is_top_level: bool,
    ) -> Result<(), TablespacePolicyError> {
        let stmt = unsafe { &mut *stmt };
        let Some(options) = CreateTablespaceStorageOptions::extract_from_stmt(stmt)?
        else {
            return Ok(());
        };
        ensure_runtime_preloaded()?;
        // CREATE TABLESPACE already has this PostgreSQL restriction; call it here
        // before any config read or binding lock acquisition.
        unsafe {
            pg_sys::PreventInTransactionBlock(
                is_top_level,
                c"CREATE TABLESPACE".as_ptr(),
            )
        };
        let control = StorageVolumeControl::current();
        let binding = control.resolve_binding(options.volume_name())?;
        let volume_id = binding.volume_id();
        VolumeBindingLock::new(volume_id).acquire();
        control.ensure_unbound_name(options.volume_name(), volume_id)?;
        let tablespace_name =
            unsafe { CStr::from_ptr(stmt.tablespacename) }.to_owned();
        self.pending_binding = Some(PendingTablespaceBinding {
            tablespace_name,
            binding,
        });
        Ok(())
    }

    /// # Safety
    ///
    /// `stmt` must point to the live `AlterTableSpaceOptionsStmt` for this
    /// utility invocation.
    unsafe fn guard_alter_tablespace(
        stmt: *const pg_sys::AlterTableSpaceOptionsStmt,
    ) -> Result<(), TablespacePolicyError> {
        let name = unsafe { CStr::from_ptr((*stmt).tablespacename) };
        let oid = get_tablespace_oid(name, true)?;
        if oid != pg_sys::InvalidOid && is_distributed_tablespace(oid)? {
            return Err(TablespacePolicyError::AlterOptions);
        }
        Ok(())
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

#[derive(Clone, Copy)]
struct VolumeBindingLock {
    id: StorageVolumeId,
}

impl VolumeBindingLock {
    const fn new(id: StorageVolumeId) -> Self {
        Self { id }
    }

    fn acquire(self) {
        let value = self.id.get();
        let tag = pg_sys::LOCKTAG {
            locktag_field1: pg_sys::InvalidOid.to_u32(),
            locktag_field2: (value >> 32) as u32,
            locktag_field3: value as u32,
            locktag_field4: VOLUME_BINDING_LOCK_CLASS,
            locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
            locktag_lockmethodid: pg_sys::USER_LOCKMETHOD as u8,
        };
        // SAFETY: this is a complete cluster-wide advisory tag. sessionLock
        // false makes PostgreSQL release it at top-level transaction end.
        unsafe {
            pg_sys::LockAcquire(&tag, pg_sys::ExclusiveLock as _, false, false);
        }
    }
}
