use std::cell::RefCell;
use std::ffi::CStr;

use pg_lakebase_core::catalog::get_tablespace_oid;
use pg_lakebase_core::diag::{PgReportError, SqlStateError};
use pg_lakebase_core::options::{
    CreateTablespaceStorageOptions, TablespaceBinding, TablespaceCacheError,
    TablespaceError, is_distributed_tablespace,
};
use pg_lakebase_core::storage::volume::StorageVolumeId;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::control::StorageVolumeControl;
use super::domain::StorageVolumeError;

const VOLUME_BINDING_LOCK_CLASS: u16 = 0x4c56;

#[derive(Debug, thiserror::Error)]
enum BindingError {
    #[error(transparent)]
    Tablespace(#[from] TablespaceError),
    #[error(transparent)]
    TablespaceCache(#[from] TablespaceCacheError),
    #[error(transparent)]
    Volume(#[from] StorageVolumeError),
    #[error("failed to resolve created tablespace: {0}")]
    Catalog(#[from] pg_lakebase_core::diag::PgError),
    #[error("cannot alter options of a LagoDB tablespace")]
    AlterOptions,
}

impl SqlStateError for BindingError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Tablespace(error) => error.sql_error_code(),
            Self::TablespaceCache(error) => error.sql_error_code(),
            Self::Volume(error) => error.sql_error_code(),
            Self::Catalog(error) => error.sql_error_code(),
            Self::AlterOptions => PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        }
    }
}

impl BindingError {
    fn report(self) -> ! {
        PgReportError::from_domain_error(self).report()
    }
}

struct PendingBinding {
    tablespace_name: Vec<u8>,
    binding: TablespaceBinding,
}

thread_local! {
    static PENDING_BINDING: RefCell<Option<PendingBinding>> = const { RefCell::new(None) };
}

pub(crate) fn handles_utility(tag: pg_sys::NodeTag) -> bool {
    matches!(
        tag,
        pg_sys::NodeTag::T_CreateTableSpaceStmt
            | pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt
    )
}

/// Runtime-owned pre phase. It runs before extension utility hooks.
///
/// # Safety
/// `node` must point to the live parse node matching its tag.
pub(crate) unsafe fn utility_pre(node: *mut pg_sys::Node, is_top_level: bool) {
    let result = unsafe {
        match (*node).type_ {
            pg_sys::NodeTag::T_CreateTableSpaceStmt => {
                prepare_create(node.cast(), is_top_level)
            }
            pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt => guard_alter(node.cast()),
            _ => Ok(()),
        }
    };
    if let Err(error) = result {
        error.report();
    }
}

/// Runtime-owned final post phase. It runs after all extension post hooks.
///
/// # Safety
/// `original_node` is the router-owned copy of the original parse node.
pub(crate) unsafe fn utility_post(original_node: *mut pg_sys::Node) {
    if unsafe { (*original_node).type_ } != pg_sys::NodeTag::T_CreateTableSpaceStmt {
        return;
    }
    let Some(pending) = PENDING_BINDING.with(|slot| slot.borrow_mut().take()) else {
        return;
    };
    let stmt = unsafe { &*original_node.cast::<pg_sys::CreateTableSpaceStmt>() };
    let statement_name = unsafe { CStr::from_ptr(stmt.tablespacename) };
    if statement_name.to_bytes() != pending.tablespace_name.as_slice() {
        BindingError::Volume(StorageVolumeError::Invariant(
            "pending tablespace binding does not match utility statement",
        ))
        .report();
    }
    let result = (|| -> Result<(), BindingError> {
        let oid = get_tablespace_oid(statement_name, false)?;
        pending.binding.persist_to_catalog(oid)?;
        StorageVolumeControl::current().bind(pending.binding.volume_id(), oid)?;
        Ok(())
    })();
    if let Err(error) = result {
        error.report();
    }
}

unsafe fn prepare_create(
    stmt: *mut pg_sys::CreateTableSpaceStmt,
    is_top_level: bool,
) -> Result<(), BindingError> {
    PENDING_BINDING.with(|slot| *slot.borrow_mut() = None);
    let stmt = unsafe { &mut *stmt };
    let Some(options) = CreateTablespaceStorageOptions::extract_from_stmt(stmt)?
    else {
        return Ok(());
    };
    crate::ensure_runtime_preloaded();
    // CREATE TABLESPACE already has this PostgreSQL restriction; call it here
    // before any config read or binding lock acquisition.
    unsafe {
        pg_sys::PreventInTransactionBlock(is_top_level, c"CREATE TABLESPACE".as_ptr())
    };
    let control = StorageVolumeControl::current();
    let binding = control.resolve_binding(options.volume_name())?;
    let volume_id = binding.volume_id();
    VolumeBindingLock::new(volume_id).acquire();
    control.ensure_unbound_name(options.volume_name(), volume_id)?;
    let tablespace_name = unsafe { CStr::from_ptr(stmt.tablespacename) }
        .to_bytes()
        .to_vec();
    PENDING_BINDING.with(|slot| {
        *slot.borrow_mut() = Some(PendingBinding {
            tablespace_name,
            binding,
        });
    });
    Ok(())
}

unsafe fn guard_alter(
    stmt: *const pg_sys::AlterTableSpaceOptionsStmt,
) -> Result<(), BindingError> {
    let name = unsafe { CStr::from_ptr((*stmt).tablespacename) };
    let oid = get_tablespace_oid(name, true)?;
    if oid != pg_sys::InvalidOid && is_distributed_tablespace(oid)? {
        return Err(BindingError::AlterOptions);
    }
    Ok(())
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
