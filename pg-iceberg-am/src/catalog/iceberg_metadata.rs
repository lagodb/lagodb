//! IcebergMetadata - CRUD operations for the lakebase.iceberg_metadata table.
//!
//! This module provides functions to manage Iceberg table metadata stored in
//! the PostgreSQL catalog table `lakebase.iceberg_metadata`.
//!
//! Table schema:
//! ```sql
//! CREATE TABLE lakebase.iceberg_metadata (
//!     relid regclass NOT NULL,
//!     metadata_location text,
//!     previous_metadata_location text,
//!     default_spec_id integer,
//!     PRIMARY KEY (relid)
//! );
//! ```

use pg_lakebase_core::catalog::LAKEBASE_SCHEMA;
use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::handles::{SysScanGuard, TableGuard};
use pg_lakebase_core::pg_wrapper::{CatalogUpdateResult, PgWrapper};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{FromDatum, IntoDatum, pg_sys};
use std::ffi::CStr;
use std::sync::OnceLock;
use thiserror::Error;

// ============================================================================
//  Constants
// ============================================================================

/// The table name for storing iceberg metadata location.
pub const ICEBERG_METADATA_TABLE: &CStr = c"iceberg_metadata";
pub const ICEBERG_METADATA_PKEY: &CStr = c"iceberg_metadata_pkey";

/// Column numbers in lakebase.iceberg_metadata (1-based)
mod column {
    pub const RELID: i16 = 1;
    pub const METADATA_LOCATION: i16 = 2;
    pub const PREVIOUS_METADATA_LOCATION: i16 = 3;
    pub const DEFAULT_SPEC_ID: i16 = 4;
}

// ============================================================================
//  OID Cache
// ============================================================================

static ICEBERG_METADATA_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static ICEBERG_METADATA_PKEY_OID: OnceLock<pg_sys::Oid> = OnceLock::new();

/// Get the OID of the `lakebase.iceberg_metadata` table.
pub fn get_iceberg_metadata_oid() -> Result<pg_sys::Oid, IcebergMetadataError> {
    if let Some(&oid) = ICEBERG_METADATA_OID.get() {
        return Ok(oid);
    }

    let schema_oid = PgWrapper::get_namespace_oid(LAKEBASE_SCHEMA, false)
        .map_err(|e| IcebergMetadataError::CatalogAccess(e.to_string()))?;
    let oid = PgWrapper::get_relname_relid(ICEBERG_METADATA_TABLE, schema_oid)
        .map_err(|e| IcebergMetadataError::CatalogAccess(e.to_string()))?;

    let _ = ICEBERG_METADATA_OID.set(oid);

    Ok(oid)
}

/// Get the OID of the primary key index on `lakebase.iceberg_metadata`.
pub fn get_iceberg_metadata_pkey_oid() -> Result<pg_sys::Oid, IcebergMetadataError> {
    if let Some(&oid) = ICEBERG_METADATA_PKEY_OID.get() {
        return Ok(oid);
    }

    let schema_oid = PgWrapper::get_namespace_oid(LAKEBASE_SCHEMA, false)
        .map_err(|e| IcebergMetadataError::CatalogAccess(e.to_string()))?;
    let oid = PgWrapper::get_relname_relid(ICEBERG_METADATA_PKEY, schema_oid)
        .map_err(|e| IcebergMetadataError::CatalogAccess(e.to_string()))?;

    let _ = ICEBERG_METADATA_PKEY_OID.set(oid);

    Ok(oid)
}

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when operating on the iceberg_metadata table.
#[derive(Error, Debug, Clone)]
pub enum IcebergMetadataError {
    #[error("catalog access error: {0}")]
    CatalogAccess(String),

    #[error("record not found for relid: {0}")]
    NotFound(pg_sys::Oid),

    #[error("record already exists for relid: {0}")]
    AlreadyExists(pg_sys::Oid),

    #[error("failed to insert record: {0}")]
    InsertFailed(String),

    #[error("failed to update record: {0}")]
    UpdateFailed(String),

    #[error("failed to delete record: {0}")]
    DeleteFailed(String),

    #[error("failed to read record: {0}")]
    ReadFailed(String),

    #[error("optimistic locking failed: metadata location changed concurrently")]
    Conflict,
}

impl SqlStateError for IcebergMetadataError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            IcebergMetadataError::NotFound(_) => {
                PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
            }
            IcebergMetadataError::AlreadyExists(_) => {
                PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION
            }
            IcebergMetadataError::Conflict => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }
            _ => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

impl From<IcebergMetadataError> for ErrorReport {
    fn from(value: IcebergMetadataError) -> Self {
        ErrorReport::new(value.sql_error_code(), format!("{value}"), "")
    }
}

// ============================================================================
//  IcebergMetadata
// ============================================================================

/// Represents a record in the `lakebase.iceberg_metadata` table.
#[derive(Debug, Clone, Default)]
pub struct IcebergMetadata {
    /// The OID of the associated table.
    pub relid: pg_sys::Oid,
    /// The current metadata file location.
    pub metadata_location: Option<String>,
    /// The previous metadata file location.
    pub previous_metadata_location: Option<String>,
    /// The default partition spec ID.
    pub default_spec_id: Option<i32>,
}

impl IcebergMetadata {
    pub fn new(relid: pg_sys::Oid) -> Self {
        Self {
            relid,
            metadata_location: None,
            previous_metadata_location: None,
            default_spec_id: None,
        }
    }

    pub fn with_metadata_location(mut self, location: impl Into<String>) -> Self {
        self.metadata_location = Some(location.into());
        self
    }

    pub fn with_previous_metadata_location(
        mut self,
        location: impl Into<String>,
    ) -> Self {
        self.previous_metadata_location = Some(location.into());
        self
    }

    pub fn with_default_spec_id(mut self, spec_id: i32) -> Self {
        self.default_spec_id = Some(spec_id);
        self
    }

    /// Insert this record into the `lakebase.iceberg_metadata` table.
    ///
    /// Returns an error if a record with the same relid already exists.
    pub fn insert(&self) -> Result<(), IcebergMetadataError> {
        let table_oid = get_iceberg_metadata_oid()?;

        unsafe {
            let rel = PgWrapper::table_open(table_oid, pg_sys::RowExclusiveLock as _)
                .map_err(|e| IcebergMetadataError::InsertFailed(e.to_string()))?;

            // Prepare datum values for all columns
            let relid_datum = self.relid.into_datum().unwrap();
            let metadata_location_datum = self.metadata_location.clone().into_datum();
            let previous_metadata_location_datum =
                self.previous_metadata_location.clone().into_datum();
            let default_spec_id_datum = self.default_spec_id.into_datum();

            let mut values = [
                relid_datum,
                metadata_location_datum.unwrap_or(pg_sys::Datum::from(0)),
                previous_metadata_location_datum.unwrap_or(pg_sys::Datum::from(0)),
                default_spec_id_datum.unwrap_or(pg_sys::Datum::from(0)),
            ];
            let mut nulls = [
                false,
                metadata_location_datum.is_none(),
                previous_metadata_location_datum.is_none(),
                default_spec_id_datum.is_none(),
            ];

            let tup_desc = (*rel).rd_att;
            let tuple = pg_sys::heap_form_tuple(
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            PgWrapper::catalog_tuple_insert(rel, tuple)
                .map_err(|e| IcebergMetadataError::InsertFailed(e.to_string()))?;

            pg_sys::heap_freetuple(tuple);
            PgWrapper::table_close(rel, pg_sys::RowExclusiveLock as _)
                .map_err(|e| IcebergMetadataError::InsertFailed(e.to_string()))?;
        }

        Ok(())
    }

    /// Find a record by relid from the `lakebase.iceberg_metadata` table.
    ///
    /// Returns `Ok(None)` if no record is found.
    pub fn find_by_relid(
        relid: pg_sys::Oid,
    ) -> Result<Option<Self>, IcebergMetadataError> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        // RAII Guards automatically handle resources via Drop trait
        let table_guard =
            TableGuard::open(table_oid, pg_sys::AccessShareLock as _)
                .map_err(|e| IcebergMetadataError::ReadFailed(e.to_string()))?;
        let rel = table_guard.as_raw();

        unsafe {
            let mut key: pg_sys::ScanKeyData = std::mem::zeroed();
            PgWrapper::scan_key_init(
                &mut key,
                column::RELID as pg_sys::AttrNumber,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_OIDEQ),
                relid.into_datum().unwrap(),
            );

            let scan = PgWrapper::systable_beginscan(
                rel,
                index_oid,
                true,
                std::ptr::null_mut(),
                1,
                &key as *const _ as *mut _,
            )
            .map_err(|e| IcebergMetadataError::ReadFailed(e.to_string()))?;

            // Guard the scan to ensure systable_endscan is called
            let _scan_guard = SysScanGuard::from_raw(scan);

            let tuple = PgWrapper::systable_getnext(scan)
                .map_err(|e| IcebergMetadataError::ReadFailed(e.to_string()))?;

            let result = if let Some(tuple) = tuple {
                Some(Self::from_tuple(rel, tuple)?)
            } else {
                None
            };

            // Guards will automatically close scan and table

            Ok(result)
        }
    }

    pub fn get(relid: pg_sys::Oid) -> Result<Self, IcebergMetadataError> {
        Self::find_by_relid(relid)?.ok_or(IcebergMetadataError::NotFound(relid))
    }

    pub fn exists(relid: pg_sys::Oid) -> Result<bool, IcebergMetadataError> {
        Ok(Self::find_by_relid(relid)?.is_some())
    }

    /// Update this record in the `lakebase.iceberg_metadata` table.
    /// Performs an optimistic lock check against `expected_previous_location`.
    ///
    /// Returns an error if the record does not exist or if the current metadata location
    /// does not match `expected_previous_location`.
    pub fn update(
        &self,
        expected_previous_location: Option<&str>,
    ) -> Result<(), IcebergMetadataError> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        // RAII Guards automatically handle resources via Drop trait
        let table_guard = TableGuard::open(table_oid, pg_sys::RowExclusiveLock as _)
            .map_err(|e| IcebergMetadataError::UpdateFailed(e.to_string()))?;
        let rel = table_guard.as_raw();

        unsafe {
            // Find the existing tuple
            let mut key: pg_sys::ScanKeyData = std::mem::zeroed();
            PgWrapper::scan_key_init(
                &mut key,
                column::RELID as pg_sys::AttrNumber,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_OIDEQ),
                self.relid.into_datum().unwrap(),
            );

            let scan = PgWrapper::systable_beginscan(
                rel,
                index_oid,
                true,
                std::ptr::null_mut(),
                1,
                &key as *const _ as *mut _,
            )
            .map_err(|e| IcebergMetadataError::UpdateFailed(e.to_string()))?;

            // Guard the scan to ensure systable_endscan is called
            let _scan_guard = SysScanGuard::from_raw(scan);

            let old_tuple = PgWrapper::systable_getnext(scan)
                .map_err(|e| IcebergMetadataError::UpdateFailed(e.to_string()))?;

            let old_tuple = match old_tuple {
                Some(t) => t,
                None => {
                    // Guards will automatically close scan and table
                    return Err(IcebergMetadataError::NotFound(self.relid));
                }
            };

            // Optimistic Lock Check (CAS)
            // Parse existing tuple to check current value
            let existing_metadata = Self::from_tuple(rel, old_tuple)?;
            if existing_metadata.metadata_location.as_deref()
                != expected_previous_location
            {
                // Guards will automatically close scan and table
                return Err(IcebergMetadataError::Conflict);
            }

            // Prepare new values
            let tup_desc = (*rel).rd_att;
            let natts = (*tup_desc).natts as usize;

            let mut values = vec![pg_sys::Datum::from(0); natts];
            let mut nulls = vec![false; natts];
            let mut repls = vec![false; natts];

            // Update metadata_location (column 2, index 1)
            let metadata_location_datum = self.metadata_location.clone().into_datum();
            values[(column::METADATA_LOCATION - 1) as usize] =
                metadata_location_datum.unwrap_or(pg_sys::Datum::from(0));
            nulls[(column::METADATA_LOCATION - 1) as usize] =
                metadata_location_datum.is_none();
            repls[(column::METADATA_LOCATION - 1) as usize] = true;

            // Update previous_metadata_location (column 3, index 2)
            let previous_metadata_location_datum =
                self.previous_metadata_location.clone().into_datum();
            values[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] =
                previous_metadata_location_datum.unwrap_or(pg_sys::Datum::from(0));
            nulls[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] =
                previous_metadata_location_datum.is_none();
            repls[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] = true;

            // Update default_spec_id (column 4, index 3)
            let default_spec_id_datum = self.default_spec_id.into_datum();
            values[(column::DEFAULT_SPEC_ID - 1) as usize] =
                default_spec_id_datum.unwrap_or(pg_sys::Datum::from(0));
            nulls[(column::DEFAULT_SPEC_ID - 1) as usize] =
                default_spec_id_datum.is_none();
            repls[(column::DEFAULT_SPEC_ID - 1) as usize] = true;

            let new_tuple = pg_sys::heap_modify_tuple(
                old_tuple,
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                repls.as_mut_ptr(),
            );

            // Handle physical-level concurrent modification (PostgreSQL tuple version conflict)
            // This complements the logical CAS check above - even if metadata_location matched
            // our expectation, another transaction might have committed between our read and write.
            // In this case, heap_update returns TM_Updated/TM_Deleted, and we return Conflict
            // to trigger the retry/rebase logic in metadata_tracking.rs.
            match PgWrapper::catalog_tuple_update_optimistic(
                rel,
                &mut (*old_tuple).t_self,
                new_tuple,
            ) {
                Ok(CatalogUpdateResult::Success) => {
                    pg_sys::heap_freetuple(new_tuple);
                }
                Ok(CatalogUpdateResult::Conflict) => {
                    pg_sys::heap_freetuple(new_tuple);
                    return Err(IcebergMetadataError::Conflict);
                }
                Err(e) => {
                    pg_sys::heap_freetuple(new_tuple);
                    return Err(IcebergMetadataError::UpdateFailed(e.to_string()));
                }
            }
            // Guards will automatically close scan and table upon scope exit
        }

        Ok(())
    }

    /// Delete the record for the given relid from the `lakebase.iceberg_metadata` table.
    ///
    /// Returns an error if the record does not exist.
    pub fn delete(relid: pg_sys::Oid) -> Result<(), IcebergMetadataError> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        unsafe {
            let rel = PgWrapper::table_open(table_oid, pg_sys::RowExclusiveLock as _)
                .map_err(|e| IcebergMetadataError::DeleteFailed(e.to_string()))?;

            let mut key: pg_sys::ScanKeyData = std::mem::zeroed();
            PgWrapper::scan_key_init(
                &mut key,
                column::RELID as pg_sys::AttrNumber,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_OIDEQ),
                relid.into_datum().unwrap(),
            );

            let scan = PgWrapper::systable_beginscan(
                rel,
                index_oid,
                true,
                std::ptr::null_mut(),
                1,
                &key as *const _ as *mut _,
            )
            .map_err(|e| IcebergMetadataError::DeleteFailed(e.to_string()))?;

            let tuple = PgWrapper::systable_getnext(scan)
                .map_err(|e| IcebergMetadataError::DeleteFailed(e.to_string()))?;

            let tuple = match tuple {
                Some(t) => t,
                None => {
                    PgWrapper::systable_endscan(scan).map_err(|e| {
                        IcebergMetadataError::DeleteFailed(e.to_string())
                    })?;
                    PgWrapper::table_close(rel, pg_sys::RowExclusiveLock as _)
                        .map_err(|e| {
                            IcebergMetadataError::DeleteFailed(e.to_string())
                        })?;
                    return Err(IcebergMetadataError::NotFound(relid));
                }
            };

            pg_sys::CatalogTupleDelete(rel, &mut (*tuple).t_self);

            PgWrapper::systable_endscan(scan)
                .map_err(|e| IcebergMetadataError::DeleteFailed(e.to_string()))?;
            PgWrapper::table_close(rel, pg_sys::RowExclusiveLock as _)
                .map_err(|e| IcebergMetadataError::DeleteFailed(e.to_string()))?;
        }

        Ok(())
    }

    // ========================================================================
    //  Private helper methods
    // ========================================================================

    /// Extract an IcebergMetadata from a HeapTuple.
    unsafe fn from_tuple(
        rel: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
    ) -> Result<Self, IcebergMetadataError> {
        let tup_desc = unsafe { (*rel).rd_att };

        let mut is_null = false;
        let relid_datum = unsafe {
            pg_sys::heap_getattr(tuple, column::RELID as _, tup_desc, &mut is_null)
        };
        let relid = if is_null {
            return Err(IcebergMetadataError::ReadFailed(
                "relid is null".to_string(),
            ));
        } else {
            unsafe { pg_sys::Oid::from_datum(relid_datum, is_null) }
                .unwrap_or(pg_sys::InvalidOid)
        };

        let metadata_location_datum = unsafe {
            pg_sys::heap_getattr(
                tuple,
                column::METADATA_LOCATION as _,
                tup_desc,
                &mut is_null,
            )
        };
        let metadata_location = if is_null {
            None
        } else {
            unsafe { String::from_datum(metadata_location_datum, is_null) }
        };

        let previous_metadata_location_datum = unsafe {
            pg_sys::heap_getattr(
                tuple,
                column::PREVIOUS_METADATA_LOCATION as _,
                tup_desc,
                &mut is_null,
            )
        };
        let previous_metadata_location = if is_null {
            None
        } else {
            unsafe { String::from_datum(previous_metadata_location_datum, is_null) }
        };

        let default_spec_id_datum = unsafe {
            pg_sys::heap_getattr(
                tuple,
                column::DEFAULT_SPEC_ID as _,
                tup_desc,
                &mut is_null,
            )
        };
        let default_spec_id = if is_null {
            None
        } else {
            unsafe { i32::from_datum(default_spec_id_datum, is_null) }
        };

        Ok(Self {
            relid,
            metadata_location,
            previous_metadata_location,
            default_spec_id,
        })
    }
}
