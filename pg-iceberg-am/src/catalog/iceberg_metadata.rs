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

use crate::error::{
    IcebergError, IcebergResult, MetadataCatalogOperation as CatalogOp,
};
use pg_lakebase_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, CatalogUpdateResult,
    LAKEBASE_SCHEMA, get_namespace_oid, get_relation_oid,
};
use pg_lakebase_core::handles::HeapTupleGuard;
use pgrx::{FromDatum, IntoDatum, pg_sys};
use std::ffi::CStr;
use std::sync::OnceLock;

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
pub fn get_iceberg_metadata_oid() -> IcebergResult<pg_sys::Oid> {
    if let Some(&oid) = ICEBERG_METADATA_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_namespace_oid(LAKEBASE_SCHEMA, false).map_err(|source| {
        IcebergError::metadata_catalog(CatalogOp::Access, source)
    })?;
    let oid =
        get_relation_oid(ICEBERG_METADATA_TABLE, schema_oid).map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Access, source)
        })?;

    let _ = ICEBERG_METADATA_OID.set(oid);

    Ok(oid)
}

/// Get the OID of the primary key index on `lakebase.iceberg_metadata`.
pub fn get_iceberg_metadata_pkey_oid() -> IcebergResult<pg_sys::Oid> {
    if let Some(&oid) = ICEBERG_METADATA_PKEY_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_namespace_oid(LAKEBASE_SCHEMA, false).map_err(|source| {
        IcebergError::metadata_catalog(CatalogOp::Access, source)
    })?;
    let oid =
        get_relation_oid(ICEBERG_METADATA_PKEY, schema_oid).map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Access, source)
        })?;

    let _ = ICEBERG_METADATA_PKEY_OID.set(oid);

    Ok(oid)
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
    pub fn insert(&self) -> IcebergResult<()> {
        let table_oid = get_iceberg_metadata_oid()?;

        let table_guard =
            CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _).map_err(
                |source| IcebergError::metadata_catalog(CatalogOp::Insert, source),
            )?;

        let relid_datum = self.relid.into_datum().unwrap();
        let metadata_location_datum = self.metadata_location.as_deref().into_datum();
        let previous_metadata_location_datum =
            self.previous_metadata_location.as_deref().into_datum();
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

        // SAFETY: `table_guard.tuple_desc()` returns the relation's valid
        // TupleDesc. `heap_form_tuple` returns an owned heap tuple that
        // `HeapTupleGuard` will free via `heap_freetuple`.
        let tuple_guard = unsafe {
            let tuple = pg_sys::heap_form_tuple(
                table_guard.tuple_desc(),
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
            HeapTupleGuard::new(tuple)
        };

        table_guard.catalog_insert(&tuple_guard).map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Insert, source)
        })?;

        Ok(())
    }

    /// Find a record by relid from the `lakebase.iceberg_metadata` table.
    ///
    /// Returns `Ok(None)` if no record is found.
    pub fn find_by_relid(relid: pg_sys::Oid) -> IcebergResult<Option<Self>> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        let table_guard =
            CatalogRelation::open(table_oid, pg_sys::AccessShareLock as _).map_err(
                |source| IcebergError::metadata_catalog(CatalogOp::Read, source),
            )?;

        let mut scan_guard = table_guard
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_err(|source| {
                IcebergError::metadata_catalog(CatalogOp::Read, source)
            })?;

        let tuple = scan_guard.get_next().map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Read, source)
        })?;

        match tuple {
            Some(tuple) => {
                // SAFETY: `table_guard.as_raw()` is a valid open relation and
                // `tuple.as_raw()` points to a live scan result tuple.
                let row = unsafe {
                    Self::from_tuple(table_guard.as_raw(), tuple.as_raw())?
                };
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    pub fn get(relid: pg_sys::Oid) -> IcebergResult<Self> {
        Self::find_by_relid(relid)?
            .ok_or(IcebergError::MetadataCatalogNotFound(relid))
    }

    pub fn exists(relid: pg_sys::Oid) -> IcebergResult<bool> {
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
    ) -> IcebergResult<()> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        let table_guard =
            CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _).map_err(
                |source| IcebergError::metadata_catalog(CatalogOp::Update, source),
            )?;

        let mut scan_guard = table_guard
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, self.relid)],
            )
            .map_err(|source| {
                IcebergError::metadata_catalog(CatalogOp::Update, source)
            })?;

        let old_tuple = scan_guard.get_next().map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Update, source)
        })?;

        let old_tuple = match old_tuple {
            Some(t) => t,
            None => return Err(IcebergError::MetadataCatalogNotFound(self.relid)),
        };

        // Optimistic Lock Check (CAS)
        // SAFETY: relation and tuple are valid while the guards are alive.
        let existing_metadata =
            unsafe { Self::from_tuple(table_guard.as_raw(), old_tuple.as_raw())? };
        if existing_metadata.metadata_location.as_deref()
            != expected_previous_location
        {
            return Err(IcebergError::MetadataCatalogConflict);
        }

        // Prepare replacement column values
        let tup_desc = table_guard.tuple_desc();
        let natts = unsafe { (*tup_desc).natts } as usize;

        let mut values = vec![pg_sys::Datum::from(0); natts];
        let mut nulls = vec![false; natts];
        let mut repls = vec![false; natts];

        let metadata_location_datum = self.metadata_location.as_deref().into_datum();
        values[(column::METADATA_LOCATION - 1) as usize] =
            metadata_location_datum.unwrap_or(pg_sys::Datum::from(0));
        nulls[(column::METADATA_LOCATION - 1) as usize] =
            metadata_location_datum.is_none();
        repls[(column::METADATA_LOCATION - 1) as usize] = true;

        let previous_metadata_location_datum =
            self.previous_metadata_location.as_deref().into_datum();
        values[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] =
            previous_metadata_location_datum.unwrap_or(pg_sys::Datum::from(0));
        nulls[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] =
            previous_metadata_location_datum.is_none();
        repls[(column::PREVIOUS_METADATA_LOCATION - 1) as usize] = true;

        let default_spec_id_datum = self.default_spec_id.into_datum();
        values[(column::DEFAULT_SPEC_ID - 1) as usize] =
            default_spec_id_datum.unwrap_or(pg_sys::Datum::from(0));
        nulls[(column::DEFAULT_SPEC_ID - 1) as usize] =
            default_spec_id_datum.is_none();
        repls[(column::DEFAULT_SPEC_ID - 1) as usize] = true;

        // SAFETY: `heap_modify_tuple` returns an owned heap tuple freed by
        // `HeapTupleGuard` via `heap_freetuple`.
        let new_tuple_guard = unsafe {
            let new_tuple = pg_sys::heap_modify_tuple(
                old_tuple.as_raw(),
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                repls.as_mut_ptr(),
            );
            HeapTupleGuard::new(new_tuple)
        };

        // Handle physical-level concurrent modification (PostgreSQL tuple version conflict)
        // This complements the logical CAS check above - even if metadata_location matched
        // our expectation, another transaction might have committed between our read and write.
        // In this case, heap_update returns TM_Updated/TM_Deleted, and we return Conflict
        // to trigger the retry/rebase logic in metadata_tracking.rs.
        match table_guard.catalog_update_optimistic(old_tuple, &new_tuple_guard) {
            Ok(CatalogUpdateResult::Success) => Ok(()),
            Ok(CatalogUpdateResult::Conflict) => {
                Err(IcebergError::MetadataCatalogConflict)
            }
            Err(source) => {
                Err(IcebergError::metadata_catalog(CatalogOp::Update, source))
            }
        }
    }

    /// Delete the record for the given relid from the `lakebase.iceberg_metadata` table.
    ///
    /// Returns an error if the record does not exist.
    pub fn delete(relid: pg_sys::Oid) -> IcebergResult<()> {
        let table_oid = get_iceberg_metadata_oid()?;
        let index_oid = get_iceberg_metadata_pkey_oid()?;

        let table_guard =
            CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _).map_err(
                |source| IcebergError::metadata_catalog(CatalogOp::Delete, source),
            )?;

        let mut scan_guard = table_guard
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_err(|source| {
                IcebergError::metadata_catalog(CatalogOp::Delete, source)
            })?;

        let tuple = scan_guard.get_next().map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Delete, source)
        })?;

        let tuple = match tuple {
            Some(t) => t,
            None => return Err(IcebergError::MetadataCatalogNotFound(relid)),
        };

        table_guard.catalog_delete(tuple).map_err(|source| {
            IcebergError::metadata_catalog(CatalogOp::Delete, source)
        })?;

        Ok(())
    }

    // ========================================================================
    //  Private helper methods
    // ========================================================================

    /// Extract an IcebergMetadata from a HeapTuple.
    unsafe fn from_tuple(
        rel: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
    ) -> IcebergResult<Self> {
        let tup_desc = unsafe { (*rel).rd_att };

        let mut is_null = false;
        let relid_datum = unsafe {
            pg_sys::heap_getattr(tuple, column::RELID as _, tup_desc, &mut is_null)
        };
        let relid = if is_null {
            return Err(IcebergError::MetadataCatalogInvalidRecord(
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
