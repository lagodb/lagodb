//! IcebergMetadata - CRUD operations for the iceberg.iceberg_metadata table.
//!
//! This module reads and writes the PostgreSQL catalog row that tracks the
//! current Iceberg metadata file location for each Iceberg-AM relation:
//!
//! ```sql
//! CREATE TABLE iceberg.iceberg_metadata (
//!     relid regclass NOT NULL,
//!     metadata_location text,
//!     previous_metadata_location text,
//!     default_spec_id integer,
//!     maintenance_due_at timestamptz,
//!     PRIMARY KEY (relid)
//! );
//! ```

use std::ffi::CStr;

use pg_lakebase_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, CatalogUpdateResult,
    get_namespace_oid, get_relation_oid,
};
use pg_lakebase_core::diag::PgError;
use pg_lakebase_core::handles::HeapTupleGuard;
use pgrx::prelude::TimestampWithTimeZone;
use pgrx::{FromDatum, IntoDatum, pg_sys};

use crate::error::{
    IcebergError, IcebergResult, MetadataCatalogOperation as CatalogOp,
};

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

const ICEBERG_SCHEMA: &CStr = c"iceberg";
const ICEBERG_METADATA_TABLE: &CStr = c"iceberg_metadata";
const ICEBERG_METADATA_PKEY: &CStr = c"iceberg_metadata_pkey";
const ICEBERG_METADATA_MAINTENANCE_DUE_IDX: &CStr =
    c"iceberg_metadata_maintenance_due_idx";

/// Column numbers in `iceberg.iceberg_metadata` (1-based, as required by
/// `heap_getattr`/`heap_modify_tuple`).
mod column {
    pub const RELID: i16 = 1;
    pub const METADATA_LOCATION: i16 = 2;
    pub const PREVIOUS_METADATA_LOCATION: i16 = 3;
    pub const DEFAULT_SPEC_ID: i16 = 4;
    pub const MAINTENANCE_DUE_AT: i16 = 5;
}

// ---------------------------------------------------------------------------
// Error-mapping ergonomics
// ---------------------------------------------------------------------------

/// Adapt a [`PgError`]-returning catalog call to [`IcebergResult`] by tagging
/// it with a [`CatalogOp`].
///
/// This is the single point where `PgError -> IcebergError::MetadataCatalog`
/// happens in this module, in line with the policy in `error.rs` ("keep that
/// inside meaningful Iceberg object methods"). Every catalog call uses
/// `.map_catalog_err(CatalogOp::*)?` instead of an inline closure.
trait CatalogResultExt<T> {
    fn map_catalog_err(self, op: CatalogOp) -> IcebergResult<T>;
}

impl<T> CatalogResultExt<T> for Result<T, PgError> {
    #[inline]
    fn map_catalog_err(self, op: CatalogOp) -> IcebergResult<T> {
        self.map_err(|source| IcebergError::metadata_catalog(op, source))
    }
}

// ---------------------------------------------------------------------------
// Catalog OID resolution
// ---------------------------------------------------------------------------

/// Resolve a relation OID under the Iceberg AM schema.
///
/// Deliberately avoid a backend-lifetime cache so a long-lived backend recovers
/// after `DROP EXTENSION pg_iceberg_am; CREATE EXTENSION pg_iceberg_am`.
fn iceberg_relation_oid(name: &CStr) -> IcebergResult<pg_sys::Oid> {
    let schema = get_namespace_oid(ICEBERG_SCHEMA, false)
        .map_catalog_err(CatalogOp::Access)?;
    get_relation_oid(name, schema).map_catalog_err(CatalogOp::Access)
}

// ---------------------------------------------------------------------------
// Tuple field accessors
// ---------------------------------------------------------------------------

/// Read column `attno` (1-based) as `Option<T>`. Returns `None` for SQL NULL.
///
/// # Safety
///
/// `tuple` must be a live heap tuple consistent with `tup_desc`; `attno` must
/// identify an attribute in that descriptor; and the attribute's PostgreSQL
/// type must be compatible with `T`'s [`FromDatum`] implementation.
unsafe fn get_attr<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tup_desc: pg_sys::TupleDesc,
    attno: i16,
) -> Option<T> {
    let mut is_null = false;
    // SAFETY: the caller guarantees that the tuple and descriptor match and
    // that `attno` identifies an attribute in the descriptor. `is_null` is a
    // valid out-parameter for the duration of the call.
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tup_desc, &mut is_null) };
    if is_null {
        None
    } else {
        // SAFETY: `heap_getattr` reported a non-NULL Datum, and the caller
        // guarantees that the attribute type is compatible with `T`.
        unsafe { T::from_datum(datum, false) }
    }
}

/// Like [`get_attr`] but returns an `IcebergError::MetadataCatalogInvalidRecord`
/// when the column is NULL or fails to decode.
///
/// # Safety
///
/// Same as [`get_attr`].
unsafe fn get_attr_required<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tup_desc: pg_sys::TupleDesc,
    attno: i16,
    field_name: &'static str,
) -> IcebergResult<T> {
    // SAFETY: this function has the same caller requirements as `get_attr` and
    // forwards the tuple, descriptor, attribute number, and target type unchanged.
    unsafe { get_attr::<T>(tuple, tup_desc, attno) }.ok_or_else(|| {
        IcebergError::MetadataCatalogInvalidRecord(format!(
            "{field_name} (attno {attno}) is null or undecodable"
        ))
    })
}

// ---------------------------------------------------------------------------
// Tuple field builders
// ---------------------------------------------------------------------------

/// Parallel `values`/`nulls` arrays sized for one tuple, with a column-by-
/// column setter API that hides the 1-based-to-0-based attno arithmetic.
///
/// Used directly for `heap_form_tuple` (insert) and as the storage backing
/// for [`TupleReplacement`] (update).
struct TupleFields {
    values: Vec<pg_sys::Datum>,
    nulls: Vec<bool>,
}

impl TupleFields {
    /// # Safety
    ///
    /// `tup_desc` must be a non-null, live `TupleDesc`.
    unsafe fn new(tup_desc: pg_sys::TupleDesc) -> Self {
        // SAFETY: the caller guarantees that `tup_desc` is non-null and live.
        let natts = unsafe { (*tup_desc).natts } as usize;
        // Default to NULL for every column. Callers explicitly `set` the
        // columns they intend to write; any column they forget about
        // becomes SQL NULL rather than a silent zero/empty Datum. For
        // NOT NULL columns this turns a missed `set` into a loud
        // `null value in column ...` error from PostgreSQL instead of
        // garbage data.
        Self {
            values: vec![pg_sys::Datum::from(0); natts],
            nulls: vec![true; natts],
        }
    }

    /// Set column `attno` (1-based). `None` writes SQL NULL.
    fn set<T: IntoDatum>(&mut self, attno: i16, value: Option<T>) {
        let idx = (attno - 1) as usize;
        let datum = value.into_datum();
        self.values[idx] = datum.unwrap_or(pg_sys::Datum::from(0));
        self.nulls[idx] = datum.is_none();
    }
}

/// Builder for `heap_modify_tuple`. Adds the `repls` array on top of
/// [`TupleFields`] so callers can express "replace these columns, leave the
/// rest alone". Only columns explicitly set via [`Self::set`] are replaced.
///
/// This is what lets the CAS update path leave columns like `default_spec_id`
/// untouched without re-reading them from the catalog first.
struct TupleReplacement {
    fields: TupleFields,
    repls: Vec<bool>,
}

impl TupleReplacement {
    /// # Safety
    ///
    /// `tup_desc` must be a valid TupleDesc for the relation that the
    /// replacement will be applied to.
    unsafe fn new(tup_desc: pg_sys::TupleDesc) -> Self {
        // SAFETY: the caller guarantees that `tup_desc` is non-null and live.
        let natts = unsafe { (*tup_desc).natts } as usize;
        Self {
            // SAFETY: this forwards the same live descriptor required by this
            // constructor to `TupleFields::new`.
            fields: unsafe { TupleFields::new(tup_desc) },
            repls: vec![false; natts],
        }
    }

    /// Replace column `attno` (1-based). `None` writes SQL NULL.
    fn set<T: IntoDatum>(&mut self, attno: i16, value: Option<T>) {
        self.fields.set(attno, value);
        self.repls[(attno - 1) as usize] = true;
    }

    /// Apply the replacement against `old_tuple`, producing an owned tuple
    /// guard.
    ///
    /// # Safety
    ///
    /// `tup_desc` and `old_tuple` must come from the same relation, and
    /// `tup_desc` must be the descriptor used to construct this replacement.
    unsafe fn apply(
        mut self,
        tup_desc: pg_sys::TupleDesc,
        old_tuple: pg_sys::HeapTuple,
    ) -> HeapTupleGuard {
        // SAFETY: the caller guarantees that `old_tuple` matches `tup_desc`.
        // Construction from that same descriptor makes all three replacement
        // arrays exactly `natts` elements long, and their mutable pointers stay
        // valid for the duration of `heap_modify_tuple`. PostgreSQL returns a
        // separately allocated tuple owned by `HeapTupleGuard`.
        unsafe {
            HeapTupleGuard::new(pg_sys::heap_modify_tuple(
                old_tuple,
                tup_desc,
                self.fields.values.as_mut_ptr(),
                self.fields.nulls.as_mut_ptr(),
                self.repls.as_mut_ptr(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// CasUpdate — replacement values for `IcebergMetadata::cas_update`
// ---------------------------------------------------------------------------

/// Replacement values for the next [`IcebergMetadata::cas_update`].
///
/// Every field listed here is written on a successful CAS. Columns that are
/// **not** present on this struct (e.g. `default_spec_id`) are preserved
/// as-is on the existing row.
///
/// Using a struct of named fields rather than positional `Option<&str>`
/// arguments removes a class of "I swapped the second and third argument"
/// bugs at the call site, where multiple parameters share both type
/// (`Option<&str>`) and likely value (the same `last_base`).
#[derive(Debug, Clone, Copy)]
pub struct CasUpdate<'a> {
    pub metadata_location: Option<&'a str>,
    pub previous_metadata_location: Option<&'a str>,
    pub maintenance_schedule: MaintenanceScheduleUpdate,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum MaintenanceScheduleUpdate {
    #[default]
    Preserve,
    ScheduleNoLaterThan(pg_sys::TimestampTz),
    CompleteIfDueMatches(Option<pg_sys::TimestampTz>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct MaintenanceCompletionToken {
    pub(crate) metadata_location: String,
    pub(crate) maintenance_due_at: Option<pg_sys::TimestampTz>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct MaintenanceCandidate {
    pub(crate) relid: pg_sys::Oid,
    pub(crate) metadata_location: Option<String>,
    pub(crate) due_at: pg_sys::TimestampTz,
}

impl MaintenanceCandidate {
    pub(crate) fn matches(&self, token: &MaintenanceCompletionToken) -> bool {
        self.metadata_location.as_deref() == Some(token.metadata_location.as_str())
            && Some(self.due_at) == token.maintenance_due_at
    }
}

// ---------------------------------------------------------------------------
// IcebergMetadata
// ---------------------------------------------------------------------------

/// One row of `iceberg.iceberg_metadata`.
#[derive(Debug, Clone, Default)]
pub struct IcebergMetadata {
    pub relid: pg_sys::Oid,
    pub metadata_location: Option<String>,
    pub previous_metadata_location: Option<String>,
    pub default_spec_id: Option<i32>,
    pub maintenance_due_at: Option<pg_sys::TimestampTz>,
}

impl IcebergMetadata {
    pub fn new(relid: pg_sys::Oid) -> Self {
        Self {
            relid,
            ..Self::default()
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

    // -- catalog OID accessors ------------------------------------------------

    fn table_oid() -> IcebergResult<pg_sys::Oid> {
        iceberg_relation_oid(ICEBERG_METADATA_TABLE)
    }

    fn pkey_oid() -> IcebergResult<pg_sys::Oid> {
        iceberg_relation_oid(ICEBERG_METADATA_PKEY)
    }

    fn maintenance_due_index_oid() -> IcebergResult<pg_sys::Oid> {
        iceberg_relation_oid(ICEBERG_METADATA_MAINTENANCE_DUE_IDX)
    }

    // -- CRUD: Insert ---------------------------------------------------------

    /// Insert this record. Errors if a row with the same `relid` already
    /// exists.
    ///
    /// Note: `IcebergMetadata::exists()` followed by `insert()` is technically
    /// TOCTOU. We accept that today because all current callers (DDL hooks
    /// for CREATE TABLE) hold a lock on the target relation OID, so the row
    /// cannot be inserted between our check and our insert.
    pub fn insert(&self) -> IcebergResult<()> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Insert)?;

        let tup_desc = table_guard.as_handle().tuple_desc();

        // SAFETY: `tup_desc` belongs to the open relation and is valid for
        // the lifetime of `table_guard`.
        let mut fields = unsafe { TupleFields::new(tup_desc) };
        // `relid` is the primary key; PG cannot store NULL here. Wrapping it
        // in `Some` makes the call site uniform with the nullable columns.
        fields.set(column::RELID, Some(self.relid));
        fields.set(column::METADATA_LOCATION, self.metadata_location.as_deref());
        fields.set(
            column::PREVIOUS_METADATA_LOCATION,
            self.previous_metadata_location.as_deref(),
        );
        fields.set(column::DEFAULT_SPEC_ID, self.default_spec_id);
        fields.set(column::MAINTENANCE_DUE_AT, self.maintenance_due_at);

        // SAFETY: `heap_form_tuple` returns an owned heap tuple freed by
        // `HeapTupleGuard` via `heap_freetuple`.
        let tuple_guard = unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tup_desc,
                fields.values.as_mut_ptr(),
                fields.nulls.as_mut_ptr(),
            ))
        };

        table_guard
            .catalog_insert(&tuple_guard)
            .map_catalog_err(CatalogOp::Insert)
    }

    // -- CRUD: Read -----------------------------------------------------------

    /// Look up by relid. Returns `None` if the row does not exist.
    pub fn find_by_relid(relid: pg_sys::Oid) -> IcebergResult<Option<Self>> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::AccessShareLock as _)
                .map_catalog_err(CatalogOp::Read)?;

        let mut scan = table_guard
            .begin_scan(
                Self::pkey_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_catalog_err(CatalogOp::Read)?;

        let Some(tuple) = scan.get_next().map_catalog_err(CatalogOp::Read)? else {
            return Ok(None);
        };

        // SAFETY: `table_guard` is alive (so the TupleDesc is valid) and
        // `tuple` is a live scan result.
        let row = unsafe {
            Self::from_tuple(table_guard.as_handle().tuple_desc(), tuple.as_raw())?
        };
        Ok(Some(row))
    }

    /// Look up by relid; returns `MetadataCatalogNotFound` when absent.
    pub fn get(relid: pg_sys::Oid) -> IcebergResult<Self> {
        Self::find_by_relid(relid)?
            .ok_or(IcebergError::MetadataCatalogNotFound(relid))
    }

    /// Existence probe.
    pub fn exists(relid: pg_sys::Oid) -> IcebergResult<bool> {
        Ok(Self::find_by_relid(relid)?.is_some())
    }

    pub(crate) fn maintenance_candidates(
        limit: usize,
        now: pg_sys::TimestampTz,
    ) -> IcebergResult<Vec<MaintenanceCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::AccessShareLock as _)
                .map_catalog_err(CatalogOp::Read)?;
        let mut scan = table_guard
            .begin_ordered_scan(
                Self::maintenance_due_index_oid()?,
                CatalogSnapshot::Default,
                [CatalogScanKey::timestamptz_le(
                    column::MAINTENANCE_DUE_AT as _,
                    now,
                )],
            )
            .map_catalog_err(CatalogOp::Read)?;
        let tuple_desc = table_guard.as_handle().tuple_desc();
        let mut candidates = Vec::with_capacity(limit);
        while candidates.len() < limit {
            let Some(tuple) = scan.get_next().map_catalog_err(CatalogOp::Read)?
            else {
                break;
            };
            // SAFETY: `tuple` is a live result from the scan over `table_guard`;
            // `tuple_desc` belongs to that relation, and RELID is an OID column.
            let relid = unsafe {
                get_attr_required(tuple.as_raw(), tuple_desc, column::RELID, "relid")?
            };
            // SAFETY: the tuple and descriptor match, and METADATA_LOCATION is
            // a text column decoded to an owned String.
            let metadata_location = unsafe {
                get_attr(tuple.as_raw(), tuple_desc, column::METADATA_LOCATION)
            };
            // SAFETY: the tuple and descriptor match, and MAINTENANCE_DUE_AT is
            // a timestamptz column.
            let due_at = unsafe {
                get_attr_required::<TimestampWithTimeZone>(
                    tuple.as_raw(),
                    tuple_desc,
                    column::MAINTENANCE_DUE_AT,
                    "maintenance_due_at",
                )?
            }
            .into_inner();
            candidates.push(MaintenanceCandidate {
                relid,
                metadata_location,
                due_at,
            });
        }
        Ok(candidates)
    }

    pub(crate) fn next_maintenance_due_at()
    -> IcebergResult<Option<pg_sys::TimestampTz>> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::AccessShareLock as _)
                .map_catalog_err(CatalogOp::Read)?;
        let mut scan = table_guard
            .begin_ordered_scan(
                Self::maintenance_due_index_oid()?,
                CatalogSnapshot::Default,
                std::iter::empty(),
            )
            .map_catalog_err(CatalogOp::Read)?;
        let Some(tuple) = scan.get_next().map_catalog_err(CatalogOp::Read)? else {
            return Ok(None);
        };
        // SAFETY: `tuple` comes from this open relation's scan, its descriptor
        // remains live through `table_guard`, and the column is timestamptz.
        Ok(unsafe {
            get_attr::<TimestampWithTimeZone>(
                tuple.as_raw(),
                table_guard.as_handle().tuple_desc(),
                column::MAINTENANCE_DUE_AT,
            )
            .map(TimestampWithTimeZone::into_inner)
        })
    }

    pub(crate) fn defer_maintenance(
        candidate: &MaintenanceCandidate,
        retry_at: pg_sys::TimestampTz,
    ) -> IcebergResult<bool> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Update)?;
        let mut scan = table_guard
            .begin_scan(
                Self::pkey_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, candidate.relid)],
            )
            .map_catalog_err(CatalogOp::Update)?;
        let Some(old_tuple) = scan.get_next().map_catalog_err(CatalogOp::Update)?
        else {
            return Ok(false);
        };
        let tuple_desc = table_guard.as_handle().tuple_desc();
        // SAFETY: `old_tuple` comes from this open relation's scan, the
        // descriptor matches it, and METADATA_LOCATION is a text column.
        let current_location: Option<String> = unsafe {
            get_attr(old_tuple.as_raw(), tuple_desc, column::METADATA_LOCATION)
        };
        // SAFETY: the tuple and descriptor match, and MAINTENANCE_DUE_AT is a
        // timestamptz column.
        let current_due = unsafe {
            get_attr::<TimestampWithTimeZone>(
                old_tuple.as_raw(),
                tuple_desc,
                column::MAINTENANCE_DUE_AT,
            )
            .map(TimestampWithTimeZone::into_inner)
        };
        if current_location.as_deref() != candidate.metadata_location.as_deref()
            || current_due != Some(candidate.due_at)
        {
            return Ok(false);
        }
        // SAFETY: `tuple_desc` belongs to the live open relation.
        let mut replacement = unsafe { TupleReplacement::new(tuple_desc) };
        replacement.set(column::MAINTENANCE_DUE_AT, Some(retry_at));
        // SAFETY: the replacement and `old_tuple` use the same live descriptor.
        let new_tuple = unsafe { replacement.apply(tuple_desc, old_tuple.as_raw()) };
        Ok(matches!(
            table_guard
                .catalog_update_optimistic(old_tuple, &new_tuple)
                .map_catalog_err(CatalogOp::Update)?,
            CatalogUpdateResult::Success
        ))
    }

    pub(crate) fn finish_maintenance(
        relid: pg_sys::Oid,
        expected_location: &str,
        schedule: MaintenanceScheduleUpdate,
    ) -> IcebergResult<()> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Update)?;
        let mut scan = table_guard
            .begin_scan(
                Self::pkey_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_catalog_err(CatalogOp::Update)?;
        let Some(old_tuple) = scan.get_next().map_catalog_err(CatalogOp::Update)?
        else {
            return Err(IcebergError::MetadataCatalogNotFound(relid));
        };
        let tuple_desc = table_guard.as_handle().tuple_desc();
        // SAFETY: `old_tuple` comes from this open relation's scan, the
        // descriptor matches it, and METADATA_LOCATION is a text column.
        let current_location: Option<String> = unsafe {
            get_attr(old_tuple.as_raw(), tuple_desc, column::METADATA_LOCATION)
        };
        if current_location.as_deref() != Some(expected_location) {
            return Err(IcebergError::MetadataCatalogConflict);
        }
        // SAFETY: `tuple_desc` belongs to the live open relation.
        let mut replacement = unsafe { TupleReplacement::new(tuple_desc) };
        match schedule {
            MaintenanceScheduleUpdate::CompleteIfDueMatches(expected_due) => {
                // SAFETY: `old_tuple` and `tuple_desc` match, and
                // MAINTENANCE_DUE_AT is a timestamptz column.
                let current_due = unsafe {
                    get_attr::<TimestampWithTimeZone>(
                        old_tuple.as_raw(),
                        tuple_desc,
                        column::MAINTENANCE_DUE_AT,
                    )
                    .map(TimestampWithTimeZone::into_inner)
                };
                // A newer due value represents work this maintenance plan did
                // not authorize itself to complete.
                if current_due == expected_due {
                    replacement
                        .set::<pg_sys::TimestampTz>(column::MAINTENANCE_DUE_AT, None);
                }
            }
            MaintenanceScheduleUpdate::Preserve => {}
            MaintenanceScheduleUpdate::ScheduleNoLaterThan(_) => {
                return Err(IcebergError::InvariantViolated(
                    "maintenance completion cannot schedule new maintenance work",
                ));
            }
        }
        // SAFETY: the replacement and `old_tuple` use the same live descriptor.
        let new_tuple = unsafe { replacement.apply(tuple_desc, old_tuple.as_raw()) };
        match table_guard
            .catalog_update_optimistic(old_tuple, &new_tuple)
            .map_catalog_err(CatalogOp::Update)?
        {
            CatalogUpdateResult::Success => Ok(()),
            CatalogUpdateResult::Conflict => {
                Err(IcebergError::MetadataCatalogConflict)
            }
        }
    }

    // -- CRUD: CAS update -----------------------------------------------------

    /// Compare-and-swap update for the row identified by `relid`.
    ///
    /// Reads the row, verifies its `metadata_location` matches
    /// `expected_previous_location`, then writes the columns named in
    /// `new`. Columns absent from [`CasUpdate`] (e.g. `default_spec_id`)
    /// are preserved as-is, which is what lets us avoid an extra row
    /// fetch for columns we never modify on this path.
    ///
    /// Returns [`IcebergError::MetadataCatalogConflict`] for both the
    /// logical CAS mismatch and PostgreSQL's tuple-version conflict
    /// (`heap_update` returning `TM_Updated`/`TM_Deleted`). The caller's
    /// retry loop handles both as "rebase and try again".
    pub fn cas_update(
        relid: pg_sys::Oid,
        expected_previous_location: Option<&str>,
        new: CasUpdate<'_>,
    ) -> IcebergResult<bool> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Update)?;

        let mut scan = table_guard
            .begin_scan(
                Self::pkey_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_catalog_err(CatalogOp::Update)?;

        let Some(old_tuple) = scan.get_next().map_catalog_err(CatalogOp::Update)?
        else {
            return Err(IcebergError::MetadataCatalogNotFound(relid));
        };

        let tup_desc = table_guard.as_handle().tuple_desc();

        // Logical CAS check against the metadata_location column only. The
        // physical-tuple-version check below in `catalog_update_optimistic`
        // covers the race window between this read and the write.
        // SAFETY: `tup_desc` and `old_tuple` come from the same open relation.
        let current_location: Option<String> = unsafe {
            get_attr::<String>(
                old_tuple.as_raw(),
                tup_desc,
                column::METADATA_LOCATION,
            )
        };
        if current_location.as_deref() != expected_previous_location {
            return Err(IcebergError::MetadataCatalogConflict);
        }

        // SAFETY: `tup_desc` belongs to the live open relation.
        let mut replacement = unsafe { TupleReplacement::new(tup_desc) };
        replacement.set(column::METADATA_LOCATION, new.metadata_location);
        replacement.set(
            column::PREVIOUS_METADATA_LOCATION,
            new.previous_metadata_location,
        );
        // SAFETY: `old_tuple` and `tup_desc` come from the same open relation,
        // and MAINTENANCE_DUE_AT is a timestamptz column.
        let current_due = unsafe {
            get_attr::<TimestampWithTimeZone>(
                old_tuple.as_raw(),
                tup_desc,
                column::MAINTENANCE_DUE_AT,
            )
            .map(TimestampWithTimeZone::into_inner)
        };
        let maintenance_deadline_advanced = match new.maintenance_schedule {
            MaintenanceScheduleUpdate::Preserve => false,
            MaintenanceScheduleUpdate::ScheduleNoLaterThan(proposed)
                if current_due.is_none_or(|current| proposed < current) =>
            {
                replacement.set(column::MAINTENANCE_DUE_AT, Some(proposed));
                true
            }
            MaintenanceScheduleUpdate::ScheduleNoLaterThan(_) => false,
            MaintenanceScheduleUpdate::CompleteIfDueMatches(expected_due)
                if current_due == expected_due && current_due.is_some() =>
            {
                replacement
                    .set::<pg_sys::TimestampTz>(column::MAINTENANCE_DUE_AT, None);
                false
            }
            // Preserve a newer scheduling token while still publishing the
            // metadata columns through the existing optimistic tuple update.
            MaintenanceScheduleUpdate::CompleteIfDueMatches(_) => false,
        };

        // SAFETY: the replacement was built from this relation's TupleDesc
        // and `old_tuple` is from the matching scan.
        let new_tuple_guard =
            unsafe { replacement.apply(tup_desc, old_tuple.as_raw()) };

        // Funnel the PgError branch through `map_catalog_err` to keep the
        // "single point of error mapping" contract in this module.
        match table_guard
            .catalog_update_optimistic(old_tuple, &new_tuple_guard)
            .map_catalog_err(CatalogOp::Update)?
        {
            CatalogUpdateResult::Success => Ok(maintenance_deadline_advanced),
            CatalogUpdateResult::Conflict => {
                Err(IcebergError::MetadataCatalogConflict)
            }
        }
    }

    // -- CRUD: Delete ---------------------------------------------------------

    /// Delete the row for `relid`. Errors if the row is missing.
    pub fn delete(relid: pg_sys::Oid) -> IcebergResult<()> {
        Self::delete_inner(relid, /* missing_ok */ false)
    }

    /// Delete the row for `relid` if present; success when absent.
    pub fn delete_if_exists(relid: pg_sys::Oid) -> IcebergResult<()> {
        Self::delete_inner(relid, /* missing_ok */ true)
    }

    /// Internal helper for the two delete variants. The boolean is a
    /// deliberate internal detail; the public surface above uses semantic
    /// names.
    fn delete_inner(relid: pg_sys::Oid, missing_ok: bool) -> IcebergResult<()> {
        let table_guard =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Delete)?;

        let mut scan = table_guard
            .begin_scan(
                Self::pkey_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_catalog_err(CatalogOp::Delete)?;

        let tuple = scan.get_next().map_catalog_err(CatalogOp::Delete)?;

        let tuple = match tuple {
            Some(t) => t,
            None if missing_ok => return Ok(()),
            None => return Err(IcebergError::MetadataCatalogNotFound(relid)),
        };

        table_guard
            .catalog_delete(tuple)
            .map_catalog_err(CatalogOp::Delete)
    }

    // -- Tuple decoding -------------------------------------------------------

    /// Decode a tuple from `iceberg.iceberg_metadata` into [`Self`].
    ///
    /// # Safety
    ///
    /// `tup_desc` must be the relation's tuple descriptor and `tuple` a live
    /// heap tuple from a scan over the same relation.
    unsafe fn from_tuple(
        tup_desc: pg_sys::TupleDesc,
        tuple: pg_sys::HeapTuple,
    ) -> IcebergResult<Self> {
        // SAFETY: the caller guarantees that the tuple and descriptor match.
        // Each attribute number names the documented catalog column, and each
        // target Rust type matches that column's PostgreSQL type.
        unsafe {
            Ok(Self {
                relid: get_attr_required(tuple, tup_desc, column::RELID, "relid")?,
                metadata_location: get_attr(
                    tuple,
                    tup_desc,
                    column::METADATA_LOCATION,
                ),
                previous_metadata_location: get_attr(
                    tuple,
                    tup_desc,
                    column::PREVIOUS_METADATA_LOCATION,
                ),
                default_spec_id: get_attr(tuple, tup_desc, column::DEFAULT_SPEC_ID),
                maintenance_due_at: get_attr::<TimestampWithTimeZone>(
                    tuple,
                    tup_desc,
                    column::MAINTENANCE_DUE_AT,
                )
                .map(TimestampWithTimeZone::into_inner),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_candidate_matches_location_and_due_token() {
        let candidate = MaintenanceCandidate {
            relid: pg_sys::Oid::from(42_u32),
            metadata_location: Some("metadata/v1.json".to_owned()),
            due_at: 100,
        };

        assert!(candidate.matches(&MaintenanceCompletionToken {
            metadata_location: "metadata/v1.json".to_owned(),
            maintenance_due_at: Some(100),
        }));
        assert!(!candidate.matches(&MaintenanceCompletionToken {
            metadata_location: "metadata/v2.json".to_owned(),
            maintenance_due_at: Some(100),
        }));
        assert!(!candidate.matches(&MaintenanceCompletionToken {
            metadata_location: "metadata/v1.json".to_owned(),
            maintenance_due_at: Some(101),
        }));
    }
}
