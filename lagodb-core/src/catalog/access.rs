//! Catalog relation guards, scan keys, and snapshot selection.
//!
//! Tuple writes live in `write` and system-table scans in `scan`; both extend
//! [`CatalogRelation`] with the PostgreSQL calls they own.

use std::ptr::{addr_of_mut, null_mut};

use pgrx::datum::Uuid;
use pgrx::{IntoDatum, PgTryBuilder, pg_sys};

use crate::diag::PgError;
use crate::handles::{HeapTupleRef, RelationGuard, RelationHandle, SnapshotHandle};

/// Snapshot selection for PostgreSQL system catalog scans.
///
/// `Default` asks PostgreSQL to use and register the catalog snapshot for the
/// scanned relation. `SelfVisible` exposes catalog changes made by the current
/// command, as required by PostgreSQL object-access hooks. `Borrowed` passes an
/// existing live snapshot without taking ownership of it.
#[derive(Debug, Clone, Copy)]
pub enum CatalogSnapshot<'a> {
    Default,
    /// See catalog rows inserted or updated by the current command.
    SelfVisible,
    Borrowed(&'a SnapshotHandle<'a>),
}

impl CatalogSnapshot<'_> {
    #[inline]
    pub(super) fn as_raw(&self) -> pg_sys::Snapshot {
        match self {
            CatalogSnapshot::Default => null_mut(),
            CatalogSnapshot::SelfVisible => {
                // PostgreSQL owns this backend-local static snapshot for the
                // process lifetime; catalog scans only borrow it.
                addr_of_mut!(pg_sys::SnapshotSelfData)
            }
            CatalogSnapshot::Borrowed(snapshot) => snapshot.as_raw(),
        }
    }
}

/// Owned scan key data for catalog scans.
#[derive(Debug)]
pub struct CatalogScanKey {
    data: pg_sys::ScanKeyData,
}

impl CatalogScanKey {
    /// # Safety
    ///
    /// `procedure` must identify a valid PostgreSQL comparison function for
    /// the supplied scan key argument and strategy.
    pub unsafe fn new(
        attribute_number: pg_sys::AttrNumber,
        strategy: u16,
        procedure: pg_sys::RegProcedure,
        argument: pg_sys::Datum,
    ) -> Self {
        let mut data = pg_sys::ScanKeyData::default();
        // SAFETY: the caller guarantees `procedure` is a valid comparison
        // function. ScanKeyInit sets every field, with `C_COLLATION_OID`.
        unsafe {
            pg_sys::ScanKeyInit(
                &mut data,
                attribute_number,
                strategy,
                procedure,
                argument,
            );
        }
        Self { data }
    }

    /// # Safety
    ///
    /// `procedure` must identify a valid PostgreSQL comparison function for
    /// the supplied scan key argument, strategy, and collation.
    unsafe fn new_with_collation(
        attribute_number: pg_sys::AttrNumber,
        strategy: u16,
        procedure: pg_sys::RegProcedure,
        collation: pg_sys::Oid,
        argument: pg_sys::Datum,
    ) -> Self {
        let mut data = pg_sys::ScanKeyData::default();
        // SAFETY: the caller guarantees `procedure` is a valid comparison
        // function for `collation`.
        unsafe {
            pg_sys::ScanKeyEntryInitialize(
                &mut data,
                0,
                attribute_number,
                strategy,
                pg_sys::InvalidOid,
                collation,
                procedure,
                argument,
            );
        }
        Self { data }
    }

    pub fn oid_eq(attribute_number: pg_sys::AttrNumber, oid: pg_sys::Oid) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_OIDEQ),
                oid.into_datum().expect("Oid should convert to Datum"),
            )
        }
    }

    pub fn uuid_eq(attribute_number: pg_sys::AttrNumber, value: Uuid) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_UUID_EQ),
                value.into_datum().expect("UUID should convert to Datum"),
            )
        }
    }

    pub fn i16_eq(attribute_number: pg_sys::AttrNumber, value: i16) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_INT2EQ),
                value.into_datum().expect("i16 should convert to Datum"),
            )
        }
    }

    pub fn i32_eq(attribute_number: pg_sys::AttrNumber, value: i32) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_INT4EQ),
                value.into_datum().expect("i32 should convert to Datum"),
            )
        }
    }

    pub fn i64_eq(attribute_number: pg_sys::AttrNumber, value: i64) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_INT8EQ),
                value.into_datum().expect("i64 should convert to Datum"),
            )
        }
    }

    pub fn bool_eq(attribute_number: pg_sys::AttrNumber, value: bool) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_BOOLEQ),
                value.into_datum().expect("bool should convert to Datum"),
            )
        }
    }

    pub fn text_eq(attribute_number: pg_sys::AttrNumber, value: &str) -> Self {
        Self::text_eq_with_collation(
            attribute_number,
            value,
            pg_sys::DEFAULT_COLLATION_OID,
        )
    }

    pub fn text_eq_with_collation(
        attribute_number: pg_sys::AttrNumber,
        value: &str,
        collation: pg_sys::Oid,
    ) -> Self {
        unsafe {
            Self::new_with_collation(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_TEXTEQ),
                collation,
                value.into_datum().expect("str should convert to Datum"),
            )
        }
    }

    pub fn timestamptz_le(
        attribute_number: pg_sys::AttrNumber,
        value: pg_sys::TimestampTz,
    ) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTLessEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_TIMESTAMPTZ_LE),
                pg_sys::Datum::from(value),
            )
        }
    }

    #[inline]
    pub(super) fn into_data(self) -> pg_sys::ScanKeyData {
        self.data
    }
}

/// RAII guard for an opened PostgreSQL catalog relation.
#[derive(Debug)]
pub struct CatalogRelation {
    guard: RelationGuard,
}

impl CatalogRelation {
    /// Make catalog writes visible to subsequent work in this command.
    pub fn make_changes_visible() -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(|| {
                pg_sys::CommandCounterIncrement();
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub fn open(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            guard: RelationGuard::open_table(oid, lock_mode)?,
        })
    }

    /// Opens a catalog relation and retains its lock until transaction end.
    pub fn open_retain_lock(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            guard: RelationGuard::open_table_retain_lock(oid, lock_mode)?,
        })
    }

    #[inline]
    pub fn as_handle(&self) -> RelationHandle<'_> {
        self.guard.as_handle()
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.guard.as_raw()
    }

    /// Read one user attribute from a tuple produced by a scan of this
    /// catalog relation.
    ///
    /// The returned datum borrows storage owned by `tuple`; callers must copy
    /// pass-by-reference values before advancing the scan.
    ///
    /// # Safety
    ///
    /// `tuple` must come from a scan of this relation, remain valid for this
    /// call, and `attribute_number` must identify a valid user attribute in
    /// this relation's tuple descriptor.
    #[inline]
    pub unsafe fn get_attr(
        &self,
        tuple: HeapTupleRef<'_>,
        attribute_number: pg_sys::AttrNumber,
    ) -> Option<pg_sys::Datum> {
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::heap_getattr(
                tuple.as_raw(),
                attribute_number as _,
                (*self.as_raw()).rd_att,
                &mut is_null,
            )
        };
        (!is_null).then_some(datum)
    }
}
