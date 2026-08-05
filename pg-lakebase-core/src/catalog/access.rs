use crate::diag::PgError;
use crate::handles::{
    HeapTupleGuard, HeapTupleRef, RelationGuard, RelationHandle, SnapshotHandle,
};
use crate::wrapper::PgWrapper;
use pgrx::datum::Uuid;
use pgrx::{IntoDatum, pg_sys};

/// Result of a catalog tuple update operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogUpdateResult {
    /// The update succeeded.
    Success,
    /// The update failed because PostgreSQL reported a tuple version conflict.
    Conflict,
}

/// Snapshot selection for PostgreSQL system catalog scans.
///
/// `Default` asks PostgreSQL to use and register the catalog snapshot for the
/// scanned relation. `Borrowed` passes an existing live snapshot without taking
/// ownership of it.
#[derive(Debug, Clone, Copy)]
pub enum CatalogSnapshot<'a> {
    Default,
    Borrowed(&'a SnapshotHandle<'a>),
}

impl CatalogSnapshot<'_> {
    #[inline]
    fn as_raw(&self) -> pg_sys::Snapshot {
        match self {
            CatalogSnapshot::Default => std::ptr::null_mut(),
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
        let mut data = unsafe { std::mem::zeroed() };
        unsafe {
            PgWrapper::scan_key_init(
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
        let mut data = unsafe { std::mem::zeroed() };
        unsafe {
            PgWrapper::scan_key_init_with_collation(
                &mut data,
                attribute_number,
                strategy,
                procedure,
                collation,
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

    pub fn name_eq_text(attribute_number: pg_sys::AttrNumber, value: &str) -> Self {
        unsafe {
            Self::new(
                attribute_number,
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_NAMEEQTEXT),
                value.into_datum().expect("str should convert to Datum"),
            )
        }
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
    fn into_data(self) -> pg_sys::ScanKeyData {
        self.data
    }
}

/// RAII guard for an opened PostgreSQL catalog relation.
#[derive(Debug)]
pub struct CatalogRelation {
    guard: RelationGuard,
}

impl CatalogRelation {
    pub fn open(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            guard: RelationGuard::open(oid, lock_mode)?,
        })
    }

    /// Opens a catalog relation and retains its lock until transaction end.
    pub fn open_retain_lock(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            guard: RelationGuard::open_retain_lock(oid, lock_mode)?,
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

    /// Insert an owned heap tuple into this catalog relation.
    pub fn catalog_insert(&self, tuple: &HeapTupleGuard) -> Result<(), PgError> {
        unsafe { PgWrapper::catalog_tuple_insert_raw(self.as_raw(), tuple.as_raw()) }
    }

    /// Open catalog indexes once for a bounded sequence of inserts.
    pub fn writer(&self) -> Result<CatalogWriter<'_>, PgError> {
        let index_state =
            unsafe { PgWrapper::catalog_open_indexes_raw(self.as_raw())? };
        Ok(CatalogWriter {
            relation: self,
            index_state,
        })
    }

    /// Update a catalog tuple and keep PostgreSQL catalog indexes current.
    pub fn catalog_update(
        &self,
        old_tuple: HeapTupleRef<'_>,
        new_tuple: &HeapTupleGuard,
    ) -> Result<(), PgError> {
        let mut old_tid = old_tuple.item_pointer_data();
        unsafe {
            PgWrapper::catalog_tuple_update_raw(
                self.as_raw(),
                &mut old_tid,
                new_tuple.as_raw(),
            )
        }
    }

    /// Update a catalog tuple and report PostgreSQL tuple-version conflicts.
    pub fn catalog_update_optimistic(
        &self,
        old_tuple: HeapTupleRef<'_>,
        new_tuple: &HeapTupleGuard,
    ) -> Result<CatalogUpdateResult, PgError> {
        let mut old_tid = old_tuple.item_pointer_data();
        let updated = unsafe {
            PgWrapper::catalog_tuple_update_optimistic_raw(
                self.as_raw(),
                &mut old_tid,
                new_tuple.as_raw(),
            )
        }?;
        Ok(if updated {
            CatalogUpdateResult::Success
        } else {
            CatalogUpdateResult::Conflict
        })
    }

    /// Delete a catalog tuple.
    pub fn catalog_delete(&self, tuple: HeapTupleRef<'_>) -> Result<(), PgError> {
        let mut tid = tuple.item_pointer_data();
        unsafe { PgWrapper::catalog_tuple_delete_raw(self.as_raw(), &mut tid) }
    }

    /// Begin a system catalog scan tied to this relation guard.
    pub fn begin_scan<'scan, I>(
        &'scan self,
        index_id: pg_sys::Oid,
        index_ok: bool,
        snapshot: CatalogSnapshot<'scan>,
        keys: I,
    ) -> Result<CatalogScan<'scan>, PgError>
    where
        I: IntoIterator<Item = CatalogScanKey>,
    {
        let mut keys = Self::collect_scan_keys(keys);
        let key_ptr = Self::scan_key_ptr(&mut keys);
        let scan = unsafe {
            PgWrapper::systable_beginscan_raw(
                self.as_raw(),
                index_id,
                index_ok,
                snapshot.as_raw(),
                keys.len() as i32,
                key_ptr,
            )?
        };

        Ok(CatalogScan {
            scan,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Begin a scan that explicitly guarantees index order.
    pub fn begin_ordered_scan<'scan, I>(
        &'scan self,
        index_id: pg_sys::Oid,
        snapshot: CatalogSnapshot<'scan>,
        keys: I,
    ) -> Result<CatalogOrderedScan<'scan>, PgError>
    where
        I: IntoIterator<Item = CatalogScanKey>,
    {
        let index_relation = unsafe {
            PgWrapper::index_open_raw(index_id, pg_sys::AccessShareLock as _)?
        };
        let mut keys = Self::collect_scan_keys(keys);
        let key_ptr = Self::scan_key_ptr(&mut keys);
        let scan = unsafe {
            PgWrapper::systable_beginscan_ordered_raw(
                self.as_raw(),
                index_relation,
                snapshot.as_raw(),
                keys.len() as i32,
                key_ptr,
            )
        };
        match scan {
            Ok(scan) => Ok(CatalogOrderedScan {
                scan,
                index_relation,
                _phantom: std::marker::PhantomData,
            }),
            Err(error) => {
                let _ = unsafe {
                    PgWrapper::index_close_raw(
                        index_relation,
                        pg_sys::AccessShareLock as _,
                    )
                };
                Err(error)
            }
        }
    }

    #[inline]
    fn collect_scan_keys<I>(keys: I) -> Vec<pg_sys::ScanKeyData>
    where
        I: IntoIterator<Item = CatalogScanKey>,
    {
        keys.into_iter().map(CatalogScanKey::into_data).collect()
    }

    #[inline]
    fn scan_key_ptr(keys: &mut [pg_sys::ScanKeyData]) -> pg_sys::ScanKey {
        if keys.is_empty() {
            std::ptr::null_mut()
        } else {
            keys.as_mut_ptr()
        }
    }
}

/// Reusable catalog index state for a bounded sequence of inserts.
#[derive(Debug)]
pub struct CatalogWriter<'a> {
    relation: &'a CatalogRelation,
    index_state: pg_sys::CatalogIndexState,
}

impl CatalogWriter<'_> {
    pub fn insert(&mut self, tuple: &HeapTupleGuard) -> Result<(), PgError> {
        unsafe {
            PgWrapper::catalog_tuple_insert_with_info_raw(
                self.relation.as_raw(),
                tuple.as_raw(),
                self.index_state,
            )
        }
    }
}

impl Drop for CatalogWriter<'_> {
    fn drop(&mut self) {
        if self.index_state.is_null() {
            return;
        }
        let _ = unsafe { PgWrapper::catalog_close_indexes_raw(self.index_state) };
        self.index_state = std::ptr::null_mut();
    }
}

/// RAII guard for system table scan - automatically ends scan when dropped.
#[derive(Debug)]
pub struct CatalogScan<'a> {
    scan: pg_sys::SysScanDesc,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl CatalogScan<'_> {
    /// Get the raw scan descriptor.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::SysScanDesc {
        self.scan
    }

    /// Fetch the next tuple from this system table scan.
    ///
    /// PostgreSQL invalidates the previously returned tuple on the next
    /// `systable_getnext` call, so this method requires `&mut self` to prevent
    /// callers from holding two scan tuple references at the same time.
    pub fn get_next(&mut self) -> Result<Option<HeapTupleRef<'_>>, PgError> {
        let tuple = unsafe { PgWrapper::systable_getnext_raw(self.scan)? };
        Ok(tuple.map(|tuple| unsafe { HeapTupleRef::from_raw(tuple) }))
    }
}

impl Drop for CatalogScan<'_> {
    fn drop(&mut self) {
        // Drop cannot report PostgreSQL errors; ending the scan is best-effort
        // cleanup at this point.
        let _ = unsafe { PgWrapper::systable_endscan_raw(self.scan) };
    }
}

/// RAII guard for a catalog scan with explicit index ordering.
#[derive(Debug)]
pub struct CatalogOrderedScan<'a> {
    scan: pg_sys::SysScanDesc,
    index_relation: pg_sys::Relation,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl CatalogOrderedScan<'_> {
    pub fn get_next(&mut self) -> Result<Option<HeapTupleRef<'_>>, PgError> {
        let tuple = unsafe { PgWrapper::systable_getnext_ordered_raw(self.scan)? };
        Ok(tuple.map(|tuple| unsafe { HeapTupleRef::from_raw(tuple) }))
    }
}

impl Drop for CatalogOrderedScan<'_> {
    fn drop(&mut self) {
        let _ = unsafe { PgWrapper::systable_endscan_ordered_raw(self.scan) };
        let _ = unsafe {
            PgWrapper::index_close_raw(
                self.index_relation,
                pg_sys::AccessShareLock as _,
            )
        };
    }
}
