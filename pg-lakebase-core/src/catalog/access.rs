use crate::diag::PgError;
use crate::handles::{
    HeapTupleGuard, HeapTupleRef, RelationGuard, RelationHandle, SnapshotHandle,
};
use crate::wrapper::PgWrapper;
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

    #[inline]
    pub fn as_handle(&self) -> RelationHandle<'_> {
        self.guard.as_handle()
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.guard.as_raw()
    }

    #[inline]
    pub fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { (*self.as_raw()).rd_att }
    }

    /// Insert an owned heap tuple into this catalog relation.
    pub fn catalog_insert(&self, tuple: &HeapTupleGuard) -> Result<(), PgError> {
        unsafe { PgWrapper::catalog_tuple_insert_raw(self.as_raw(), tuple.as_raw()) }
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
        snapshot: CatalogSnapshot<'_>,
        keys: I,
    ) -> Result<CatalogScan<'scan>, PgError>
    where
        I: IntoIterator<Item = CatalogScanKey>,
    {
        let mut keys: Vec<pg_sys::ScanKeyData> =
            keys.into_iter().map(|key| key.into_data()).collect();
        let key_ptr = if keys.is_empty() {
            std::ptr::null_mut()
        } else {
            keys.as_mut_ptr()
        };
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
