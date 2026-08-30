use std::ffi::CStr;

use lagodb_core::catalog::{
    self, CatalogRelation, CatalogScanKey, CatalogSnapshot, LAGODB_SCHEMA,
};
use lagodb_core::diag::PgError;
use pgrx::{FromDatum, PgTryBuilder, pg_sys};

use crate::error::{
    LagodbError, LagodbResult, WorkerCatalogOperation, WorkerCatalogResultExt,
};

use super::worker_row::WorkerTuple;
pub(crate) use super::worker_row::{
    CatalogName, NewWorkerRegistration, WorkerRegistrationRow,
};

const WORKERS_TABLE: &CStr = c"workers";
const WORKER_ID_SEQUENCE: &CStr = c"worker_id_seq";
const WORKERS_PRIMARY_KEY: &CStr = c"workers_pkey";
const WORKERS_NAME_KEY: &CStr = c"workers_worker_name_key";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct WorkerId(i32);

impl WorkerId {
    pub(crate) const fn new(value: i32) -> Self {
        Self(value)
    }

    pub(crate) const fn as_i32(self) -> i32 {
        self.0
    }
}

pub(crate) struct WorkerCatalog {
    relation: CatalogRelation,
    schema_oid: pg_sys::Oid,
}

impl WorkerCatalog {
    /// Adapt pgrx's Rust-ABI binding to PostgreSQL's `PGFunction` ABI.
    ///
    /// # Safety
    ///
    /// `fcinfo` must be the live call frame PostgreSQL supplies to
    /// `DirectFunctionCall1Coll` for `nextval_oid`.
    unsafe extern "C-unwind" fn nextval_oid_ffi(
        fcinfo: pg_sys::FunctionCallInfo,
    ) -> pg_sys::Datum {
        // SAFETY: the caller supplies PostgreSQL's live FunctionCallInfo frame.
        unsafe { pg_sys::nextval_oid(fcinfo) }
    }

    /// Return whether the runtime worker table is present without opening it.
    ///
    /// This is used by the coordinator's lifecycle barrier transaction. OID
    /// lookup uses PostgreSQL syscaches and deliberately does not acquire a
    /// relation lock that could overlap the lifecycle lock.
    pub(crate) fn exists() -> LagodbResult<bool> {
        let schema_oid = catalog::get_namespace_oid(LAGODB_SCHEMA, true)
            .map_worker_catalog_err(WorkerCatalogOperation::ResolveSchema)?;
        if schema_oid == pg_sys::InvalidOid {
            return Ok(false);
        }
        catalog::get_relation_oid(WORKERS_TABLE, schema_oid)
            .map(|relation_oid| relation_oid != pg_sys::InvalidOid)
            .map_worker_catalog_err(WorkerCatalogOperation::ResolveRelation)
    }

    pub(crate) fn open(lock_mode: pg_sys::LOCKMODE) -> LagodbResult<Self> {
        let schema_oid = catalog::get_namespace_oid(LAGODB_SCHEMA, false)
            .map_worker_catalog_err(WorkerCatalogOperation::ResolveSchema)?;
        let relation_oid = catalog::get_relation_oid(WORKERS_TABLE, schema_oid)
            .map_worker_catalog_err(WorkerCatalogOperation::ResolveRelation)?;
        if relation_oid == pg_sys::InvalidOid {
            return Err(LagodbError::WorkersTableMissing);
        }
        let relation = CatalogRelation::open_retain_lock(relation_oid, lock_mode)
            .map_worker_catalog_err(WorkerCatalogOperation::Open)?;
        Ok(Self {
            relation,
            schema_oid,
        })
    }

    pub(crate) fn rows(&self) -> LagodbResult<Vec<WorkerRegistrationRow>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(pg_sys::InvalidOid, false, CatalogSnapshot::Default, [])
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        let mut rows = Vec::new();
        while let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        {
            // SAFETY: the tuple comes from lagodb.workers and its descriptor
            // belongs to this live catalog relation.
            rows.push(unsafe { WorkerTuple::new(tuple, tuple_desc) }.decode());
        }
        Ok(rows)
    }

    pub(crate) fn insert(
        &self,
        registration: NewWorkerRegistration<'_>,
    ) -> LagodbResult<WorkerId> {
        let worker_id = self.next_worker_id()?;
        // SAFETY: the descriptor belongs to the open lagodb.workers relation.
        let tuple = unsafe {
            WorkerTuple::encode(
                &registration,
                worker_id,
                self.relation.as_handle().tuple_desc(),
            )
        };
        self.relation
            .catalog_insert(&tuple)
            .map_worker_catalog_err(WorkerCatalogOperation::Insert)?;
        Ok(worker_id)
    }

    /// Resolve a database-global worker name and verify its expected owner.
    ///
    /// `workers_worker_name_key` deliberately contains only `worker_name`.
    /// `extension_name` is an ownership guard against a stale or incorrect
    /// provider locator, not a second lookup key.
    pub(crate) fn worker_id_by_locator(
        &self,
        extension_name: &CStr,
        worker_name: &str,
    ) -> LagodbResult<Option<WorkerId>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.name_key_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::text_eq(
                    WorkerTuple::worker_name_attno(),
                    worker_name,
                )],
            )
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        else {
            return Ok(None);
        };
        // SAFETY: the tuple comes from lagodb.workers and its descriptor
        // belongs to this live catalog relation.
        let tuple = unsafe { WorkerTuple::new(tuple, tuple_desc) };
        Ok(tuple
            .extension_name_eq(extension_name)
            .then(|| tuple.worker_id()))
    }

    pub(crate) fn row_by_id(
        &self,
        worker_id: WorkerId,
    ) -> LagodbResult<Option<WorkerRegistrationRow>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.primary_key_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::i32_eq(
                    WorkerTuple::worker_id_attno(),
                    worker_id.as_i32(),
                )],
            )
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        else {
            return Ok(None);
        };
        // SAFETY: the tuple comes from lagodb.workers and its descriptor
        // belongs to this live catalog relation.
        Ok(Some(
            unsafe { WorkerTuple::new(tuple, tuple_desc) }.decode(),
        ))
    }

    /// Delete by database-global worker name.
    ///
    /// No extension key is needed because the catalog rejects duplicate worker
    /// names across extensions.
    pub(crate) fn delete_by_name(
        &self,
        worker_name: &str,
    ) -> LagodbResult<Option<WorkerId>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.name_key_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::text_eq(
                    WorkerTuple::worker_name_attno(),
                    worker_name,
                )],
            )
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        else {
            return Ok(None);
        };
        // SAFETY: the tuple comes from lagodb.workers and its descriptor
        // belongs to this live catalog relation.
        let worker_id = unsafe { WorkerTuple::new(tuple, tuple_desc) }.worker_id();
        self.relation
            .catalog_delete(tuple)
            .map_worker_catalog_err(WorkerCatalogOperation::Delete)?;
        Ok(Some(worker_id))
    }

    pub(crate) fn delete_by_id(&self, worker_id: WorkerId) -> LagodbResult<bool> {
        let mut scan = self
            .relation
            .begin_scan(
                self.primary_key_oid()?,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::i32_eq(
                    WorkerTuple::worker_id_attno(),
                    worker_id.as_i32(),
                )],
            )
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        else {
            return Ok(false);
        };
        self.relation
            .catalog_delete(tuple)
            .map_worker_catalog_err(WorkerCatalogOperation::Delete)?;
        Ok(true)
    }

    pub(crate) fn contains_extension_name(
        &self,
        extension_name: &CStr,
    ) -> LagodbResult<bool> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(pg_sys::InvalidOid, false, CatalogSnapshot::Default, [])
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        while let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        {
            // SAFETY: the tuple comes from lagodb.workers and its descriptor
            // belongs to this live catalog relation.
            if unsafe { WorkerTuple::new(tuple, tuple_desc) }
                .extension_name_eq(extension_name)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn delete_by_extension_name(
        &self,
        extension_name: &CStr,
    ) -> LagodbResult<()> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(pg_sys::InvalidOid, false, CatalogSnapshot::Default, [])
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?;
        while let Some(tuple) = scan
            .get_next()
            .map_worker_catalog_err(WorkerCatalogOperation::Scan)?
        {
            // SAFETY: the tuple comes from lagodb.workers and its descriptor
            // belongs to this live catalog relation.
            if unsafe { WorkerTuple::new(tuple, tuple_desc) }
                .extension_name_eq(extension_name)
            {
                self.relation
                    .catalog_delete(tuple)
                    .map_worker_catalog_err(WorkerCatalogOperation::Delete)?;
            }
        }
        Ok(())
    }

    fn next_worker_id(&self) -> LagodbResult<WorkerId> {
        let sequence_oid = self.sequence_oid()?;
        // SAFETY: nextval_oid is PostgreSQL's native sequence function. The
        // sequence OID resolves to lagodb.worker_id_seq, which is declared AS
        // integer. PostgreSQL raises the authoritative permission/type error.
        let datum = unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::DirectFunctionCall1Coll(
                    Some(Self::nextval_oid_ffi),
                    pg_sys::InvalidOid,
                    pg_sys::Datum::from(sequence_oid),
                ))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }
        .map_worker_catalog_err(WorkerCatalogOperation::AllocateId)?;
        // SAFETY: nextval_oid returns an int8 Datum.
        let value = unsafe { i64::from_datum(datum, false) }
            .expect("nextval_oid returns bigint");
        Ok(WorkerId(i32::try_from(value).expect(
            "an integer sequence cannot produce a value outside int4",
        )))
    }

    fn sequence_oid(&self) -> LagodbResult<pg_sys::Oid> {
        let oid = self.object_oid(
            WORKER_ID_SEQUENCE,
            WorkerCatalogOperation::ResolveSequence,
        )?;
        if oid == pg_sys::InvalidOid {
            Err(LagodbError::WorkerIdSequenceMissing)
        } else {
            Ok(oid)
        }
    }

    fn primary_key_oid(&self) -> LagodbResult<pg_sys::Oid> {
        let oid = self
            .object_oid(WORKERS_PRIMARY_KEY, WorkerCatalogOperation::ResolveIndex)?;
        if oid == pg_sys::InvalidOid {
            Err(LagodbError::WorkersPrimaryKeyMissing)
        } else {
            Ok(oid)
        }
    }

    fn name_key_oid(&self) -> LagodbResult<pg_sys::Oid> {
        let oid =
            self.object_oid(WORKERS_NAME_KEY, WorkerCatalogOperation::ResolveIndex)?;
        if oid == pg_sys::InvalidOid {
            Err(LagodbError::WorkersNameIndexMissing)
        } else {
            Ok(oid)
        }
    }

    fn object_oid(
        &self,
        name: &CStr,
        operation: WorkerCatalogOperation,
    ) -> LagodbResult<pg_sys::Oid> {
        catalog::get_relation_oid(name, self.schema_oid)
            .map_worker_catalog_err(operation)
    }
}
