use std::ffi::{CStr, CString};

use pg_lakebase_core::catalog::{
    self, CatalogRelation, CatalogScanKey, CatalogSnapshot, LAKEBASE_SCHEMA,
};
use pg_lakebase_core::diag::PgError;
use pg_lakebase_core::handles::HeapTupleGuard;
use pgrx::{FromDatum, IntoDatum, PgTryBuilder, pg_sys};

use crate::error::{LakebaseError, LakebaseResult, WorkerCatalogOperation};

const WORKERS_TABLE: &CStr = c"workers";
const WORKER_ID_SEQUENCE: &CStr = c"worker_id_seq";
const WORKERS_PRIMARY_KEY: &CStr = c"workers_pkey";
const WORKERS_NAME_KEY: &CStr = c"workers_worker_name_key";

mod column {
    pub const WORKER_ID: i16 = 1;
    pub const EXTENSION_NAME: i16 = 2;
    pub const WORKER_NAME: i16 = 3;
    pub const ENTRYPOINT_SCHEMA: i16 = 4;
    pub const ENTRYPOINT_FUNCTION: i16 = 5;
    pub const COUNT: usize = 5;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub(crate) struct WorkerId(i32);

impl WorkerId {
    pub(crate) const fn new(value: i32) -> Self {
        Self(value)
    }

    pub(crate) const fn as_i32(self) -> i32 {
        self.0
    }
}

#[derive(Debug)]
pub(crate) struct WorkerRegistrationRow {
    pub(crate) worker_id: WorkerId,
    pub(crate) extension_name: String,
    pub(crate) worker_name: String,
    pub(crate) entrypoint_schema: String,
    pub(crate) entrypoint_function: String,
}

pub(crate) struct NewWorkerRegistration<'a> {
    pub(crate) extension_name: &'a str,
    pub(crate) worker_name: &'a str,
    pub(crate) entrypoint_schema: &'a str,
    pub(crate) entrypoint_function: &'a str,
}

pub(crate) struct WorkerCatalog {
    relation: CatalogRelation,
    sequence_oid: pg_sys::Oid,
    primary_key_oid: pg_sys::Oid,
    name_key_oid: pg_sys::Oid,
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
    pub(crate) fn exists() -> LakebaseResult<bool> {
        let schema_oid =
            catalog::get_namespace_oid(LAKEBASE_SCHEMA, true).map_err(|source| {
                LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::ResolveSchema,
                    source,
                }
            })?;
        if schema_oid == pg_sys::InvalidOid {
            return Ok(false);
        }
        catalog::get_relation_oid(WORKERS_TABLE, schema_oid)
            .map(|relation_oid| relation_oid != pg_sys::InvalidOid)
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::ResolveRelation,
                source,
            })
    }

    pub(crate) fn open(lock_mode: pg_sys::LOCKMODE) -> LakebaseResult<Self> {
        let schema_oid =
            catalog::get_namespace_oid(LAKEBASE_SCHEMA, false).map_err(|source| {
                LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::ResolveSchema,
                    source,
                }
            })?;
        let relation_oid = catalog::get_relation_oid(WORKERS_TABLE, schema_oid)
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::ResolveRelation,
                source,
            })?;
        if relation_oid == pg_sys::InvalidOid {
            return Err(LakebaseError::WorkersTableMissing);
        }
        let sequence_oid = catalog::get_relation_oid(WORKER_ID_SEQUENCE, schema_oid)
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::ResolveSequence,
                source,
            })?;
        if sequence_oid == pg_sys::InvalidOid {
            return Err(LakebaseError::WorkerIdSequenceMissing);
        }
        let primary_key_oid =
            catalog::get_relation_oid(WORKERS_PRIMARY_KEY, schema_oid).map_err(
                |source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::ResolveIndex,
                    source,
                },
            )?;
        if primary_key_oid == pg_sys::InvalidOid {
            return Err(LakebaseError::WorkersPrimaryKeyMissing);
        }
        let name_key_oid = catalog::get_relation_oid(WORKERS_NAME_KEY, schema_oid)
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::ResolveIndex,
                source,
            })?;
        if name_key_oid == pg_sys::InvalidOid {
            return Err(LakebaseError::WorkersNameIndexMissing);
        }
        let relation = CatalogRelation::open_retain_lock(relation_oid, lock_mode)
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Open,
                source,
            })?;
        Ok(Self {
            relation,
            sequence_oid,
            primary_key_oid,
            name_key_oid,
        })
    }

    pub(crate) fn rows(&self) -> LakebaseResult<Vec<WorkerRegistrationRow>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                pg_sys::InvalidOid,
                false,
                CatalogSnapshot::Default,
                std::iter::empty(),
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        let mut rows = Vec::new();
        while let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        {
            // SAFETY: `workers` bootstrap defines all five columns NOT NULL;
            // the tuple descriptor is owned by this catalog relation.
            rows.push(unsafe {
                WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc)
            });
        }
        Ok(rows)
    }

    pub(crate) fn insert(
        &self,
        registration: NewWorkerRegistration<'_>,
    ) -> LakebaseResult<WorkerId> {
        let worker_id = self.next_worker_id()?;
        let tuple =
            registration.encode(worker_id, self.relation.as_handle().tuple_desc());
        self.relation.catalog_insert(&tuple).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Insert,
                source,
            }
        })?;
        Ok(worker_id)
    }

    pub(crate) fn worker_id_by_name(
        &self,
        extension_name: &str,
        worker_name: &str,
    ) -> LakebaseResult<Option<WorkerId>> {
        Ok(self
            .row_by_name(worker_name)?
            .filter(|row| row.extension_name == extension_name)
            .map(|row| row.worker_id))
    }

    pub(crate) fn row_by_id(
        &self,
        worker_id: WorkerId,
    ) -> LakebaseResult<Option<WorkerRegistrationRow>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.primary_key_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::i32_eq(
                    column::WORKER_ID as _,
                    worker_id.as_i32(),
                )],
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        else {
            return Ok(None);
        };
        Ok(Some(unsafe {
            WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc)
        }))
    }

    pub(crate) fn delete_by_name(
        &self,
        worker_name: &str,
    ) -> LakebaseResult<Option<WorkerId>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.name_key_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::text_eq(
                    column::WORKER_NAME as _,
                    worker_name,
                )],
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        else {
            return Ok(None);
        };
        let worker_id = WorkerId(unsafe {
            required_attr(tuple.as_raw(), tuple_desc, column::WORKER_ID, "worker_id")
        });
        self.relation.catalog_delete(tuple).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Delete,
                source,
            }
        })?;
        Ok(Some(worker_id))
    }

    pub(crate) fn delete_by_id(&self, worker_id: WorkerId) -> LakebaseResult<bool> {
        let mut scan = self
            .relation
            .begin_scan(
                self.primary_key_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::i32_eq(
                    column::WORKER_ID as _,
                    worker_id.as_i32(),
                )],
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        else {
            return Ok(false);
        };
        self.relation.catalog_delete(tuple).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Delete,
                source,
            }
        })?;
        Ok(true)
    }

    pub(crate) fn delete_by_extension_name(
        &self,
        extension_name: &CStr,
    ) -> LakebaseResult<()> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                pg_sys::InvalidOid,
                false,
                CatalogSnapshot::Default,
                std::iter::empty(),
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        while let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        {
            let row =
                unsafe { WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc) };
            if row.extension_name.as_bytes() == extension_name.to_bytes() {
                self.relation.catalog_delete(tuple).map_err(|source| {
                    LakebaseError::WorkerCatalog {
                        operation: WorkerCatalogOperation::Delete,
                        source,
                    }
                })?;
            }
        }
        Ok(())
    }

    fn row_by_name(
        &self,
        worker_name: &str,
    ) -> LakebaseResult<Option<WorkerRegistrationRow>> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let mut scan = self
            .relation
            .begin_scan(
                self.name_key_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::text_eq(
                    column::WORKER_NAME as _,
                    worker_name,
                )],
            )
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: WorkerCatalogOperation::Scan,
                source,
            })?;
        let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: WorkerCatalogOperation::Scan,
                    source,
                })?
        else {
            return Ok(None);
        };
        Ok(Some(unsafe {
            WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc)
        }))
    }

    fn next_worker_id(&self) -> LakebaseResult<WorkerId> {
        // SAFETY: nextval_oid is PostgreSQL's native sequence function. The sequence OID was
        // resolved from lakebase.worker_id_seq by this catalog domain object, and the Datum is
        // an OID argument. PostgreSQL raises the authoritative permission/type error if invalid.
        let sequence_oid = self.sequence_oid;
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
        .map_err(|source| LakebaseError::WorkerCatalog {
            operation: WorkerCatalogOperation::AllocateId,
            source,
        })?;
        let value = unsafe { i64::from_datum(datum, false) }
            .expect("nextval_oid returns bigint");
        Ok(WorkerId(i32::try_from(value).expect(
            "an integer sequence cannot produce a value outside int4",
        )))
    }
}

impl NewWorkerRegistration<'_> {
    fn encode(
        &self,
        worker_id: WorkerId,
        tuple_desc: pg_sys::TupleDesc,
    ) -> HeapTupleGuard {
        let extension_name = PgName::new(self.extension_name);
        let entrypoint_schema = PgName::new(self.entrypoint_schema);
        let entrypoint_function = PgName::new(self.entrypoint_function);
        let mut values = [pg_sys::Datum::from(0_usize); column::COUNT];
        let mut nulls = [false; column::COUNT];
        values[index(column::WORKER_ID)] = pg_sys::Datum::from(worker_id.as_i32());
        values[index(column::EXTENSION_NAME)] = extension_name.datum();
        values[index(column::WORKER_NAME)] = self
            .worker_name
            .into_datum()
            .expect("str converts to Datum");
        values[index(column::ENTRYPOINT_SCHEMA)] = entrypoint_schema.datum();
        values[index(column::ENTRYPOINT_FUNCTION)] = entrypoint_function.datum();
        // SAFETY: values and nulls match the five NOT NULL lakebase.workers columns.
        unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            ))
        }
    }
}

impl WorkerRegistrationRow {
    unsafe fn decode(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Self {
        Self {
            worker_id: WorkerId(unsafe {
                required_attr(tuple, tuple_desc, column::WORKER_ID, "worker_id")
            }),
            extension_name: unsafe {
                name_attr(tuple, tuple_desc, column::EXTENSION_NAME, "extension_name")
            },
            worker_name: unsafe {
                required_attr(tuple, tuple_desc, column::WORKER_NAME, "worker_name")
            },
            entrypoint_schema: unsafe {
                name_attr(
                    tuple,
                    tuple_desc,
                    column::ENTRYPOINT_SCHEMA,
                    "entrypoint_schema",
                )
            },
            entrypoint_function: unsafe {
                name_attr(
                    tuple,
                    tuple_desc,
                    column::ENTRYPOINT_FUNCTION,
                    "entrypoint_function",
                )
            },
        }
    }
}

struct PgName {
    data: pg_sys::NameData,
}

impl PgName {
    fn new(value: &str) -> Self {
        let value = CString::new(value).expect("PostgreSQL names cannot contain NUL");
        let mut data = pg_sys::NameData::default();
        unsafe { pg_sys::namestrcpy(&mut data, value.as_ptr()) };
        Self { data }
    }
    fn datum(&self) -> pg_sys::Datum {
        unsafe { pg_sys::NameGetDatum(&self.data) }
    }
}

unsafe fn required_attr<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
    name: &str,
) -> T {
    let mut is_null = false;
    // SAFETY: tuple and descriptor come from the active catalog scan.
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tuple_desc, &mut is_null) };
    // SAFETY: the catalog column's SQL type is fixed by bootstrap.sql and the
    // producer guarantees a non-null datum.
    unsafe { T::from_datum(datum, false) }
        .unwrap_or_else(|| panic!("workers.{name} has invalid Datum"))
}

unsafe fn name_attr(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
    _name: &str,
) -> String {
    let mut is_null = false;
    // SAFETY: tuple and descriptor come from the active catalog scan.
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tuple_desc, &mut is_null) };
    // SAFETY: the datum is the fixed-width PostgreSQL `name` column declared
    // by the worker catalog schema.
    let name = datum.cast_mut_ptr::<pg_sys::NameData>();
    unsafe { CStr::from_ptr((*name).data.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

fn index(attno: i16) -> usize {
    usize::try_from(attno - 1).expect("attribute numbers are positive")
}
