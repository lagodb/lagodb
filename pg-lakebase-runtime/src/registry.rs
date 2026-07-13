use std::ffi::{CStr, CString};

use pg_lakebase_core::catalog::{
    self, CatalogRelation, CatalogScanKey, CatalogSnapshot, LAKEBASE_SCHEMA,
};
use pg_lakebase_core::handles::HeapTupleGuard;
use pgrx::{FromDatum, IntoDatum, pg_sys};

use crate::error::{
    LakebaseError, LakebaseResult, WorkerCatalogOperation as CatalogOperation,
};

const WORKERS_TABLE: &CStr = c"workers";

mod column {
    pub const EXTENSION_NAME: i16 = 1;
    pub const WORKER_NAME: i16 = 2;
    pub const ENTRYPOINT_SCHEMA: i16 = 3;
    pub const ENTRYPOINT_FUNCTION: i16 = 4;
    pub const COUNT: usize = 4;
}

#[derive(Clone, Debug)]
pub(crate) struct WorkerRegistration {
    pub(crate) extension_oid: pg_sys::Oid,
    pub(crate) worker_name: String,
    pub(crate) function_oid: pg_sys::Oid,
}

#[derive(Clone, Debug)]
pub(crate) struct RegisteredWorker {
    pub(crate) extension_oid: pg_sys::Oid,
    pub(crate) worker_name: String,
}

#[derive(Clone, Debug)]
struct WorkerRegistrationRow {
    extension_name: String,
    worker_name: String,
    entrypoint_schema: String,
    entrypoint_function: String,
}

struct WorkerCatalog {
    relation: CatalogRelation,
}

impl WorkerCatalog {
    fn open(lock_mode: pg_sys::LOCKMODE) -> LakebaseResult<Self> {
        Ok(Self {
            relation: open_workers(lock_mode)?,
        })
    }

    fn rows(&self) -> LakebaseResult<Vec<WorkerRegistrationRow>> {
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
                operation: CatalogOperation::Scan,
                source,
            })?;
        let mut rows = Vec::new();
        while let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: CatalogOperation::Scan,
                    source,
                })?
        {
            rows.push(unsafe {
                WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc)
            });
        }
        Ok(rows)
    }

    fn insert(&self, row: WorkerRegistrationRow) -> LakebaseResult<()> {
        let tuple_desc = self.relation.as_handle().tuple_desc();
        let tuple = row.encode(tuple_desc);
        self.relation.catalog_insert(&tuple).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: CatalogOperation::Insert,
                source,
            }
        })
    }

    fn delete_by_extension_name(&self, extension_name: &CStr) -> LakebaseResult<()> {
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
                operation: CatalogOperation::Scan,
                source,
            })?;
        while let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: CatalogOperation::Scan,
                    source,
                })?
        {
            if unsafe {
                name_attr(
                    tuple.as_raw(),
                    tuple_desc,
                    column::EXTENSION_NAME,
                    "extension_name",
                )
            }
            .as_bytes()
                == extension_name.to_bytes()
            {
                self.relation.catalog_delete(tuple).map_err(|source| {
                    LakebaseError::WorkerCatalog {
                        operation: CatalogOperation::Delete,
                        source,
                    }
                })?;
            }
        }
        Ok(())
    }

    fn delete(
        &self,
        extension_name: &str,
        worker_name: &str,
    ) -> LakebaseResult<bool> {
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
                operation: CatalogOperation::Scan,
                source,
            })?;
        while let Some(tuple) =
            scan.get_next()
                .map_err(|source| LakebaseError::WorkerCatalog {
                    operation: CatalogOperation::Scan,
                    source,
                })?
        {
            let row =
                unsafe { WorkerRegistrationRow::decode(tuple.as_raw(), tuple_desc) };
            if row.extension_name == extension_name && row.worker_name == worker_name
            {
                self.relation.catalog_delete(tuple).map_err(|source| {
                    LakebaseError::WorkerCatalog {
                        operation: CatalogOperation::Delete,
                        source,
                    }
                })?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl WorkerRegistrationRow {
    unsafe fn decode(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Self {
        Self {
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

    fn encode(&self, tuple_desc: pg_sys::TupleDesc) -> HeapTupleGuard {
        let extension_name = PgName::new(&self.extension_name);
        let entrypoint_schema = PgName::new(&self.entrypoint_schema);
        let entrypoint_function = PgName::new(&self.entrypoint_function);
        let mut values = [pg_sys::Datum::from(0_usize); column::COUNT];
        let mut nulls = [false; column::COUNT];
        values[idx(column::EXTENSION_NAME)] = extension_name.datum();
        values[idx(column::WORKER_NAME)] = self
            .worker_name
            .as_str()
            .into_datum()
            .expect("str converts to Datum");
        values[idx(column::ENTRYPOINT_SCHEMA)] = entrypoint_schema.datum();
        values[idx(column::ENTRYPOINT_FUNCTION)] = entrypoint_function.datum();
        unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            ))
        }
    }
}

struct PgName {
    data: pg_sys::NameData,
}

impl PgName {
    fn new(value: &str) -> Self {
        let cstring =
            CString::new(value).expect("PostgreSQL names cannot contain NUL");
        let mut data = pg_sys::NameData::default();
        unsafe { pg_sys::namestrcpy(&mut data, cstring.as_ptr()) };
        Self { data }
    }

    fn datum(&self) -> pg_sys::Datum {
        unsafe { pg_sys::NameGetDatum(&self.data) }
    }
}

pub(crate) fn load_all() -> LakebaseResult<Vec<WorkerRegistration>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    let mut registrations = Vec::new();
    for row in catalog.rows()? {
        if let Some(registration) = resolve_registration(&row)? {
            registrations.push(registration);
        }
    }
    registrations.sort_by(|left, right| {
        (left.extension_oid.to_u32(), left.worker_name.as_str())
            .cmp(&(right.extension_oid.to_u32(), right.worker_name.as_str()))
    });
    Ok(registrations)
}

pub(crate) fn load_if_runtime_installed()
-> LakebaseResult<Option<Vec<WorkerRegistration>>> {
    // SAFETY: called inside the reconciler's database transaction. The
    // missing-ok lookup uses the extension syscache and does not raise ERROR
    // for databases where pg_lakebase_runtime is not installed.
    let extension_oid =
        unsafe { pg_sys::get_extension_oid(c"pg_lakebase_runtime".as_ptr(), true) };
    if extension_oid == pg_sys::InvalidOid {
        Ok(None)
    } else {
        load_all().map(Some)
    }
}

pub(crate) fn load_one(
    extension_oid: pg_sys::Oid,
    worker_name: &str,
) -> LakebaseResult<Option<WorkerRegistration>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    for row in catalog.rows()? {
        if row.worker_name != worker_name {
            continue;
        }
        let Some(registration) = resolve_registration(&row)? else {
            continue;
        };
        if registration.extension_oid == extension_oid {
            return Ok(Some(registration));
        }
    }
    Ok(None)
}

pub(crate) fn registration_extension_oid(
    extension_name: &str,
    worker_name: &str,
) -> LakebaseResult<Option<pg_sys::Oid>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    for row in catalog.rows()? {
        if row.extension_name == extension_name && row.worker_name == worker_name {
            return extension_oid_by_name(&row.extension_name);
        }
    }
    Ok(None)
}

pub(crate) fn delete_extension_registrations(
    extension_name: &CStr,
) -> LakebaseResult<()> {
    WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?
        .delete_by_extension_name(extension_name)
}

pub(crate) fn extension_has_registrations(
    extension_name: &CStr,
) -> LakebaseResult<bool> {
    Ok(WorkerCatalog::open(pg_sys::AccessShareLock as _)?
        .rows()?
        .into_iter()
        .any(|row| row.extension_name.as_bytes() == extension_name.to_bytes()))
}

pub(crate) fn register(
    worker_name: &str,
    function_oid: pg_sys::Oid,
) -> LakebaseResult<RegisteredWorker> {
    if !unsafe { pg_sys::superuser() } {
        return Err(LakebaseError::WorkerRegistrationRequiresSuperuser);
    }
    if !unsafe { pg_sys::creating_extension } {
        return Err(LakebaseError::WorkerRegistrationRequiresExtensionScript);
    }
    if worker_name.is_empty() || worker_name.len() > 255 {
        return Err(LakebaseError::InvalidWorkerName);
    }

    let extension_oid = unsafe { pg_sys::CurrentExtensionObject };
    let owner_oid = unsafe {
        pg_sys::getExtensionOfObject(pg_sys::ProcedureRelationId, function_oid)
    };
    if owner_oid != extension_oid {
        return Err(LakebaseError::EntryPointNotOwnedByExtension);
    }

    let (entrypoint_schema, entrypoint_function) = validate_entrypoint(function_oid)?;
    let extension_name = current_extension_name(extension_oid)?;
    let catalog = WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?;
    catalog.insert(WorkerRegistrationRow {
        extension_name,
        worker_name: worker_name.to_owned(),
        entrypoint_schema,
        entrypoint_function,
    })?;
    Ok(RegisteredWorker {
        extension_oid,
        worker_name: worker_name.to_owned(),
    })
}

pub(crate) fn deregister(worker_name: &str) -> LakebaseResult<bool> {
    if !unsafe { pg_sys::superuser() } {
        return Err(LakebaseError::WorkerDeregistrationRequiresSuperuser);
    }
    if !unsafe { pg_sys::creating_extension } {
        return Err(LakebaseError::WorkerDeregistrationRequiresExtensionScript);
    }
    let extension_oid = unsafe { pg_sys::CurrentExtensionObject };
    let extension_name = current_extension_name(extension_oid)?;
    WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?
        .delete(&extension_name, worker_name)
}

fn resolve_registration(
    row: &WorkerRegistrationRow,
) -> LakebaseResult<Option<WorkerRegistration>> {
    let Some(extension_oid) = extension_oid_by_name(&row.extension_name)? else {
        return Ok(None);
    };
    let Some(function_oid) = resolve_entrypoint(
        &row.entrypoint_schema,
        &row.entrypoint_function,
        extension_oid,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(WorkerRegistration {
        extension_oid,
        worker_name: row.worker_name.clone(),
        function_oid,
    }))
}

fn extension_oid_by_name(
    extension_name: &str,
) -> LakebaseResult<Option<pg_sys::Oid>> {
    let cstring = CString::new(extension_name)
        .expect("PostgreSQL extension names cannot contain NUL");
    let oid = unsafe { pg_sys::get_extension_oid(cstring.as_ptr(), true) };
    Ok((oid != pg_sys::InvalidOid).then_some(oid))
}

fn resolve_entrypoint(
    schema_name: &str,
    function_name: &str,
    extension_oid: pg_sys::Oid,
) -> LakebaseResult<Option<pg_sys::Oid>> {
    let schema_name = CString::new(schema_name)
        .expect("PostgreSQL schema names cannot contain NUL");
    let schema_oid = catalog::get_namespace_oid(schema_name.as_c_str(), true)
        .map_err(|source| LakebaseError::WorkerCatalog {
            operation: CatalogOperation::ResolveEntrypoint,
            source,
        })?;
    if schema_oid == pg_sys::InvalidOid {
        return Ok(None);
    }

    let relation = CatalogRelation::open(
        pg_sys::ProcedureRelationId,
        pg_sys::AccessShareLock as _,
    )
    .map_err(|source| LakebaseError::WorkerCatalog {
        operation: CatalogOperation::ResolveEntrypoint,
        source,
    })?;
    let mut scan = relation
        .begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            [CatalogScanKey::oid_eq(
                pg_sys::Anum_pg_proc_pronamespace as _,
                schema_oid,
            )],
        )
        .map_err(|source| LakebaseError::WorkerCatalog {
            operation: CatalogOperation::ResolveEntrypoint,
            source,
        })?;
    while let Some(tuple) =
        scan.get_next()
            .map_err(|source| LakebaseError::WorkerCatalog {
                operation: CatalogOperation::ResolveEntrypoint,
                source,
            })?
    {
        let procedure =
            unsafe { pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_proc };
        let matches = unsafe {
            let proname = CStr::from_ptr((*procedure).proname.data.as_ptr());
            proname.to_bytes() == function_name.as_bytes()
                && (*procedure).pronargs == 1
                && (*procedure).proargtypes.values.as_slice(1)[0]
                    == pg_sys::INTERNALOID
                && (*procedure).prorettype == pg_sys::INT8OID
                && pg_sys::getExtensionOfObject(
                    pg_sys::ProcedureRelationId,
                    (*procedure).oid,
                ) == extension_oid
        };
        if matches {
            return Ok(Some(unsafe { (*procedure).oid }));
        }
    }
    Ok(None)
}

fn open_workers(lock_mode: pg_sys::LOCKMODE) -> LakebaseResult<CatalogRelation> {
    let schema_oid =
        catalog::get_namespace_oid(LAKEBASE_SCHEMA, false).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: CatalogOperation::ResolveSchema,
                source,
            }
        })?;
    let relation_oid =
        catalog::get_relation_oid(WORKERS_TABLE, schema_oid).map_err(|source| {
            LakebaseError::WorkerCatalog {
                operation: CatalogOperation::ResolveRelation,
                source,
            }
        })?;
    if relation_oid == pg_sys::InvalidOid {
        return Err(LakebaseError::WorkersTableMissing);
    }
    CatalogRelation::open(relation_oid, lock_mode).map_err(|source| {
        LakebaseError::WorkerCatalog {
            operation: CatalogOperation::Open,
            source,
        }
    })
}

fn validate_entrypoint(
    function_oid: pg_sys::Oid,
) -> LakebaseResult<(String, String)> {
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::PROCOID as i32,
            function_oid
                .into_datum()
                .expect("Oid has a Datum representation"),
        )
    };
    if tuple.is_null() {
        return Err(LakebaseError::EntryPointMissing);
    }
    let result = unsafe {
        let procedure = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_proc;
        let valid_signature = (*procedure).pronargs == 1
            && (*procedure).proargtypes.values.as_slice(1)[0] == pg_sys::INTERNALOID
            && (*procedure).prorettype == pg_sys::INT8OID;
        if !valid_signature {
            pg_sys::ReleaseSysCache(tuple);
            return Err(LakebaseError::InvalidEntryPointSignature);
        }
        let namespace = pg_sys::get_namespace_name((*procedure).pronamespace);
        if namespace.is_null() {
            pg_sys::ReleaseSysCache(tuple);
            return Err(LakebaseError::EntryPointSchemaMissing);
        }
        let schema_name = CStr::from_ptr(namespace).to_string_lossy().into_owned();
        let function_name = CStr::from_ptr((*procedure).proname.data.as_ptr())
            .to_string_lossy()
            .into_owned();
        pg_sys::ReleaseSysCache(tuple);
        (schema_name, function_name)
    };
    Ok(result)
}

fn current_extension_name(extension_oid: pg_sys::Oid) -> LakebaseResult<String> {
    let name = unsafe { pg_sys::get_extension_name(extension_oid) };
    if name.is_null() {
        Err(LakebaseError::RegisteringExtensionMissing)
    } else {
        Ok(unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned())
    }
}

unsafe fn required_attr<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
    name: &str,
) -> T {
    let mut is_null = false;
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tuple_desc, &mut is_null) };
    assert!(!is_null, "workers.{name} must not be null");
    unsafe { T::from_datum(datum, false) }
        .unwrap_or_else(|| panic!("workers.{name} has invalid Datum"))
}

unsafe fn name_attr(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
    name: &str,
) -> String {
    let mut is_null = false;
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tuple_desc, &mut is_null) };
    assert!(!is_null, "workers.{name} must not be null");
    let actual = unsafe {
        let name = datum.cast_mut_ptr::<pg_sys::NameData>();
        CStr::from_ptr((*name).data.as_ptr())
    };
    actual.to_string_lossy().into_owned()
}

fn idx(attno: i16) -> usize {
    usize::try_from(attno - 1).expect("attribute numbers are positive")
}
