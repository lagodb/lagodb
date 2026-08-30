use std::ffi::{CStr, c_char};

use lagodb_core::catalog;
use pgrx::{IntoDatum, pg_sys};

use crate::catalog::worker::{
    CatalogName, NewWorkerRegistration, WorkerCatalog, WorkerId,
    WorkerRegistrationRow,
};
use crate::error::{
    LagodbError, LagodbResult, WorkerCatalogOperation as CatalogOperation,
    WorkerCatalogResultExt,
};

struct WorkerEntrypointContract;

impl WorkerEntrypointContract {
    fn accepts(procedure: &pg_sys::FormData_pg_proc) -> bool {
        procedure.prokind == pg_sys::PROKIND_FUNCTION as c_char
            && !procedure.proretset
            && procedure.pronargs == 1
            // SAFETY: pronargs == 1 guarantees that the first oidvector element
            // belongs to this live pg_proc tuple.
            && unsafe { procedure.proargtypes.values.as_slice(1)[0] }
                == pg_sys::INTERNALOID
            && procedure.prorettype == pg_sys::INT8OID
    }
}

#[derive(Debug)]
pub(crate) struct WorkerRegistration {
    pub(crate) worker_id: i32,
    pub(crate) registration_owner_oid: pg_sys::Oid,
    pub(crate) worker_name: String,
    pub(crate) function_oid: pg_sys::Oid,
}

pub(crate) fn load_all() -> LagodbResult<Vec<WorkerRegistration>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    let rows = catalog.rows()?;
    let mut registrations = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(registration) = resolve_registration(row)? {
            registrations.push(registration);
        }
    }
    registrations.sort_by_key(|registration| registration.worker_id);
    Ok(registrations)
}

pub(crate) fn runtime_catalog_exists() -> LagodbResult<bool> {
    // SAFETY: called inside the coordinator's lifecycle transaction. The
    // missing-ok extension lookup and the worker-table OID lookup do not open
    // or lock the runtime worker relation.
    let extension_oid =
        unsafe { pg_sys::get_extension_oid(c"lagodb_base".as_ptr(), true) };
    if extension_oid == pg_sys::InvalidOid {
        return Ok(false);
    }
    WorkerCatalog::exists()
}

pub(crate) fn database_is_template(database_oid: u32) -> bool {
    // SAFETY: database_oid is the current backend database OID and Oid has a
    // stable scalar Datum representation.
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::DATABASEOID as i32,
            pg_sys::Oid::from(database_oid)
                .into_datum()
                .expect("Oid has a Datum representation"),
        )
    };
    assert!(
        !tuple.is_null(),
        "current database must have a pg_database syscache entry",
    );
    // SAFETY: DATABASEOID returned a pinned pg_database tuple, which remains
    // valid until ReleaseSysCache below.
    let is_template = unsafe {
        (*(pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_database)).datistemplate
    };
    // SAFETY: tuple is the pinned syscache tuple returned above.
    unsafe { pg_sys::ReleaseSysCache(tuple) };
    is_template
}

pub(crate) fn load_if_runtime_installed()
-> LagodbResult<Option<Vec<WorkerRegistration>>> {
    // SAFETY: called inside the coordinator's database transaction. The
    // missing-ok lookup does not raise ERROR for databases where
    // lagodb_base is not installed.
    let extension_oid =
        unsafe { pg_sys::get_extension_oid(c"lagodb_base".as_ptr(), true) };
    if extension_oid == pg_sys::InvalidOid {
        Ok(None)
    } else {
        load_all().map(Some)
    }
}

pub(crate) fn load_one(worker_id: i32) -> LagodbResult<Option<WorkerRegistration>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    let Some(row) = catalog.row_by_id(WorkerId::new(worker_id))? else {
        return Ok(None);
    };
    resolve_registration(row)
}

pub(crate) fn resolve_worker_id(
    extension_name: &CStr,
    worker_name: &str,
) -> LagodbResult<Option<i32>> {
    let catalog = WorkerCatalog::open(pg_sys::AccessShareLock as _)?;
    catalog
        .worker_id_by_locator(extension_name, worker_name)
        .map(|worker_id| worker_id.map(WorkerId::as_i32))
}

pub(crate) fn delete_extension_registrations(
    extension_name: &CStr,
) -> LagodbResult<()> {
    WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?
        .delete_by_extension_name(extension_name)
}

pub(crate) fn extension_has_registrations(
    extension_name: &CStr,
) -> LagodbResult<bool> {
    WorkerCatalog::open(pg_sys::AccessShareLock as _)?
        .contains_extension_name(extension_name)
}

pub(crate) fn register(
    worker_name: &str,
    function_oid: pg_sys::Oid,
) -> LagodbResult<i32> {
    if !unsafe { pg_sys::creating_extension } {
        return Err(LagodbError::WorkerRegistrationRequiresExtensionScript);
    }
    if worker_name.is_empty() || worker_name.len() > 255 {
        return Err(LagodbError::InvalidWorkerName);
    }

    let extension_oid = unsafe { pg_sys::CurrentExtensionObject };
    let (entrypoint_schema, entrypoint_function) = validate_entrypoint(function_oid)?;
    let extension_name = current_extension_name(extension_oid)?;
    let catalog = WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?;
    let worker_id = catalog.insert(NewWorkerRegistration {
        extension_name: extension_name.as_c_str(),
        worker_name,
        entrypoint_schema: entrypoint_schema.as_c_str(),
        entrypoint_function: entrypoint_function.as_c_str(),
    })?;
    Ok(worker_id.as_i32())
}

pub(crate) fn deregister(worker_name: &str) -> LagodbResult<Option<i32>> {
    let catalog = WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?;
    catalog
        .delete_by_name(worker_name)
        .map(|worker_id| worker_id.map(WorkerId::as_i32))
}

pub(crate) fn deregister_by_id(worker_id: i32) -> LagodbResult<bool> {
    WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?
        .delete_by_id(WorkerId::new(worker_id))
}

pub(crate) fn deregister_self(worker_id: i32) -> LagodbResult<()> {
    let deleted = WorkerCatalog::open(pg_sys::RowExclusiveLock as _)?
        .delete_by_id(WorkerId::new(worker_id))?;
    if deleted {
        Ok(())
    } else {
        Err(LagodbError::WorkerIdNotRegistered { worker_id })
    }
}

fn resolve_registration(
    row: WorkerRegistrationRow,
) -> LagodbResult<Option<WorkerRegistration>> {
    let Some(extension_oid) = extension_oid_by_name(row.extension_name.as_c_str())?
    else {
        return Ok(None);
    };
    let Some(function_oid) = resolve_entrypoint(
        row.entrypoint_schema.as_c_str(),
        row.entrypoint_function.as_c_str(),
    )?
    else {
        return Ok(None);
    };
    Ok(Some(WorkerRegistration {
        worker_id: row.worker_id.as_i32(),
        registration_owner_oid: extension_oid,
        worker_name: row.worker_name,
        function_oid,
    }))
}

fn extension_oid_by_name(extension_name: &CStr) -> LagodbResult<Option<pg_sys::Oid>> {
    // SAFETY: extension_name is a live, NUL-terminated catalog name for this
    // synchronous lookup; missing_ok prevents an absent extension from ERROR.
    let oid = unsafe { pg_sys::get_extension_oid(extension_name.as_ptr(), true) };
    Ok((oid != pg_sys::InvalidOid).then_some(oid))
}

fn resolve_entrypoint(
    schema_name: &CStr,
    function_name: &CStr,
) -> LagodbResult<Option<pg_sys::Oid>> {
    let schema_oid = catalog::get_namespace_oid(schema_name, true)
        .map_worker_catalog_err(CatalogOperation::ResolveEntrypoint)?;
    if schema_oid == pg_sys::InvalidOid {
        return Ok(None);
    }
    let argument_types = [pg_sys::INTERNALOID];
    // SAFETY: buildoidvector copies the single live OID. PROCNAMEARGSNSP uses
    // (proname, proargtypes, pronamespace), exactly matching this lookup.
    let argument_vector = unsafe {
        pg_sys::buildoidvector(argument_types.as_ptr(), argument_types.len() as i32)
    };
    let tuple = unsafe {
        pg_sys::SearchSysCache3(
            pg_sys::SysCacheIdentifier::PROCNAMEARGSNSP as i32,
            pg_sys::Datum::from(function_name.as_ptr() as usize),
            pg_sys::Datum::from(argument_vector as usize),
            pg_sys::Datum::from(schema_oid),
        )
    };
    unsafe { pg_sys::pfree(argument_vector.cast()) };
    if tuple.is_null() {
        return Ok(None);
    }
    // SAFETY: PROCNAMEARGSNSP returned a pinned pg_proc tuple that remains
    // live until ReleaseSysCache below.
    let procedure = unsafe { &*(pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_proc) };
    let matches = WorkerEntrypointContract::accepts(procedure);
    let function_oid = matches.then_some(procedure.oid);
    // SAFETY: tuple is the pinned syscache tuple returned above.
    unsafe { pg_sys::ReleaseSysCache(tuple) };
    Ok(function_oid)
}

fn validate_entrypoint(
    function_oid: pg_sys::Oid,
) -> LagodbResult<(CatalogName, CatalogName)> {
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::PROCOID as i32,
            function_oid
                .into_datum()
                .expect("Oid has a Datum representation"),
        )
    };
    if tuple.is_null() {
        return Err(LagodbError::EntryPointMissing);
    }
    // SAFETY: PROCOID returned a pinned pg_proc tuple that remains live until
    // ReleaseSysCache below.
    let procedure = unsafe { &*(pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_proc) };
    if !WorkerEntrypointContract::accepts(procedure) {
        // SAFETY: tuple is the pinned syscache tuple returned above.
        unsafe { pg_sys::ReleaseSysCache(tuple) };
        return Err(LagodbError::InvalidEntryPointSignature);
    }
    // SAFETY: procedure is a live pg_proc tuple and pronamespace is its valid
    // namespace OID. PostgreSQL returns a palloc'd copy or NULL.
    let namespace = unsafe { pg_sys::get_namespace_name(procedure.pronamespace) };
    if namespace.is_null() {
        // SAFETY: tuple is the pinned syscache tuple returned above.
        unsafe { pg_sys::ReleaseSysCache(tuple) };
        return Err(LagodbError::EntryPointSchemaMissing);
    }
    // SAFETY: namespace is the live, NUL-terminated result from
    // get_namespace_name and is copied before being freed.
    let schema_name = CatalogName::from_c_str(unsafe { CStr::from_ptr(namespace) });
    // SAFETY: namespace is the palloc'd result from get_namespace_name.
    unsafe { pg_sys::pfree(namespace.cast()) };
    // SAFETY: pg_proc.proname is a PostgreSQL-produced NameData value and is
    // therefore NUL-terminated within its fixed-width buffer.
    let function_name = unsafe { CatalogName::from_name_data(procedure.proname) };
    // SAFETY: tuple is the pinned syscache tuple returned above, and all data
    // needed after release has been copied into owned values.
    unsafe { pg_sys::ReleaseSysCache(tuple) };
    Ok((schema_name, function_name))
}

fn current_extension_name(extension_oid: pg_sys::Oid) -> LagodbResult<CatalogName> {
    // SAFETY: extension_oid is CurrentExtensionObject during registration;
    // PostgreSQL returns a palloc'd copy or NULL.
    let name_ptr = unsafe { pg_sys::get_extension_name(extension_oid) };
    if name_ptr.is_null() {
        Err(LagodbError::RegisteringExtensionMissing)
    } else {
        // SAFETY: get_extension_name returned a live palloc'd C string.
        let name = CatalogName::from_c_str(unsafe { CStr::from_ptr(name_ptr) });
        unsafe { pg_sys::pfree(name_ptr.cast()) };
        Ok(name)
    }
}
