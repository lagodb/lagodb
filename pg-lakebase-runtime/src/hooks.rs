use std::cell::Cell;
use std::ffi::CStr;

use pg_lakebase_core::catalog::{
    self, CatalogRelation, CatalogScanKey, CatalogSnapshot,
};
use pg_lakebase_core::object_cleanup::ObjectCleanupQueue;
use pgrx::prelude::*;

use crate::{error::LakebaseError, lifecycle, registry, runtime};

thread_local! {
    static PREFLIGHT_ENABLED: Cell<bool> = const { Cell::new(false) };
}

const PG_CATALOG_SCHEMA: &CStr = c"pg_catalog";
const PG_DEPEND_TABLE: &CStr = c"pg_depend";
const PG_DEPEND_REFERENCE_INDEX: &CStr = c"pg_depend_reference_index";

mod dependency_column {
    pub const CLASS_ID: i16 = 1;
    pub const OBJECT_ID: i16 = 2;
    pub const REFERENCED_CLASS_ID: i16 = 4;
    pub const REFERENCED_OBJECT_ID: i16 = 5;
    pub const REFERENCED_OBJECT_SUB_ID: i16 = 6;
}

struct RuntimeDropPreflight {
    runtime_oid: pg_sys::Oid,
}

impl RuntimeDropPreflight {
    const fn new(runtime_oid: pg_sys::Oid) -> Self {
        Self { runtime_oid }
    }

    fn ensure_no_dependent_extensions(&self) -> Result<(), LakebaseError> {
        // SAFETY: the runtime extension is a database-local object identified by
        // its live pg_extension OID. The transaction-level object lock is retained
        // by PostgreSQL and serializes this preflight with dependency changes.
        unsafe {
            pg_sys::LockDatabaseObject(
                pg_sys::ExtensionRelationId,
                self.runtime_oid,
                0,
                pg_sys::AccessExclusiveLock as _,
            );
        }
        let Some(extension_oid) = self.first_dependent_extension()? else {
            return Ok(());
        };
        let name = unsafe { pg_sys::get_extension_name(extension_oid) };
        let extension_name = if name.is_null() {
            format!("OID {}", extension_oid.to_u32())
        } else {
            unsafe { CStr::from_ptr(name) }
                .to_string_lossy()
                .into_owned()
        };
        Err(LakebaseError::RuntimeHasDependentExtension { extension_name })
    }

    fn first_dependent_extension(
        &self,
    ) -> Result<Option<pg_sys::Oid>, LakebaseError> {
        let namespace_oid = catalog::get_namespace_oid(PG_CATALOG_SCHEMA, false)
            .map_err(|source| LakebaseError::RuntimeDependencyInspection {
                source,
            })?;
        let table_oid = catalog::get_relation_oid(PG_DEPEND_TABLE, namespace_oid)
            .map_err(|source| LakebaseError::RuntimeDependencyInspection {
                source,
            })?;
        let index_oid =
            catalog::get_relation_oid(PG_DEPEND_REFERENCE_INDEX, namespace_oid)
                .map_err(|source| LakebaseError::RuntimeDependencyInspection {
                    source,
                })?;
        if table_oid == pg_sys::InvalidOid || index_oid == pg_sys::InvalidOid {
            return Err(LakebaseError::RuntimeDependencyCatalogMissing);
        }
        let relation = CatalogRelation::open(table_oid, pg_sys::AccessShareLock as _)
            .map_err(|source| LakebaseError::RuntimeDependencyInspection {
                source,
            })?;
        let tuple_desc = relation.as_handle().tuple_desc();
        let mut scan = relation
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [
                    CatalogScanKey::oid_eq(
                        dependency_column::REFERENCED_CLASS_ID as _,
                        pg_sys::ExtensionRelationId,
                    ),
                    CatalogScanKey::oid_eq(
                        dependency_column::REFERENCED_OBJECT_ID as _,
                        self.runtime_oid,
                    ),
                    CatalogScanKey::i32_eq(
                        dependency_column::REFERENCED_OBJECT_SUB_ID as _,
                        0,
                    ),
                ],
            )
            .map_err(|source| LakebaseError::RuntimeDependencyInspection {
                source,
            })?;
        while let Some(tuple) = scan
            .get_next()
            .map_err(|source| LakebaseError::RuntimeDependencyInspection { source })?
        {
            let class_id = Self::required_oid(
                tuple.as_raw(),
                tuple_desc,
                dependency_column::CLASS_ID,
            )?;
            if class_id != pg_sys::ExtensionRelationId {
                continue;
            }
            let object_id = Self::required_oid(
                tuple.as_raw(),
                tuple_desc,
                dependency_column::OBJECT_ID,
            )?;
            if object_id != self.runtime_oid {
                return Ok(Some(object_id));
            }
        }
        Ok(None)
    }

    fn required_oid(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        attribute_number: i16,
    ) -> Result<pg_sys::Oid, LakebaseError> {
        let mut is_null = false;
        // SAFETY: tuple is owned by the live catalog scan, tuple_desc belongs to
        // the open pg_depend relation, and the supplied attributes are fixed
        // NOT NULL OID columns from pg_depend.
        let datum = unsafe {
            pg_sys::heap_getattr(
                tuple,
                attribute_number as _,
                tuple_desc,
                &mut is_null,
            )
        };
        if is_null {
            return Err(LakebaseError::RuntimeDependencyCatalogMissing);
        }
        // SAFETY: the non-null Datum was read from an OID-typed pg_depend column.
        Ok(unsafe { pg_sys::DatumGetObjectId(datum) })
    }
}

pub(crate) fn init() {
    PREFLIGHT_ENABLED.set(true);
}

pub(crate) unsafe fn preflight(node: *mut pg_sys::Node) {
    if !PREFLIGHT_ENABLED.get() {
        return;
    }
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_DropdbStmt => {
            let statement = node.cast::<pg_sys::DropdbStmt>();
            let database_oid =
                unsafe { pg_sys::get_database_oid((*statement).dbname, true) };
            if database_oid != pg_sys::InvalidOid {
                runtime::DatabaseLifecycleLock::new(database_oid.to_u32())
                    .acquire_drop();
                lifecycle::request_database_drop(database_oid.to_u32());
                runtime::stop_database(database_oid.to_u32())
                    .unwrap_or_else(|error| error.report());
            }
        }
        pg_sys::NodeTag::T_CreatedbStmt => lifecycle::request_global_reconcile(),
        pg_sys::NodeTag::T_AlterDatabaseSetStmt => {
            let statement = unsafe { &*node.cast::<pg_sys::AlterDatabaseSetStmt>() };
            let database_oid =
                unsafe { pg_sys::get_database_oid(statement.dbname, false) };
            lifecycle::request_database_workers_wakeup(database_oid.to_u32());
        }
        pg_sys::NodeTag::T_DropStmt => {
            let statement = unsafe { &*node.cast::<pg_sys::DropStmt>() };
            if statement.removeType == pg_sys::ObjectType::OBJECT_EXTENSION {
                prepare_extension_drops(statement.objects);
            }
        }
        _ => {}
    }
}

fn prepare_extension_drops(objects: *mut pg_sys::List) {
    let count = unsafe { pg_sys::list_length(objects) };
    crate::diag::info(format_args!(
        "preparing {count} Lakebase extension drop object(s)"
    ));
    for index in 0..count {
        // PG17's `DROP drop_type_name name_list` grammar stores extension
        // names directly as String nodes, unlike `any_name_list` objects.
        let name_node =
            unsafe { pg_sys::list_nth(objects, index).cast::<pg_sys::String>() };
        if name_node.is_null() || unsafe { (*name_node).sval.is_null() } {
            continue;
        }
        let extension_name = unsafe { CStr::from_ptr((*name_node).sval) };
        let extension_oid =
            unsafe { pg_sys::get_extension_oid(extension_name.as_ptr(), true) };
        crate::diag::info(format_args!(
            "preparing Lakebase extension drop: extension={}, extension_oid={}",
            extension_name.to_string_lossy(),
            extension_oid.to_u32()
        ));
        if extension_oid != pg_sys::InvalidOid {
            prepare_extension_drop(extension_oid, extension_name);
        }
    }
}

fn prepare_extension_drop(extension_oid: pg_sys::Oid, extension_name: &CStr) {
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let runtime_oid =
        unsafe { pg_sys::get_extension_oid(c"pg_lakebase_runtime".as_ptr(), true) };
    if runtime_oid == pg_sys::InvalidOid {
        return;
    }
    if extension_oid == runtime_oid {
        RuntimeDropPreflight::new(runtime_oid)
            .ensure_no_dependent_extensions()
            .unwrap_or_else(|error| error.report());
        runtime::DatabaseLifecycleLock::new(database_oid).acquire_drop();
        lifecycle::request_database_reconcile();
        runtime::stop_database(database_oid).unwrap_or_else(|error| error.report());
        let has_unresolved_items = ObjectCleanupQueue::has_unresolved_items()
            .map_err(|source| LakebaseError::MaintenanceQueueInspection { source })
            .unwrap_or_else(|error| error.report());
        if has_unresolved_items {
            LakebaseError::MaintenanceQueueNotEmpty.report();
        }
    } else {
        runtime::DatabaseLifecycleLock::new(database_oid).acquire_drop();
        let has_registrations = registry::extension_has_registrations(extension_name)
            .unwrap_or_else(|error| error.report());
        if !has_registrations {
            return;
        }
        lifecycle::request_database_reconcile();
        runtime::stop_extension(database_oid, extension_oid.to_u32())
            .unwrap_or_else(|error| error.report());
        registry::delete_extension_registrations(extension_name)
            .unwrap_or_else(|error| error.report());
    }
}
