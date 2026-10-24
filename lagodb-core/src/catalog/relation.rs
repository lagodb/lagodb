use std::ffi::{CStr, c_void};
use std::ptr;

use pgrx::pg_sys::RVROption::RVR_MISSING_OK;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

use super::{CatalogRelation, CatalogScanKey, CatalogSnapshot, search_syscache1};
use crate::diag::{PgError, PgReportError, ReportableError};
use crate::handles::RelationTablespace;
use crate::wrapper::PgWrapper;

/// PostgreSQL's `RangeVarCallbackForRenameAttribute` is private to
/// tablecmds.c. Keep its admission checks in this callback so callers retain
/// `RangeVarGetRelidExtended`'s check-before-lock and retry semantics.
#[pg_guard]
unsafe extern "C-unwind" fn rename_column_target_callback(
    _relation: *const pg_sys::RangeVar,
    relation_oid: pg_sys::Oid,
    _old_relation_oid: pg_sys::Oid,
    _argument: *mut c_void,
) {
    RenameColumnTarget::check_candidate(relation_oid).report_unwrap();
}

/// RENAME COLUMN target admitted and locked with PostgreSQL's native ordering.
#[derive(Debug, Clone, Copy)]
pub struct RenameColumnTarget {
    relation_oid: pg_sys::Oid,
}

impl RenameColumnTarget {
    fn check_candidate(relation_oid: pg_sys::Oid) -> Result<(), PgReportError> {
        let Some(tuple) = search_syscache1(
            pg_sys::SysCacheIdentifier::RELOID as _,
            relation_oid.into(),
        ) else {
            return Ok(());
        };
        // Keep the pg_class tuple pinned through diagnostic construction and
        // native permission checks. Returning an error releases the guard
        // before the callback reports it; a native ERROR releases it by unwind.
        let class = unsafe {
            &*pg_sys::GETSTRUCT(tuple.as_raw()).cast::<pg_sys::FormData_pg_class>()
        };
        let relation_name = unsafe { CStr::from_ptr(class.relname.data.as_ptr()) };

        if class.reloftype != pg_sys::InvalidOid {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
                "cannot rename column of typed table",
            ));
        }

        if !matches!(
            class.relkind as u8,
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_VIEW
                | pg_sys::RELKIND_MATVIEW
                | pg_sys::RELKIND_COMPOSITE_TYPE
                | pg_sys::RELKIND_INDEX
                | pg_sys::RELKIND_PARTITIONED_INDEX
                | pg_sys::RELKIND_FOREIGN_TABLE
                | pg_sys::RELKIND_PARTITIONED_TABLE
        ) {
            let detail = match class.relkind as u8 {
                pg_sys::RELKIND_SEQUENCE => {
                    "This operation is not supported for sequences."
                }
                pg_sys::RELKIND_TOASTVALUE => {
                    "This operation is not supported for TOAST tables."
                }
                relkind => {
                    return Err(PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("unrecognized relkind: {:?}", relkind as char),
                    ));
                }
            };
            return Err(PgReportError::from_parts(
                PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
                format!("cannot rename columns of relation {relation_name:?}"),
                Some(detail.to_owned()),
                None,
            ));
        }

        if unsafe {
            !pg_sys::object_ownercheck(
                pg_sys::RelationRelationId,
                relation_oid,
                pg_sys::GetUserId(),
            )
        } {
            unsafe {
                pg_sys::aclcheck_error(
                    pg_sys::AclResult::ACLCHECK_NOT_OWNER,
                    pg_sys::get_relkind_objtype(class.relkind),
                    class.relname.data.as_ptr(),
                )
            };
        }

        if unsafe {
            !pg_sys::allowSystemTableMods
                && pg_sys::IsSystemClass(
                    relation_oid,
                    class as *const pg_sys::FormData_pg_class
                        as pg_sys::Form_pg_class,
                )
        } {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("permission denied: {relation_name:?} is a system catalog"),
            ));
        }

        Ok(())
    }

    /// Resolve an existing relation, run rename-column admission checks before
    /// taking `AccessExclusiveLock`, and repeat them if concurrent DDL changes
    /// the name-to-OID mapping while the lock is acquired.
    ///
    /// A missing relation is returned as `None` so the native utility command
    /// retains ownership of its ERROR or `IF EXISTS` NOTICE.
    ///
    /// # Safety
    ///
    /// `relation` must point to a live PostgreSQL `RangeVar`.
    pub unsafe fn resolve(
        relation: *const pg_sys::RangeVar,
    ) -> Result<Option<Self>, PgReportError> {
        let relation_oid = unsafe {
            PgWrapper::range_var_get_relid_extended(
                relation,
                pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
                RVR_MISSING_OK,
                Some(rename_column_target_callback),
                ptr::null_mut(),
            )
        }
        .map_err(PgReportError::from_pg_error)?;
        Ok((relation_oid != pg_sys::InvalidOid).then_some(Self { relation_oid }))
    }

    #[inline]
    pub const fn relation_oid(self) -> pg_sys::Oid {
        self.relation_oid
    }
}

/// Stable fields copied from one `pg_class` tuple.
///
/// Copying the fields lets callers inspect catalog state without retaining a
/// scan tuple or exposing PostgreSQL tuple lifetimes outside the catalog layer.
#[derive(Debug, Clone, Copy)]
pub struct RelationCatalogEntry {
    oid: pg_sys::Oid,
    relkind: i8,
    access_method_oid: pg_sys::Oid,
    tablespace: RelationTablespace,
}

impl RelationCatalogEntry {
    /// Read current catalog identity without opening or locking the target.
    ///
    /// A missing row is a normal outcome for an unlocked admission probe.
    /// This does not provide the current-command visibility of
    /// [`CatalogSnapshot::SelfVisible`] or protect against concurrent DDL.
    pub fn find_cached(oid: pg_sys::Oid) -> Option<Self> {
        let tuple =
            search_syscache1(pg_sys::SysCacheIdentifier::RELOID as _, oid.into())?;
        // SAFETY: RELOID pins this pg_class tuple until `tuple` is dropped.
        let class =
            unsafe { &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class) };
        Some(Self::from_class(class))
    }

    /// Find a relation's `pg_class` row using the requested catalog snapshot.
    ///
    /// [`CatalogSnapshot::SelfVisible`] must be used when an object-access hook
    /// needs catalog changes made by the current command.
    pub fn find(
        oid: pg_sys::Oid,
        snapshot: CatalogSnapshot<'_>,
    ) -> Result<Option<Self>, PgError> {
        let pg_class = CatalogRelation::open(
            pg_sys::RelationRelationId,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let mut scan = pg_class.begin_scan(
            pg_sys::ClassOidIndexId.into(),
            true,
            snapshot,
            [CatalogScanKey::oid_eq(pg_sys::Anum_pg_class_oid as _, oid)],
        )?;
        let Some(tuple) = scan.get_next()? else {
            return Ok(None);
        };

        // SAFETY: the tuple comes from a live scan of pg_class, and its fixed
        // fields remain valid until the scan advances or is dropped.
        let class =
            unsafe { &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class) };
        Ok(Some(Self::from_class(class)))
    }

    fn from_class(class: &pg_sys::FormData_pg_class) -> Self {
        Self {
            oid: class.oid,
            relkind: class.relkind,
            access_method_oid: class.relam,
            tablespace: RelationTablespace::from_catalog_oid(class.reltablespace),
        }
    }

    /// OID stored in the `pg_class` row.
    #[inline]
    pub fn oid(self) -> pg_sys::Oid {
        self.oid
    }

    /// PostgreSQL relation kind (`pg_class.relkind`).
    #[inline]
    pub fn relkind(self) -> i8 {
        self.relkind
    }

    /// Table access method OID (`pg_class.relam`).
    #[inline]
    pub fn access_method_oid(self) -> pg_sys::Oid {
        self.access_method_oid
    }

    /// Logical tablespace placement recorded by `pg_class.reltablespace`.
    #[inline]
    pub fn tablespace(self) -> RelationTablespace {
        self.tablespace
    }
}
