//! DDL-only admission and expansion of managed column sources.
//!
//! Basic LIKE follows PostgreSQL's transformTableLikeClause semantics.
//! Unchanged LIKE clauses stay with PostgreSQL, including all INCLUDING
//! processing. Inheritance is never rewritten: PG requires matching types.

use std::{ffi::CStr, ptr, slice};

use lagodb_core::catalog::range_var_get_relid;
use lagodb_core::handles::RelationGuard;
use lagodb_core::hooks::{HookError, UtilityHookError};
use lagodb_core::tuple::PgType;
use pgrx::{PgSqlErrorCode, pg_sys};

use super::column_types::ManagedColumnTypes;

pub(super) struct ManagedColumnSources;

impl ManagedColumnSources {
    /// Called inside ManagedColumnTypes' PG error-catching boundary, before
    /// normalizing copied column declarations.
    pub(super) fn prepare(
        stmt: &mut pg_sys::CreateStmt,
    ) -> Result<(), UtilityHookError> {
        let mut elements = ptr::null_mut();
        for index in 0..unsafe { pg_sys::list_length(stmt.tableElts) } {
            let node = unsafe { pg_sys::list_nth(stmt.tableElts, index) }
                .cast::<pg_sys::Node>();
            if unsafe { (*node).type_ } == pg_sys::NodeTag::T_TableLikeClause {
                let clause = unsafe { &*node.cast::<pg_sys::TableLikeClause>() };
                let source = unsafe {
                    ColumnSource::open(clause.relation, pg_sys::AccessShareLock as _)
                }?;
                source.check_like()?;
                if source.needs_rewrite() {
                    if clause.options != 0 {
                        return Err(HookError::with_code(
                            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                            "LIKE with INCLUDING options cannot change numeric column types; use basic LIKE or declare double precision in the source",
                        ));
                    }
                    elements = unsafe {
                        pg_sys::list_concat(elements, source.column_defs())
                    };
                    continue;
                }
            }
            elements = unsafe { pg_sys::lappend(elements, node.cast()) };
        }
        for index in 0..unsafe { pg_sys::list_length(stmt.inhRelations) } {
            let parent = unsafe { pg_sys::list_nth(stmt.inhRelations, index) }.cast();
            // PG opens LIKE sources during transformation, then inheritance
            // parents in DefineRelation. Match that order and parent lock.
            // Retain the lock through MergeAttributes and transaction end.
            let source = unsafe {
                ColumnSource::open(parent, pg_sys::ShareUpdateExclusiveLock as _)
            }?;
            source.check_inherited()?;
        }
        stmt.tableElts = elements;
        Ok(())
    }
}

/// The relcache reference is scoped to inspection; its lock lasts until the
/// transaction ends, exactly as for PG's LIKE expansion and inheritance.
struct ColumnSource(RelationGuard);

impl ColumnSource {
    /// # Safety
    /// `range` belongs to the live CREATE parse tree on the backend thread.
    unsafe fn open(
        range: *mut pg_sys::RangeVar,
        lock: pg_sys::LOCKMODE,
    ) -> Result<Self, UtilityHookError> {
        let oid = unsafe { range_var_get_relid(range, lock, false) }?;
        Ok(Self(RelationGuard::open_relation_retain_lock(
            oid,
            pg_sys::NoLock as _,
        )?))
    }

    fn attributes(&self) -> &[pg_sys::FormData_pg_attribute] {
        let desc = self.0.as_handle().tuple_desc();
        // The guard pins the descriptor and PG supplies its complete array.
        unsafe {
            slice::from_raw_parts((*desc).attrs.as_ptr(), (*desc).natts as usize)
        }
    }

    fn needs_rewrite(&self) -> bool {
        self.attributes()
            .iter()
            .filter(|attr| !attr.attisdropped)
            .any(|attr| {
                ManagedColumnTypes::needs_rewrite(PgType::new(
                    attr.atttypid,
                    attr.atttypmod,
                ))
            })
    }

    fn check_like(&self) -> Result<(), UtilityHookError> {
        let rel = self.0.as_handle();
        let kind = rel.relkind() as u8;
        if !matches!(
            kind,
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_VIEW
                | pg_sys::RELKIND_MATVIEW
                | pg_sys::RELKIND_COMPOSITE_TYPE
                | pg_sys::RELKIND_FOREIGN_TABLE
                | pg_sys::RELKIND_PARTITIONED_TABLE
        ) {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
                "relation is not a supported LIKE source",
            ));
        }
        // Match transformTableLikeClause before reading any source types.
        unsafe {
            let (acl, object) = if kind == pg_sys::RELKIND_COMPOSITE_TYPE {
                (
                    pg_sys::object_aclcheck(
                        pg_sys::TypeRelationId,
                        (*(*rel.as_raw()).rd_rel).reltype,
                        pg_sys::GetUserId(),
                        pg_sys::ACL_USAGE as pg_sys::AclMode,
                    ),
                    pg_sys::ObjectType::OBJECT_TYPE,
                )
            } else {
                (
                    pg_sys::pg_class_aclcheck(
                        rel.oid(),
                        pg_sys::GetUserId(),
                        pg_sys::ACL_SELECT as pg_sys::AclMode,
                    ),
                    pg_sys::get_relkind_objtype(rel.relkind()),
                )
            };
            if acl != pg_sys::AclResult::ACLCHECK_OK {
                pg_sys::aclcheck_error(
                    acl,
                    object,
                    rel.relation_name().as_c_str().as_ptr(),
                );
            }
        }
        Ok(())
    }

    fn check_inherited(&self) -> Result<(), UtilityHookError> {
        let rel = self.0.as_handle();
        // PG owns the remaining inheritance topology checks. Check ownership
        // before inspecting the parent's type tree, as MergeAttributes does.
        unsafe {
            if !pg_sys::object_ownercheck(
                pg_sys::RelationRelationId,
                rel.oid(),
                pg_sys::GetUserId(),
            ) {
                pg_sys::aclcheck_error(
                    pg_sys::AclResult::ACLCHECK_NOT_OWNER,
                    pg_sys::get_relkind_objtype(rel.relkind()),
                    rel.relation_name().as_c_str().as_ptr(),
                );
            }
        }
        for attr in self.attributes().iter().filter(|attr| !attr.attisdropped) {
            if ManagedColumnTypes::needs_rewrite(PgType::new(
                attr.atttypid,
                attr.atttypmod,
            )) {
                let name = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) };
                return Err(HookError::with_code(
                    PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "inherited column \"{}\" requires numeric type conversion; declare double precision in the parent before creating an Iceberg child",
                        name.to_string_lossy()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Copy basic LIKE columns, preserving physical order, typmod, collation
    /// and NOT NULL; do not inherit defaults, identity, storage or compression.
    unsafe fn column_defs(&self) -> *mut pg_sys::List {
        let mut columns = ptr::null_mut();
        for attr in self.attributes().iter().filter(|attr| !attr.attisdropped) {
            let column = unsafe {
                pg_sys::makeColumnDef(
                    attr.attname.data.as_ptr(),
                    attr.atttypid,
                    attr.atttypmod,
                    attr.attcollation,
                )
            };
            unsafe {
                (*column).is_not_null = attr.attnotnull;
                columns = pg_sys::lappend(columns, column.cast());
            }
        }
        columns
    }
}
