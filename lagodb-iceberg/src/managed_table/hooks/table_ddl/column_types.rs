//! Managed column declarations whose SQL type must match their storage semantics.

use std::{ffi::CStr, panic::AssertUnwindSafe, ptr};

use lagodb_core::diag::PgError;
use lagodb_core::hooks::{HookError, UtilityHookError};
use lagodb_core::tuple::PgType;
use pgrx::{PgSqlErrorCode, PgTryBuilder, notice, pg_sys};

use super::column_sources::ManagedColumnSources;
use crate::managed_table::catalog::numeric_type::ManagedNumeric;

pub(super) struct ManagedColumnTypes;

impl ManagedColumnTypes {
    pub(super) fn create(
        stmt: &mut pg_sys::CreateStmt,
    ) -> Result<(), UtilityHookError> {
        PgTryBuilder::new(AssertUnwindSafe(|| {
            // Match transformCreateStmt's lock order: target namespace before
            // LIKE sources and inheritance parents.
            let mut existing = pg_sys::InvalidOid;
            unsafe {
                pg_sys::RangeVarGetAndCheckCreationNamespace(
                    stmt.relation,
                    pg_sys::NoLock as _,
                    &mut existing,
                )
            };
            if stmt.if_not_exists && existing != pg_sys::InvalidOid {
                // PG owns the skip notice and extension-membership check.
                // In particular, do not open sources for this no-op.
                return Ok(());
            }
            ManagedColumnSources::prepare(stmt)?;
            for index in 0..unsafe { pg_sys::list_length(stmt.tableElts) } {
                let node = unsafe { pg_sys::list_nth(stmt.tableElts, index) }
                    .cast::<pg_sys::Node>();
                if unsafe { (*node).type_ } == pg_sys::NodeTag::T_ColumnDef {
                    let _ = unsafe { Self::column(&mut *node.cast()) }?;
                }
            }
            Ok(())
        }))
        .catch_others(|error| Err(PgError::from(error).into()))
        .execute()
    }

    pub(super) fn alter(commands: *mut pg_sys::List) -> Result<(), UtilityHookError> {
        PgTryBuilder::new(AssertUnwindSafe(|| {
            for index in 0..unsafe { pg_sys::list_length(commands) } {
                let command = unsafe {
                    &*pg_sys::list_nth(commands, index)
                        .cast::<pg_sys::AlterTableCmd>()
                };
                if command.subtype == pg_sys::AlterTableType::AT_AddColumn {
                    let Some(ty) = (unsafe {
                        Self::column(&mut *command.def.cast::<pg_sys::ColumnDef>())
                    })? else {
                        continue;
                    };
                    // Parse-tree planning rejects explicit DEFAULT. Like PG's
                    // build_column_default, also consult the declared type:
                    // a domain default need not appear in the ColumnDef. Do not
                    // unwrap domains or evaluate the expression. Schema evolution
                    // emits no Iceberg initial_default, and scans do not consume
                    // PG attmissingval, so old files would return NULL instead.
                    // SAFETY: column() resolved this catalog OID on the backend
                    // thread; the expression tree lives in the DDL context.
                    if !unsafe { pg_sys::get_typdefault(ty.oid) }.is_null() {
                        return Err(HookError::with_code(
                            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                            "ALTER TABLE ADD COLUMN with DEFAULT is not supported for Iceberg relations",
                        ));
                    }
                }
            }
            Ok(())
        }))
        .catch_others(|error| Err(PgError::from(error).into()))
        .execute()
    }

    /// Return the admitted type so ALTER can check its default without resolving
    /// the declaration again. Pseudo-types remain PostgreSQL's responsibility.
    unsafe fn column(
        column: &mut pg_sys::ColumnDef,
    ) -> Result<Option<PgType>, UtilityHookError> {
        // Serial pseudo-types are resolved by transformCreateStmt later.
        if column.typeName.is_null() {
            return Ok(None);
        }
        let oid = unsafe {
            pg_sys::LookupTypeNameOid(ptr::null_mut(), column.typeName, true)
        };
        if oid == pg_sys::InvalidOid {
            return Ok(None);
        }
        let (mut oid, mut typmod) = (oid, -1);
        unsafe {
            pg_sys::typenameTypeIdAndMod(
                ptr::null_mut(),
                column.typeName,
                &mut oid,
                &mut typmod,
            )
        };
        let mut ty = PgType::new(oid, typmod);
        if let Some(replacement) = Self::rewrite(ty)? {
            column.typeName = unsafe {
                pg_sys::makeTypeNameFromOid(replacement.oid, replacement.typmod)
            };
            let name = unsafe { CStr::from_ptr(column.colname) };
            notice!(
                "column \"{}\" uses double precision for numeric outside Iceberg decimal capacity",
                name.to_string_lossy()
            );
            ty = replacement;
        }
        Ok(Some(ty))
    }

    /// Normalize plain numeric and array declarations. Domains and composites
    /// belong to the user and must already have representable members.
    fn rewrite(ty: PgType) -> Result<Option<PgType>, UtilityHookError> {
        let base = ty.base();
        // getBaseTypeAndTypmod changes the OID only for domains. Replacing one
        // would discard its identity, defaults and constraints before PG sees
        // the column. Reject such rewrites even for constraint-free domains:
        // copying constraints cannot preserve identity or ALTER DOMAIN behavior.
        if ty.oid != base.oid {
            if Self::needs_rewrite(base) {
                return Err(HookError::with_code(
                    PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "domain contains numeric outside Iceberg decimal capacity; explicitly declare a domain with compatible numeric typmods or double precision members, or use a non-domain column type",
                ));
            }
            return Ok(None);
        }
        if base.oid == pg_sys::NUMERICOID
            && ManagedNumeric::new(base.typmod).uses_double()
        {
            return Ok(Some(PgType::new(pg_sys::FLOAT8OID, -1)));
        }
        if let Some(element) = base.element() {
            return Self::rewrite(element).map(|rewritten| {
                rewritten.map(|element| {
                    PgType::new(
                        unsafe { pg_sys::get_array_type(element.oid) },
                        element.typmod,
                    )
                })
            });
        }
        if base.is_composite() && Self::needs_rewrite(base) {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "composite contains numeric outside Iceberg decimal capacity; explicitly create a composite type with compatible numeric typmods or double precision members",
            ));
        }
        Ok(None)
    }

    /// Whether any numeric leaf needs a different SQL representation. Used for
    /// composite admission and before copying or inheriting a declaration.
    pub(super) fn needs_rewrite(ty: PgType) -> bool {
        let base = ty.base();
        if base.oid == pg_sys::NUMERICOID {
            return ManagedNumeric::new(base.typmod).uses_double();
        }
        if let Some(element) = base.element() {
            return Self::needs_rewrite(element);
        }
        if base.is_composite() {
            return base
                .composite_fields()
                .iter()
                .any(|field| Self::needs_rewrite(field.pg_type));
        }
        false
    }
}
