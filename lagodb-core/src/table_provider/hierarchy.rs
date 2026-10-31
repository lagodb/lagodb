//! A table provider's relations only share a hierarchy with the same provider.
//!
//! PostgreSQL's maintenance and scan paths treat a hierarchy as one unit, while
//! the VACUUM FULL probe and Custom ModifyTable classify it from the relation a
//! command names. Both rely on every member having that relation's owner, so
//! the guard registers with the provider and has no opt-out. Provider-only
//! INHERITS keeps working unless the provider's validate_inheritance_edge()
//! rejects the edge.
//!
//! PostgreSQL's StoreCatalogInheritance1() emits pg_inherits OAT_POST_ALTER after
//! acquiring the parent/child locks and recording the relationship, but before
//! ATTACH clones indexes, triggers, or foreign keys. The same event covers
//! CREATE TABLE PARTITION OF. Enforce topology here instead of taking child
//! locks in a ProcessUtility pre-hook. Returning an error rolls back the DDL.
//!
//! SET ACCESS METHOD changes the same property without touching pg_inherits, and
//! has no provider lifecycle, so it is rejected for both directions.

use std::ffi::{CStr, c_char};
use std::marker::PhantomData;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::LagodbTableProvider;
use crate::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, RelationCatalogEntry,
    alter_table_lookup_relation,
};
use crate::hooks::{
    AlterTableStmtNode, HookError, OBJECT_ACCESS_POST_ALTER, ObjectAccessEvent,
    ObjectAccessFilter, ObjectAccessHook, ObjectAccessHookError, PostUtilityContext,
    UtilityHook, UtilityHookError, UtilityNode, register_object_access_hook,
    register_utility_hook,
};

pub(super) struct ProviderHierarchyGuard<P>(PhantomData<fn() -> P>);

impl<P: LagodbTableProvider> ProviderHierarchyGuard<P> {
    // pgrx does not export pg_inherits' catalog definitions. These identifiers
    // and attribute numbers come from PostgreSQL's src/include/catalog/pg_inherits.h;
    // re-audit them with the PostgreSQL-major-version boundary. Read attributes
    // through CatalogRelation rather than reproducing the C tuple layout.
    const CATALOG_OID: pg_sys::Oid = pg_sys::Oid::from_u32(2611);
    const CHILD_INDEX_OID: pg_sys::Oid = pg_sys::Oid::from_u32(2680);
    const CHILD_ATTRIBUTE: pg_sys::AttrNumber = 1;
    const PARENT_ATTRIBUTE: pg_sys::AttrNumber = 2;

    pub(super) fn register() {
        register_object_access_hook(Box::new(Self(PhantomData)));
        register_utility_hook(
            pg_sys::NodeTag::T_AlterTableStmt,
            Box::new(Self(PhantomData)),
        );
    }

    fn owns(access_method: pg_sys::Oid) -> bool {
        P::access_method_oid() == Some(access_method)
    }

    fn owns_relation(relation: pg_sys::Oid) -> Result<bool, ObjectAccessHookError> {
        // The event can precede a command-counter increment, including during
        // CREATE. PostgreSQL holds the target locks and both relations exist.
        let relation =
            RelationCatalogEntry::find(relation, CatalogSnapshot::SelfVisible)?
                .expect("PostgreSQL inheritance event identifies a live relation");
        Ok(Self::owns(relation.access_method_oid()))
    }

    /// Removal also emits OAT_POST_ALTER with the same identifiers. Inspect the
    /// command's own catalog changes so unlinking a relation is allowed. Do not
    /// use relispartition: ATTACH sets it after this creation event.
    fn edge_exists(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<bool, ObjectAccessHookError> {
        let inherits =
            CatalogRelation::open(Self::CATALOG_OID, pg_sys::AccessShareLock as _)?;
        let mut scan = inherits.begin_scan(
            Self::CHILD_INDEX_OID,
            true,
            CatalogSnapshot::SelfVisible,
            [CatalogScanKey::oid_eq(Self::CHILD_ATTRIBUTE, child)],
        )?;
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: this tuple belongs to the live pg_inherits scan and is
            // read before advancing that scan.
            let inherited_parent =
                unsafe { inherits.get_attr(tuple, Self::PARENT_ATTRIBUTE) }
                    .expect("pg_inherits.inhparent is not null");
            if inherited_parent.value() == u32::from(parent) as usize {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn validate_edge(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<(), ObjectAccessHookError> {
        let child_owned = Self::owns_relation(child)?;
        let parent_owned = Self::owns_relation(parent)?;
        if !child_owned && !parent_owned {
            return Ok(());
        }
        if !Self::edge_exists(child, parent)? {
            return Ok(());
        }
        if child_owned != parent_owned {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "table provider relations cannot share an inheritance or partition hierarchy with another access method",
            ));
        }
        P::validate_inheritance_edge(child, parent)
    }

    /// The access method named by the statement's SET ACCESS METHOD, null for
    /// DEFAULT. PostgreSQL rejects a second such subcommand in one statement.
    fn set_access_method_target(
        stmt: &pg_sys::AlterTableStmt,
    ) -> Option<*const c_char> {
        (0..unsafe { pg_sys::list_length(stmt.cmds) }).find_map(|index| {
            // SAFETY: an ALTER TABLE statement's list holds AlterTableCmd nodes.
            let command = unsafe {
                &*pg_sys::list_nth(stmt.cmds, index).cast::<pg_sys::AlterTableCmd>()
            };
            (command.subtype == pg_sys::AlterTableType::AT_SetAccessMethod)
                .then_some(command.name.cast_const())
        })
    }

    /// Mirrors ATPrepSetAccessMethod: DEFAULT resets a partitioned table's
    /// catalogued access method instead of selecting the configured default.
    fn names_provider(target: *const c_char, relation: RelationCatalogEntry) -> bool {
        let name = if !target.is_null() {
            target
        } else if relation.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
            return false;
        } else {
            // SAFETY: PostgreSQL's boot value keeps this GUC set.
            unsafe { pg_sys::default_table_access_method }.cast_const()
        };
        // SAFETY: both sources are NUL-terminated strings that outlive this call.
        unsafe { CStr::from_ptr(name) == P::ACCESS_METHOD_NAME }
    }
}

impl<P: LagodbTableProvider> ObjectAccessHook for ProviderHierarchyGuard<P> {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_ALTER).for_class(Self::CATALOG_OID)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        match event {
            ObjectAccessEvent::PostAlter { object_id, arg, .. } => {
                // PostgreSQL's inheritance hook supplies child as object_id and
                // parent as auxiliary_id for both creation and removal.
                let parent = arg
                    .expect(
                        "PostgreSQL inheritance event supplies post-alter arguments",
                    )
                    .auxiliary_id;
                Self::validate_edge(*object_id, parent)
            }
            _ => Ok(()),
        }
    }
}

impl<P: LagodbTableProvider> UtilityHook for ProviderHierarchyGuard<P> {
    fn name(&self) -> &'static str {
        "table provider hierarchy guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast_mut::<AlterTableStmtNode>()
            .expect("Hook registered for T_AlterTableStmt");
        if stmt.objtype != pg_sys::ObjectType::OBJECT_TABLE
            && stmt.objtype != pg_sys::ObjectType::OBJECT_MATVIEW
        {
            return Ok(());
        }
        let Some(target) = Self::set_access_method_target(stmt) else {
            return Ok(());
        };
        // Outside this provider's databases no relation can use the access
        // method, and PostgreSQL reports a name that does not resolve.
        let Some(provider) = P::access_method_oid() else {
            return Ok(());
        };

        // SAFETY: `stmt` is the live statement of this utility call.
        let oid = unsafe {
            alter_table_lookup_relation(stmt, pg_sys::AccessExclusiveLock as _)
        }?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }
        let relation = RelationCatalogEntry::find_cached(oid)
            .expect("AccessExclusiveLock keeps the ALTER TABLE target's row");
        if relation.access_method_oid() == provider
            || Self::names_provider(target, relation)
        {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "ALTER TABLE SET ACCESS METHOD is not supported for table provider relations",
            ));
        }
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}
