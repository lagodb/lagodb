//! Privilege and ownership checks on PostgreSQL catalog objects.
//!
//! PostgreSQL 16 replaced the per-class `pg_*_aclcheck` and
//! `pg_*_ownercheck` functions with `object_aclcheck` and
//! `object_ownercheck`, keyed by the object's catalog class. This type keeps
//! that difference in one place. Relation ACLs stay on `pg_class_aclcheck` in
//! every version: `object_aclmask` rejects `RelationRelationId`, because
//! `pg_class_aclmask` also applies predefined-role privileges.

use pgrx::pg_sys;

/// A catalog object whose ACL or owner LagoDB checks.
#[derive(Debug, Clone, Copy)]
pub enum CatalogObject {
    Database(pg_sys::Oid),
    ForeignServer(pg_sys::Oid),
    Relation(pg_sys::Oid),
    Tablespace(pg_sys::Oid),
    Type(pg_sys::Oid),
}

impl CatalogObject {
    /// Check `mode` for `role` on this object, as PostgreSQL's aclcheck does.
    pub fn aclcheck(
        self,
        role: pg_sys::Oid,
        mode: pg_sys::AclMode,
    ) -> pg_sys::AclResult::Type {
        // SAFETY: the catalog functions accept any OID and report a missing
        // object through PostgreSQL ERROR, which pgrx's guarded bindings
        // propagate.
        unsafe {
            match self {
                Self::Relation(oid) => pg_sys::pg_class_aclcheck(oid, role, mode),
                #[cfg(feature = "pg15")]
                Self::Database(oid) => pg_sys::pg_database_aclcheck(oid, role, mode),
                #[cfg(feature = "pg15")]
                Self::ForeignServer(oid) => {
                    pg_sys::pg_foreign_server_aclcheck(oid, role, mode)
                }
                #[cfg(feature = "pg15")]
                Self::Tablespace(oid) => {
                    pg_sys::pg_tablespace_aclcheck(oid, role, mode)
                }
                #[cfg(feature = "pg15")]
                Self::Type(oid) => pg_sys::pg_type_aclcheck(oid, role, mode),
                #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
                Self::Database(oid)
                | Self::ForeignServer(oid)
                | Self::Tablespace(oid)
                | Self::Type(oid) => {
                    pg_sys::object_aclcheck(self.class_id(), oid, role, mode)
                }
            }
        }
    }

    /// Whether `role` passes `mode` on this object.
    #[inline]
    pub fn has_privilege(self, role: pg_sys::Oid, mode: pg_sys::AclMode) -> bool {
        self.aclcheck(role, mode) == pg_sys::AclResult::ACLCHECK_OK
    }

    /// Whether `role` owns this object or is a member of its owner role.
    pub fn is_owned_by(self, role: pg_sys::Oid) -> bool {
        // SAFETY: as in `aclcheck`.
        #[cfg(feature = "pg15")]
        unsafe {
            match self {
                Self::Database(oid) => pg_sys::pg_database_ownercheck(oid, role),
                Self::ForeignServer(oid) => {
                    pg_sys::pg_foreign_server_ownercheck(oid, role)
                }
                Self::Relation(oid) => pg_sys::pg_class_ownercheck(oid, role),
                Self::Tablespace(oid) => pg_sys::pg_tablespace_ownercheck(oid, role),
                Self::Type(oid) => pg_sys::pg_type_ownercheck(oid, role),
            }
        }
        // SAFETY: as in `aclcheck`.
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        unsafe {
            pg_sys::object_ownercheck(self.class_id(), self.oid(), role)
        }
    }

    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    const fn class_id(self) -> pg_sys::Oid {
        match self {
            Self::Database(_) => pg_sys::DatabaseRelationId,
            Self::ForeignServer(_) => pg_sys::ForeignServerRelationId,
            Self::Relation(_) => pg_sys::RelationRelationId,
            Self::Tablespace(_) => pg_sys::TableSpaceRelationId,
            Self::Type(_) => pg_sys::TypeRelationId,
        }
    }

    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    const fn oid(self) -> pg_sys::Oid {
        match self {
            Self::Database(oid)
            | Self::ForeignServer(oid)
            | Self::Relation(oid)
            | Self::Tablespace(oid)
            | Self::Type(oid) => oid,
        }
    }
}
