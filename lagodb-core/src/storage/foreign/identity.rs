//! PostgreSQL catalog identity for one effective storage context.

use pgrx::pg_sys;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StorageIdentity {
    database_oid: pg_sys::Oid,
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    mapping_oid: Option<pg_sys::Oid>,
}

impl StorageIdentity {
    pub(crate) const fn new(
        database_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        mapping_oid: Option<pg_sys::Oid>,
    ) -> Self {
        Self {
            database_oid,
            server_oid,
            effective_user,
            mapping_oid,
        }
    }

    pub const fn database_oid(&self) -> pg_sys::Oid {
        self.database_oid
    }
    pub const fn server_oid(&self) -> pg_sys::Oid {
        self.server_oid
    }
    pub const fn effective_user(&self) -> pg_sys::Oid {
        self.effective_user
    }
    pub const fn mapping_oid(&self) -> Option<pg_sys::Oid> {
        self.mapping_oid
    }
}
