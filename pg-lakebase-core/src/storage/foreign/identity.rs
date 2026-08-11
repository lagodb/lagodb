//! PostgreSQL catalog identity for one effective storage context.

use pgrx::pg_sys;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StorageIdentity {
    database_oid: pg_sys::Oid,
    server_oid: pg_sys::Oid,
    umid: pg_sys::Oid,
}

impl StorageIdentity {
    pub(crate) const fn new(
        database_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        umid: pg_sys::Oid,
    ) -> Self {
        Self {
            database_oid,
            server_oid,
            umid,
        }
    }

    pub const fn database_oid(&self) -> pg_sys::Oid {
        self.database_oid
    }
    pub const fn server_oid(&self) -> pg_sys::Oid {
        self.server_oid
    }
    pub const fn umid(&self) -> pg_sys::Oid {
        self.umid
    }
}
