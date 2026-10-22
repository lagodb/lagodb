//! Catalog identities, sessions, adapters and publication requests.

mod connection;
mod key;
pub mod rest;
mod session;
mod sqlite;

pub(crate) use connection::{
    CatalogAccessIdentity, CatalogConnection, CatalogRuntimeConfig,
};
pub(crate) use key::RemoteTableKey;
pub(crate) use session::{
    BoundCatalogTable, CatalogChange, CatalogSession, CatalogSessionRequest,
    ForeignCatalog, PreparedCatalogPublication,
};
