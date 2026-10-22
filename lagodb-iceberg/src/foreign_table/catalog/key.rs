//! Stable identity for one remote Iceberg table under PostgreSQL access rules.

use std::cmp::Ordering;

use iceberg_lite::catalog::TableIdent;

use super::super::options::ForeignIdentifier;
use super::connection::CatalogAccessIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableKey {
    catalog: CatalogAccessIdentity,
    table: TableIdent,
}

impl PartialOrd for RemoteTableKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RemoteTableKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            u32::from(self.catalog.server_oid),
            u32::from(self.catalog.effective_user),
            &self.table,
        )
            .cmp(&(
                u32::from(other.catalog.server_oid),
                u32::from(other.catalog.effective_user),
                &other.table,
            ))
    }
}

impl RemoteTableKey {
    pub(crate) fn new(catalog: CatalogAccessIdentity, table: TableIdent) -> Self {
        Self { catalog, table }
    }

    pub(crate) fn catalog_identity(&self) -> &CatalogAccessIdentity {
        &self.catalog
    }

    pub(crate) fn table(&self) -> &TableIdent {
        &self.table
    }

    pub(crate) fn publication_name(&self) -> String {
        ForeignIdentifier::table_path(&self.table)
    }
}
