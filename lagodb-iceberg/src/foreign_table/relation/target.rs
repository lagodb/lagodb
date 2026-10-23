//! Catalog access for an Iceberg table target that may not exist yet.

use iceberg_lite::ErrorKind;
use iceberg_lite::catalog::TableCreation;
use iceberg_lite::spec::Schema;
use iceberg_lite::table::Table;
use pgrx::pg_sys;

use super::super::catalog::{CatalogSession, CatalogSessionRequest, RemoteTableKey};
use super::super::error::IcebergFdwError;
use super::super::options::ForeignTableIdentity;
use super::super::transaction::ForeignTransaction;

pub(in crate::foreign_table) struct RemoteTableTarget {
    session: CatalogSession,
    key: RemoteTableKey,
    identity: ForeignTableIdentity,
}

impl RemoteTableTarget {
    pub(in crate::foreign_table) fn connect(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
    ) -> Result<Self, IcebergFdwError> {
        let request = CatalogSessionRequest::resolve(server_oid, effective_user)?;
        let key =
            RemoteTableKey::new(request.identity().clone(), identity.table().clone());
        let reusable = ForeignTransaction::catalog_from_transaction(
            request.identity(),
            request.runtime_config(),
        )?;
        Ok(Self {
            session: request.connect(reusable)?,
            key,
            identity,
        })
    }

    pub(in crate::foreign_table) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(in crate::foreign_table) fn load_published(
        &self,
    ) -> Result<Option<Table>, IcebergFdwError> {
        let catalog = self.session.as_catalog();
        match catalog.load_table(self.identity.table()) {
            Ok(table) => Ok(Some(table)),
            Err(error) if error.kind() == ErrorKind::TableNotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(in crate::foreign_table) fn stage_create(
        &self,
        relation_oid: pg_sys::Oid,
        schema: Schema,
    ) -> Result<Table, IcebergFdwError> {
        if self.identity.schema_id().is_some() {
            return Err(IcebergFdwError::invalid_option(
                "schema_id",
                "cannot be specified when creating a missing Iceberg table",
            ));
        }
        let creation = TableCreation::builder()
            .name(self.identity.table().name().to_owned())
            .schema(schema)
            .build();
        ForeignTransaction::stage_create(
            relation_oid,
            &self.key,
            &self.session,
            self.identity.table().namespace(),
            creation,
        )
    }
}
