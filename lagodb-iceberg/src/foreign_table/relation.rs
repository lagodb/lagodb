//! Resolution of a PostgreSQL foreign table into one Iceberg catalog table.

use std::rc::Rc;

use iceberg_lite::spec::TableMetadata;
use iceberg_lite::table::Table;
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::catalog::{BoundCatalogTable, CatalogSessionRequest, RemoteTableKey};
use super::error::IcebergFdwError;
use super::executor_cache::ExecutorForeignTableCache;
use super::options::ForeignTableIdentity;
use super::source_identity::PlanSourceIdentity;
use super::transaction::{ForeignTableReadView, ForeignTransaction};
use crate::scan::IcebergReadSnapshot;

mod target;

pub(super) use target::RemoteTableTarget;

/// A resolved catalog session or staged generation. Metadata consumers use its
/// table directly; scans bind command-visible data through [`ForeignTransaction`].
pub(crate) struct ResolvedForeignTableBinding {
    identity: ForeignTableIdentity,
    catalog_table: BoundCatalogTable,
}

impl ResolvedForeignTableBinding {
    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_binding(relation_oid, effective_user, None)
    }

    /// Resolve local options on every bind, but select the remote Table only
    /// once for this executor and catalog/user/table identity.
    pub(crate) fn resolve_for_execution(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: &ExecutorHandle<'_>,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_binding(relation_oid, effective_user, Some(executor))
    }

    fn resolve_binding(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: planner/executor contexts supply a live foreign-table OID;
        // the returned catalog object is consumed before this method returns.
        let foreign_table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let identity = ForeignTableIdentity::from_foreign_table(foreign_table)?;
        Self::bind_catalog_table(
            relation_oid,
            foreign_table.serverid,
            effective_user,
            identity,
            executor,
        )
    }

    pub(crate) fn load(
        relation_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
    ) -> Result<Self, IcebergFdwError> {
        Self::bind_catalog_table(
            relation_oid,
            server_oid,
            effective_user,
            identity,
            None,
        )
    }

    fn bind_catalog_table(
        relation_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        let request = CatalogSessionRequest::resolve(server_oid, effective_user)?;
        let key =
            RemoteTableKey::new(request.identity().clone(), identity.table().clone());
        if let Some((catalog, table)) = ForeignTransaction::staged_table(
            relation_oid,
            &key,
            request.runtime_config(),
        )? {
            return Ok(Self {
                identity,
                catalog_table: BoundCatalogTable::new(
                    key,
                    Rc::new(request.connect(Some(catalog))?),
                    table,
                ),
            });
        }
        let reusable_catalog = ForeignTransaction::catalog_from_transaction(
            request.identity(),
            request.runtime_config(),
        )?;
        let (catalog, table) = match executor {
            Some(executor) => ExecutorForeignTableCache::acquire(executor)
                .bind_table(&key, request, reusable_catalog)?,
            None => {
                let catalog = Rc::new(request.connect(reusable_catalog)?);
                let table = catalog.as_catalog().load_table(key.table())?;
                (catalog, table)
            }
        };
        Ok(Self {
            identity,
            catalog_table: BoundCatalogTable::new(key, catalog, table),
        })
    }

    pub(crate) fn key(&self) -> &RemoteTableKey {
        self.catalog_table.key()
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn table(&self) -> &Table {
        self.catalog_table.table()
    }

    pub(crate) fn into_catalog_table(self) -> BoundCatalogTable {
        self.catalog_table
    }

    /// Reject an execution whose foreign-table options no longer name the
    /// table the plan was built for.
    pub(crate) fn ensure_planned_identity(
        &self,
        planned: &ForeignTableIdentity,
    ) -> Result<(), IcebergFdwError> {
        if &self.identity != planned {
            return Err(IcebergFdwError::PlanIdentityChanged);
        }
        Ok(())
    }

    /// Bind one execution of a planned scan to its transaction-visible read.
    ///
    /// Parallel workers can reopen only catalog-published foreign state, and a
    /// cached plan must not retain planner-time worker eligibility: INSERT can
    /// add local batches between executions of one generic plan without
    /// invalidating it. Eligibility is therefore captured here, per execution.
    /// Projection and required-column schema validation stay with the caller.
    pub(crate) fn into_execution_read(
        self,
        read_command_id: Option<pg_sys::CommandId>,
        planned_source: Option<&PlanSourceIdentity>,
    ) -> Result<ForeignExecutionRead, IcebergFdwError> {
        let worker_reconstructible =
            !ForeignTransaction::has_local_actions(self.key());
        let view =
            ForeignTransaction::scan_view(self.catalog_table, read_command_id)?;
        let generation = PlanSourceIdentity::from_metadata(view.metadata());
        if planned_source.is_some_and(|planned| planned != &generation) {
            return Err(IcebergFdwError::PlanSourceChanged);
        }
        Ok(ForeignExecutionRead {
            view,
            generation,
            worker_reconstructible,
        })
    }
}

/// A planned foreign read bound to one execution: the transaction-visible
/// view, the source generation it was checked against, and whether a parallel
/// worker can reconstruct it from published catalog state.
pub(crate) struct ForeignExecutionRead {
    view: ForeignTableReadView,
    generation: PlanSourceIdentity,
    worker_reconstructible: bool,
}

impl ForeignExecutionRead {
    pub(crate) fn metadata(&self) -> &TableMetadata {
        self.view.metadata()
    }

    pub(crate) fn worker_reconstructible(&self) -> bool {
        self.worker_reconstructible
    }

    pub(crate) fn into_parts(self) -> (IcebergReadSnapshot, PlanSourceIdentity) {
        let (_key, table, delta) = self.view.into_parts();
        (IcebergReadSnapshot::new(table, delta), self.generation)
    }
}
