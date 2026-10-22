//! Catalog selection and owned publication requests for the FDW.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::catalog::rest::{PreparedRestCommit, RestCatalog};
use iceberg_lite::catalog::sqlite::{PreparedSqliteCommit, SqliteCatalog};
use iceberg_lite::table::Table;
use iceberg_lite::transaction::PreparedTableCommit;
use iceberg_lite::transaction::StagedTableCreate;
use iceberg_lite::{Catalog, NamespaceIdent, Result as IcebergResult, TableCreation};
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::connection::{
    CatalogAccessIdentity, CatalogConnection, CatalogRuntimeConfig,
};
use super::key::RemoteTableKey;

/// PostgreSQL catalog configuration resolved for one server and effective user.
///
/// The request remains disconnected until the caller has checked transaction and
/// executor-local reuse. This keeps configuration resolution identical across
/// DDL, planning, execution and maintenance bindings.
pub(crate) struct CatalogSessionRequest {
    connection: CatalogConnection,
    identity: CatalogAccessIdentity,
    runtime_config: CatalogRuntimeConfig,
}

impl CatalogSessionRequest {
    pub(crate) fn resolve(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        let connection = CatalogConnection::resolve(server_oid, effective_user)?;
        let identity = connection.access_identity();
        let runtime_config = connection.runtime_config();
        Ok(Self {
            connection,
            identity,
            runtime_config,
        })
    }

    pub(crate) fn identity(&self) -> &CatalogAccessIdentity {
        &self.identity
    }

    pub(crate) fn runtime_config(&self) -> &CatalogRuntimeConfig {
        &self.runtime_config
    }

    pub(crate) fn connect(
        self,
        reusable: Option<ForeignCatalog>,
    ) -> Result<CatalogSession, IcebergFdwError> {
        let catalog = match reusable {
            Some(catalog) => catalog,
            None => self.connection.connect()?,
        };
        Ok(CatalogSession {
            identity: self.identity,
            runtime_config: self.runtime_config,
            catalog,
        })
    }
}

/// One connected catalog together with the PostgreSQL configuration identity
/// that admitted it.
pub(crate) struct CatalogSession {
    pub(crate) identity: CatalogAccessIdentity,
    pub(crate) runtime_config: CatalogRuntimeConfig,
    pub(crate) catalog: ForeignCatalog,
}

impl CatalogSession {
    pub(crate) fn as_catalog(&self) -> &dyn Catalog {
        self.catalog.as_catalog()
    }
}

/// One loaded table paired with the exact catalog session and remote identity
/// that selected it.
pub(crate) struct BoundCatalogTable {
    key: RemoteTableKey,
    session: Rc<CatalogSession>,
    base: Table,
}

impl BoundCatalogTable {
    pub(crate) fn new(
        key: RemoteTableKey,
        session: Rc<CatalogSession>,
        base: Table,
    ) -> Self {
        Self { key, session, base }
    }

    pub(crate) fn key(&self) -> &RemoteTableKey {
        &self.key
    }

    pub(crate) fn table(&self) -> &Table {
        &self.base
    }

    pub(crate) fn into_parts(self) -> (RemoteTableKey, Rc<CatalogSession>, Table) {
        (self.key, self.session, self.base)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ForeignCatalog {
    Rest(Arc<RestCatalog>),
    Sqlite(SqliteCatalog),
}

impl ForeignCatalog {
    pub(crate) fn stage_create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<StagedTableCreate> {
        match self {
            Self::Rest(catalog) => catalog.stage_create_table(namespace, creation),
            Self::Sqlite(catalog) => catalog.stage_create_table(namespace, creation),
        }
    }

    /// Reuse a write transaction's SQLite connection without its table view.
    /// REST clients are shared only within an executor, never acquired from
    /// transaction state that can outlive that executor.
    pub(crate) fn reuse_transaction_connection(&self) -> Option<Self> {
        match self {
            Self::Rest(_) => None,
            Self::Sqlite(catalog) => Some(Self::Sqlite(catalog.clone())),
        }
    }

    pub(crate) fn as_catalog(&self) -> &dyn Catalog {
        match self {
            Self::Rest(catalog) => catalog.as_ref(),
            Self::Sqlite(catalog) => catalog,
        }
    }

    pub(crate) fn prepare_publications(
        &self,
        changes: Vec<CatalogChange>,
    ) -> IcebergResult<Vec<PreparedCatalogPublication>> {
        match self {
            Self::Rest(catalog) => {
                let mut publications = Vec::with_capacity(changes.len());
                let mut update_targets = Vec::with_capacity(changes.len());
                let mut updates = Vec::with_capacity(changes.len());
                for change in changes {
                    match change {
                        CatalogChange::StagedCreate { target, commit } => {
                            let request = catalog.prepare_table_commit(commit)?;
                            publications.push(PreparedCatalogPublication::new(
                                PreparedCatalogCommit::Rest {
                                    request: Box::new(request),
                                    catalog: Arc::clone(catalog),
                                },
                                target,
                            ));
                        }
                        CatalogChange::TableUpdate { target, commit } => {
                            update_targets.push(target);
                            updates.push(commit);
                        }
                    }
                }
                if !updates.is_empty() {
                    let request = catalog.prepare_transaction_commit(updates)?;
                    publications.push(PreparedCatalogPublication::new(
                        PreparedCatalogCommit::Rest {
                            request: Box::new(request),
                            catalog: Arc::clone(catalog),
                        },
                        update_targets.join(", "),
                    ));
                }
                Ok(publications)
            }
            Self::Sqlite(catalog) => {
                let mut targets = Vec::with_capacity(changes.len());
                let mut commits = Vec::with_capacity(changes.len());
                for change in changes {
                    let (target, commit) = change.into_parts();
                    targets.push(target);
                    commits.push(commit);
                }
                let request = PreparedCatalogCommit::Sqlite(
                    catalog.prepare_transaction_commit(commits)?,
                );
                Ok(vec![PreparedCatalogPublication::new(
                    request,
                    targets.join(", "),
                )])
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum CatalogChange {
    StagedCreate {
        target: String,
        commit: PreparedTableCommit,
    },
    TableUpdate {
        target: String,
        commit: PreparedTableCommit,
    },
}

impl CatalogChange {
    fn into_parts(self) -> (String, PreparedTableCommit) {
        match self {
            Self::StagedCreate { target, commit }
            | Self::TableUpdate { target, commit } => (target, commit),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PreparedCatalogPublication {
    request: PreparedCatalogCommit,
    targets: String,
}

impl PreparedCatalogPublication {
    fn new(request: PreparedCatalogCommit, targets: String) -> Self {
        Self { request, targets }
    }

    pub(crate) fn into_parts(self) -> (PreparedCatalogCommit, String) {
        (self.request, self.targets)
    }
}

/// Each request owns its matching catalog; callers cannot publish through a
/// different backend or release the connection before publication finishes.
#[derive(Debug)]
pub(crate) enum PreparedCatalogCommit {
    Rest {
        catalog: Arc<RestCatalog>,
        request: Box<PreparedRestCommit>,
    },
    Sqlite(PreparedSqliteCommit),
}

impl PreparedCatalogCommit {
    pub(crate) fn publish(self) -> IcebergResult<()> {
        match self {
            Self::Rest { catalog, request } => catalog.send_prepared_commit(*request),
            Self::Sqlite(request) => request.publish(),
        }
    }
}
