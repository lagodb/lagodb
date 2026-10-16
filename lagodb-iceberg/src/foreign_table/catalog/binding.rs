//! Catalog selection and owned publication requests for the FDW.

use std::sync::Arc;

use iceberg_lite::catalog::rest::{PreparedRestCommit, RestCatalog};
use iceberg_lite::catalog::sqlite::{PreparedSqliteCommit, SqliteCatalog};
use iceberg_lite::transaction::PreparedTableCommit;
use iceberg_lite::{Catalog, Result};

#[derive(Clone, Debug)]
pub(crate) enum ForeignCatalog {
    Rest(Arc<RestCatalog>),
    Sqlite(SqliteCatalog),
}

impl ForeignCatalog {
    /// Reuse a write transaction's SQLite connection without its table view.
    /// REST clients are shared only within an executor, never acquired from
    /// a write binding that can outlive that executor.
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

    pub(crate) fn prepare_transaction_commit(
        &self,
        commits: Vec<PreparedTableCommit>,
    ) -> Result<PreparedCatalogCommit> {
        match self {
            Self::Rest(catalog) => Ok(PreparedCatalogCommit::Rest {
                request: Box::new(catalog.prepare_transaction_commit(commits)?),
                catalog: Arc::clone(catalog),
            }),
            Self::Sqlite(catalog) => Ok(PreparedCatalogCommit::Sqlite(
                catalog.prepare_transaction_commit(commits)?,
            )),
        }
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
    pub(crate) fn publish(self) -> Result<()> {
        match self {
            Self::Rest { catalog, request } => catalog.send_prepared_commit(*request),
            Self::Sqlite(request) => request.publish(),
        }
    }
}
