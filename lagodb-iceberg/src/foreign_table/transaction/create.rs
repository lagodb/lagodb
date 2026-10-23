//! Transaction-local origin and enrollment for staged catalog creation.

use std::collections::hash_map::Entry;
use std::rc::Rc;

use iceberg_lite::table::Table;
use iceberg_lite::{NamespaceIdent, TableCreation};
use pgrx::pg_sys;

use super::state::{ForeignTableState, StagedCreateDisposition};
use super::{
    CURRENT, CatalogTransactionState, ForeignTransaction, ForeignTransactionInner,
};
use crate::foreign_table::catalog::{
    CatalogRuntimeConfig, CatalogSession, ForeignCatalog, RemoteTableKey,
};
use crate::foreign_table::error::IcebergFdwError;

impl ForeignTransaction {
    pub(crate) fn is_staged_relation(relation_oid: pg_sys::Oid) -> bool {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return false;
            };
            let inner = tracker.inner.borrow();
            inner
                .catalogs
                .values()
                .flat_map(|catalog| catalog.tables.values())
                .any(|state| state.has_staged_relation(relation_oid))
        })
    }

    pub(crate) fn ensure_staged_create_available(
        key: &RemoteTableKey,
    ) -> Result<(), IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return Ok(());
            };
            let inner = tracker.inner.borrow();
            // An existing published table can still be bound by CREATE. Only
            // an active staged owner prevents the initial catalog lookup.
            if inner.staged_create_disposition(key)
                == Some(StagedCreateDisposition::ActiveStaged)
            {
                return Err(IcebergFdwError::StagedTableAlreadyProvisioned {
                    table: key.publication_name(),
                });
            }
            Ok(())
        })
    }

    pub(crate) fn staged_table(
        relation_oid: pg_sys::Oid,
        key: &RemoteTableKey,
        runtime_config: &CatalogRuntimeConfig,
    ) -> Result<Option<(ForeignCatalog, Table)>, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return Ok(None);
            };
            let inner = tracker.inner.borrow();
            let requested_identity = key.catalog_identity();
            if let Some(catalog) = inner.catalogs.get(requested_identity) {
                if catalog.invalidated
                    || &catalog.runtime_config != runtime_config
                {
                    return Err(IcebergFdwError::CatalogConfigurationChanged);
                }
                if let Some(state) = catalog.tables.get(key)
                    && state.has_staged_relation(relation_oid)
                {
                    return Ok(Some((
                        catalog.catalog.clone(),
                        state.staged_base().clone(),
                    )));
                }
            }

            // PostgreSQL FDWs bind remote access to a user mapping, and REST
            // tables also carry role-bound catalog and storage credentials.
            // Never borrow another role's staged object. Since staged creates
            // are catalog-invisible, report this capability boundary instead
            // of falling through to a misleading remote table-not-found.
            let staged_for_another_role = inner.catalogs.iter().any(
                |(catalog_identity, catalog)| {
                    catalog_identity.server_oid == requested_identity.server_oid
                        && catalog_identity.effective_user
                            != requested_identity.effective_user
                        && catalog.tables.values().any(|state| {
                            state.has_staged_relation(relation_oid)
                        })
                },
            );
            if staged_for_another_role {
                return Err(IcebergFdwError::UnsupportedOperation {
                    operation: "access to a transaction-staged table after changing the effective PostgreSQL role",
                });
            }
            Ok(None)
        })
    }

    pub(crate) fn stage_create(
        relation_oid: pg_sys::Oid,
        key: &RemoteTableKey,
        session: &CatalogSession,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table, IcebergFdwError> {
        let tracker = Self::current();
        let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        {
            let inner = tracker.inner.borrow();
            Self::validate_catalog(&inner, session)?;
            match inner.staged_create_disposition(key) {
                Some(StagedCreateDisposition::ActiveStaged) => {
                    return Err(IcebergFdwError::StagedTableAlreadyProvisioned {
                        table: key.publication_name(),
                    });
                }
                Some(StagedCreateDisposition::Published) => {
                    return Err(IcebergFdwError::TableGenerationConflict {
                        table: key.publication_name(),
                    });
                }
                Some(StagedCreateDisposition::ReplaceDropped) | None => {}
            }
        }
        let staged = session.catalog.stage_create_table(namespace, creation)?;
        let table = staged.table().clone();
        let mut inner = tracker.inner.borrow_mut();
        let catalog = inner
            .catalogs
            .entry(session.identity.clone())
            .or_insert_with(|| CatalogTransactionState::new(session));
        match catalog.tables.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().replace_dropped_staged_generation(
                    relation_oid,
                    level,
                    staged,
                );
            }
            Entry::Vacant(entry) => {
                entry.insert(ForeignTableState::staged(relation_oid, level, staged));
            }
        }
        Ok(table)
    }

    pub(crate) fn drop_staged_relation(
        relation_oid: pg_sys::Oid,
    ) -> Result<bool, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return Ok(false);
            };
            let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
            let mut inner = tracker.inner.borrow_mut();
            for catalog in inner.catalogs.values_mut() {
                for state in catalog.tables.values_mut() {
                    if !state.owns_staged_relation(relation_oid) {
                        continue;
                    }
                    state.drop_staged_relation(relation_oid, level)?;
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}

impl ForeignTransactionInner {
    fn staged_create_disposition(
        &self,
        key: &RemoteTableKey,
    ) -> Option<StagedCreateDisposition> {
        let mut disposition = None;
        // Creation ownership spans roles on the same server and table. This
        // does not grant access to another role's staged catalog credentials.
        for (catalog_identity, catalog) in &self.catalogs {
            if catalog_identity.server_oid != key.catalog_identity().server_oid {
                continue;
            }
            for (existing_key, state) in &catalog.tables {
                if existing_key.table() != key.table() {
                    continue;
                }
                match state.staged_create_disposition() {
                    StagedCreateDisposition::ActiveStaged => {
                        return Some(StagedCreateDisposition::ActiveStaged);
                    }
                    StagedCreateDisposition::Published => {
                        disposition = Some(StagedCreateDisposition::Published);
                    }
                    StagedCreateDisposition::ReplaceDropped => {
                        disposition
                            .get_or_insert(StagedCreateDisposition::ReplaceDropped);
                    }
                }
            }
        }
        disposition
    }
}
