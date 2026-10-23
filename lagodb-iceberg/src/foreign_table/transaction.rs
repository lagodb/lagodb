//! Transaction-local state and post-commit publication for writable Iceberg tables.

use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::mem;
use std::rc::Rc;

use iceberg_lite::transaction::PreparedTransaction;
use lagodb_core::catalog::{
    ForeignConfigInvalidation, register_foreign_config_invalidation_observer,
};
use lagodb_core::diag::{PgReportError, error_source_chain_detail, report_warning};
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::catalog::{
    BoundCatalogTable, CatalogAccessIdentity, CatalogChange, CatalogSession,
    PreparedCatalogPublication, RemoteTableKey,
};
use super::error::IcebergFdwError;
use super::source_identity::PlanSourceIdentity;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::transaction_resources::register_canceled_files_for_commit;
use crate::write::{
    ExclusiveTransactionAction, RelationRowRegistry, TableTransactionState,
    TxWriteBatch,
};

mod catalog_state;
mod create;
mod read;
mod state;

use catalog_state::CatalogTransactionState;
pub(crate) use read::ForeignTableReadView;
use state::ForeignTableState;

type ForeignTableTransaction = TableTransactionState<NoExclusiveAction, ()>;

#[derive(Debug, Clone)]
struct NoExclusiveAction;

impl ExclusiveTransactionAction for NoExclusiveAction {
    const NOT_EXCLUSIVE_INVARIANT: &'static str =
        "writable foreign table has a non-exclusive action conflict";
    const MISPLACED_INVARIANT: &'static str =
        "writable foreign table has an unexpected exclusive action";

    fn conflict_error(&self) -> IcebergError {
        IcebergError::InvariantViolated(Self::MISPLACED_INVARIANT)
    }
}

#[derive(Debug, Default)]
struct ForeignTransactionInner {
    // Multiple immutable catalog transaction states are deliberately supported.
    // Each catalog publishes independently after PostgreSQL commits; a later
    // publication failure does not roll back an earlier catalog. REST staged
    // creates use per-table commits before the catalog's atomic update batch,
    // while SQLite publishes the catalog in one transaction. This is part of
    // the current best-effort post-commit contract, not cross-catalog atomicity.
    catalogs: HashMap<CatalogAccessIdentity, CatalogTransactionState>,
    prepared: Vec<PreparedCatalogPublication>,
}

/// One top-level PostgreSQL transaction's writable Iceberg catalog state.
#[derive(Debug)]
pub(crate) struct ForeignTransaction {
    inner: RefCell<ForeignTransactionInner>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<ForeignTransaction>>> = const { RefCell::new(None) };
}

impl ForeignTransaction {
    pub(crate) fn register_invalidation_observer() {
        register_foreign_config_invalidation_observer(Self::invalidate_catalogs);
    }

    fn invalidate_catalogs(invalidation: ForeignConfigInvalidation) {
        CURRENT.with(|slot| {
            let current = slot.borrow().as_ref().map(Rc::clone);
            let Some(current) = current else {
                return;
            };
            for catalog in current.inner.borrow_mut().catalogs.values_mut() {
                catalog.invalidate_if_matches(invalidation);
            }
        });
    }

    fn current() -> Rc<Self> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(current) = slot.as_ref() {
                return Rc::clone(current);
            }
            let tracker = Rc::new(Self {
                inner: RefCell::new(ForeignTransactionInner::default()),
            });
            transaction::register_resource(
                Rc::clone(&tracker) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&tracker));
            tracker
        })
    }

    /// Enroll a writable table before any object is produced.
    pub(crate) fn begin_write(
        catalog_table: BoundCatalogTable,
        read_command_id: Option<pg_sys::CommandId>,
        expected_source: Option<&PlanSourceIdentity>,
    ) -> Result<ForeignTableReadView, IcebergFdwError> {
        let tracker = Self::current();
        tracker.attach_write(catalog_table, read_command_id, expected_source)
    }

    fn attach_write(
        &self,
        catalog_table: BoundCatalogTable,
        read_command_id: Option<pg_sys::CommandId>,
        expected_source: Option<&PlanSourceIdentity>,
    ) -> Result<ForeignTableReadView, IcebergFdwError> {
        let (key, session, base) = catalog_table.into_parts();
        Self::validate_catalog(&self.inner.borrow(), &session)?;
        let view = self.scan_view_parts(key, base, read_command_id)?;
        if expected_source.is_some_and(|expected| {
            expected != &PlanSourceIdentity::from_metadata(view.metadata())
        }) {
            return Err(IcebergFdwError::PlanSourceChanged);
        }
        let (key, base) = view.enrollment_parts();
        let mut inner = self.inner.borrow_mut();
        inner
            .catalogs
            .entry(session.identity.clone())
            .or_insert_with(|| CatalogTransactionState::new(&session))
            .tables
            .entry(key.clone())
            .or_insert_with(|| ForeignTableState::published(base.clone()));
        Ok(view)
    }

    fn validate_catalog(
        inner: &ForeignTransactionInner,
        session: &CatalogSession,
    ) -> Result<(), IcebergFdwError> {
        if let Some(catalog) = inner.catalogs.get(&session.identity) {
            catalog.validate(session)?;
        }
        Ok(())
    }

    pub(crate) fn row_registry(
        key: &RemoteTableKey,
    ) -> IcebergResult<RelationRowRegistry> {
        let tracker = Self::current();
        let inner = tracker.inner.try_borrow().map_err(|_| {
            IcebergError::InvariantViolated(
                "foreign transaction tracker is already mutably borrowed",
            )
        })?;
        inner
            .catalogs
            .get(key.catalog_identity())
            .and_then(|catalog| catalog.tables.get(key))
            .map(ForeignTableState::row_registry)
            .ok_or(IcebergError::InvariantViolated(
                "foreign table was not enrolled before row identity registration",
            ))
    }

    pub(crate) fn stage_batch(
        key: &RemoteTableKey,
        batch: TxWriteBatch,
    ) -> IcebergResult<()> {
        Self::stage(key, |transaction, level| {
            transaction.record_batch(level, batch)
        })
    }

    fn stage<F>(key: &RemoteTableKey, mutation: F) -> IcebergResult<()>
    where
        F: FnOnce(&mut ForeignTableTransaction, i32) -> IcebergResult<()>,
    {
        let tracker = Self::current();
        let mut inner = tracker.inner.borrow_mut();
        let state = inner
            .catalogs
            .get_mut(key.catalog_identity())
            .and_then(|catalog| catalog.tables.get_mut(key))
            .ok_or(IcebergError::InvariantViolated(
                "foreign table mutation was staged before write enrollment",
            ))?;
        let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        mutation(state.transaction_mut(), level)
    }

    fn prepare_publication(&self) -> Result<(), IcebergFdwError> {
        // Configuration admission ends at this entry check. Preserve existing
        // handles for abort cleanup if an earlier invalidation rejects it.
        // Accepted policy: invalidations received during preparation or later
        // do not revoke this publication's admission; it finishes with its
        // retained catalog sessions. This is not serialization with concurrent
        // ALTER SERVER/USER MAPPING, and PG can still abort on preparation errors.
        // PG17 postgres_fdw likewise lets an in-use invalidated connection
        // finish its transaction, then disconnects it (connection.c:
        // pgfdw_inval_callback, pgfdw_xact_callback, pgfdw_reset_xact_state).
        // LagoDB adds this entry check and publishes after local commit; do not
        // add later invalidation rejection without changing that contract.
        if self
            .inner
            .borrow()
            .catalogs
            .values()
            .any(|catalog| catalog.invalidated && catalog.has_catalog_changes())
        {
            return Err(IcebergFdwError::CatalogConfigurationChanged);
        }
        let canceled_files = {
            let inner = self.inner.borrow();
            inner
                .catalogs
                .values()
                .flat_map(|catalog| catalog.tables.values())
                .try_fold(Vec::new(), |mut canceled, state| {
                    canceled.extend(state.canceled_files()?);
                    Ok::<_, IcebergError>(canceled)
                })?
        };
        for (file_io, paths) in canceled_files {
            if !paths.is_empty() {
                register_canceled_files_for_commit(file_io, paths);
            }
        }
        let groups = {
            let inner = self.inner.borrow();
            inner
                .catalogs
                .iter()
                .filter_map(|(catalog_identity, catalog)| {
                    let tables = catalog
                        .tables
                        .iter()
                        .filter(|(_, state)| state.has_catalog_changes())
                        .map(|(key, state)| {
                            (
                                key.clone(),
                                state.is_staged_create(),
                                state.iceberg_transaction(),
                                state.commit_base().metadata_ref(),
                                Rc::clone(&state.transaction().actions),
                            )
                        })
                        .collect::<Vec<_>>();
                    (!tables.is_empty()).then(|| {
                        (catalog_identity.clone(), catalog.catalog.clone(), tables)
                    })
                })
                .collect::<Vec<_>>()
        };
        if groups.is_empty() {
            return Ok(());
        }
        let mut groups = groups;
        groups.sort_unstable_by(|left, right| {
            (
                u32::from(left.0.server_oid),
                u32::from(left.0.effective_user),
            )
                .cmp(&(
                    u32::from(right.0.server_oid),
                    u32::from(right.0.effective_user),
                ))
        });
        let mut prepared = Vec::with_capacity(groups.len());
        for (_catalog_identity, catalog, mut tables) in groups {
            tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let mut changes = Vec::with_capacity(tables.len());
            for (key, staged_create, transaction, base_metadata, actions) in tables {
                let plan = actions.commit_plan()?;
                let transaction =
                    plan.apply_to_transaction(transaction, &base_metadata)?;
                match transaction.prepare(catalog.as_catalog())? {
                    PreparedTransaction::Noop(_) => {}
                    PreparedTransaction::Commit(commit) => {
                        let target = key.publication_name();
                        let change = if staged_create {
                            CatalogChange::StagedCreate { target, commit }
                        } else {
                            CatalogChange::TableUpdate { target, commit }
                        };
                        changes.push(change);
                    }
                }
            }
            if changes.is_empty() {
                continue;
            }
            prepared.extend(catalog.prepare_publications(changes)?);
        }
        self.inner.borrow_mut().prepared = prepared;
        Ok(())
    }

    fn rollback_to_level(&self, level: i32) {
        for catalog in self.inner.borrow_mut().catalogs.values_mut() {
            catalog
                .tables
                .retain(|_, state| !state.staged_at_or_after(level));
            for state in catalog.tables.values_mut() {
                state.rollback_to_level(level);
            }
        }
    }

    fn promote_to_level(&self, level: i32) {
        for catalog in self.inner.borrow_mut().catalogs.values_mut() {
            for state in catalog.tables.values_mut() {
                state.promote_to_level(level);
            }
        }
    }

    fn reset_current() {
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }
}

impl TransactionResource for ForeignTransaction {
    fn nest_level(&self) -> i32 {
        1
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_pre_commit(&self) -> TransactionResult<()> {
        self.prepare_publication()
            .map_err(PgReportError::from_domain_error)
    }

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        // Canceled creates and superseded generations remain backend-local
        // state even when no catalog publication remains. Generation rollback
        // and owned-file cleanup have no durable two-phase representation, so
        // PREPARE rejects this state regardless of the final publication plan.
        let has_transactional_state = self
            .inner
            .borrow()
            .catalogs
            .values()
            .flat_map(|catalog| catalog.tables.values())
            .any(ForeignTableState::has_transactional_state);
        if has_transactional_state {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot PREPARE a transaction with staged Iceberg foreign-table catalog changes",
            ));
        }
        Ok(())
    }

    fn on_commit(&self) {
        // Accepted transaction boundary: PostgreSQL and the external catalog
        // make independent commit decisions. Iceberg's table commit and REST
        // multi-table commit do not define a prepare/commit/abort participant
        // protocol for PostgreSQL; neither adapter implements such a protocol.
        // Atomic publication within one catalog is not atomicity with PG.
        // See https://iceberg.apache.org/spec/#table-metadata and
        // https://iceberg.apache.org/rest-catalog-spec/.
        //
        // Publication deliberately runs synchronously from XACT_EVENT_COMMIT,
        // after the local commit record and before PostgreSQL releases all
        // backend transaction resources and locks. Catalog conflicts, SQLite
        // lock contention and transport failures can leave the local transaction
        // committed without publishing its external changes. WARNING reports
        // this accepted best-effort outcome; it cannot undo either commit.
        //
        // Retries or a durable outbox can improve delivery and recovery, but do
        // not by themselves provide cross-system atomicity. Publishing before
        // PG commits reverses the failure window instead of removing it.
        // Revisit this boundary only if the transaction contract changes and a
        // shared commit protocol is introduced; the absence of cross-system
        // atomicity alone is not an implementation defect under this contract.
        let prepared = mem::take(&mut self.inner.borrow_mut().prepared);
        for publication in prepared {
            let (request, targets) = publication.into_parts();
            if let Err(error) = request.publish() {
                if let Some(chain) =
                    error.source().and_then(error_source_chain_detail)
                {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg catalog publication for {} failed: {error}; {chain}; external table state is unknown",
                        targets,
                    ));
                } else {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg catalog publication for {} failed: {error}; external table state is unknown",
                        targets,
                    ));
                }
            }
        }
        Self::reset_current();
    }

    fn on_abort(&self) {
        Self::reset_current();
    }

    fn on_prepare(&self) {
        Self::reset_current();
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        self.promote_to_level(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.rollback_to_level(current_nest_level);
    }
}
