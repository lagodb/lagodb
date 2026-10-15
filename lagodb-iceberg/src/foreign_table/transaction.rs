//! Transaction-local state and post-commit publication for writable Iceberg tables.

use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::mem;
use std::rc::Rc;

use iceberg_lite::table::Table;
use iceberg_lite::transaction::{PreparedTransaction, Transaction};
use lagodb_core::diag::{PgReportError, error_source_chain_detail, report_warning};
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::catalog::{ForeignCatalog, PreparedCatalogCommit};
use super::error::IcebergFdwError;
use super::options::{CatalogRuntimeConfig, ServerBindingKey};
use super::relation::{IcebergForeignTable, RemoteTableKey, ResolvedCatalogBinding};
use crate::error::{IcebergError, IcebergResult};
use crate::write::{
    ExclusiveTransactionAction, RelationRowRegistry, TableTransactionState,
    TxWriteBatch,
};

mod read;
pub(crate) use read::ForeignTableView;

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

#[derive(Debug)]
struct ForeignTableState {
    base: Table,
    transaction: ForeignTableTransaction,
    row_registry: RelationRowRegistry,
}

impl ForeignTableState {
    fn new(base: Table) -> Self {
        Self {
            base,
            transaction: ForeignTableTransaction::new(),
            row_registry: RelationRowRegistry::default(),
        }
    }

    fn rollback_to_level(&mut self, level: i32) {
        self.transaction.rollback_to_level(level);
        self.row_registry.rollback_to_level(level);
    }

    fn promote_to_level(&mut self, level: i32) {
        self.transaction.promote_to_level(level);
        self.row_registry.promote_to_level(level);
    }
}

#[derive(Debug)]
struct CatalogBinding {
    catalog: ForeignCatalog,
    runtime_config: CatalogRuntimeConfig,
    tables: HashMap<RemoteTableKey, ForeignTableState>,
}

#[derive(Debug, Default)]
struct ForeignTransactionInner {
    // Multiple immutable catalog bindings are deliberately supported. Each
    // binding gets one atomic catalog transaction, but bindings publish
    // independently after PostgreSQL commits; a later publication failure does
    // not roll back an earlier catalog. This is part of the current best-effort
    // post-commit contract, not cross-catalog atomicity.
    catalogs: HashMap<ServerBindingKey, CatalogBinding>,
    prepared: Vec<PreparedPublication>,
}

#[derive(Debug)]
struct PreparedPublication {
    request: PreparedCatalogCommit,
    targets: String,
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
        resolved: IcebergForeignTable,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let tracker = Self::current();
        tracker.attach_write(resolved, read_command_id)
    }

    fn attach_write(
        &self,
        resolved: IcebergForeignTable,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let (key, binding, table) = resolved.into_parts();
        let mut inner = self.inner.borrow_mut();
        Self::validate_binding(&inner, &binding)?;
        inner
            .catalogs
            .entry(binding.key)
            .or_insert_with(|| CatalogBinding {
                catalog: binding.catalog,
                runtime_config: binding.runtime_config,
                tables: HashMap::new(),
            })
            .tables
            .entry(key.clone())
            .or_insert_with(|| ForeignTableState::new(table.clone()));
        drop(inner);
        self.overlay_parts(key, table, read_command_id)
    }

    fn validate_binding(
        inner: &ForeignTransactionInner,
        binding: &ResolvedCatalogBinding,
    ) -> Result<(), IcebergFdwError> {
        if inner
            .catalogs
            .get(&binding.key)
            .is_some_and(|frozen| frozen.runtime_config != binding.runtime_config)
        {
            return Err(IcebergFdwError::CatalogBindingChanged);
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
            .get(key.catalog_binding())
            .and_then(|binding| binding.tables.get(key))
            .map(|state| state.row_registry.clone())
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
            .get_mut(key.catalog_binding())
            .and_then(|binding| binding.tables.get_mut(key))
            .ok_or(IcebergError::InvariantViolated(
                "foreign table mutation was staged before write enrollment",
            ))?;
        let level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        mutation(&mut state.transaction, level)
    }

    fn has_actions(&self) -> bool {
        self.inner
            .borrow()
            .catalogs
            .values()
            .flat_map(|binding| binding.tables.values())
            .any(|state| !state.transaction.actions.is_empty())
    }

    fn prepare_publication(&self) -> Result<(), IcebergFdwError> {
        let groups = {
            let inner = self.inner.borrow();
            inner
                .catalogs
                .iter()
                .filter_map(|(binding_key, binding)| {
                    let tables = binding
                        .tables
                        .iter()
                        .filter(|(_, state)| !state.transaction.actions.is_empty())
                        .map(|(key, state)| {
                            (
                                key.clone(),
                                state.base.clone(),
                                Rc::clone(&state.transaction.actions),
                            )
                        })
                        .collect::<Vec<_>>();
                    (!tables.is_empty()).then(|| {
                        (binding_key.clone(), binding.catalog.clone(), tables)
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
        for (_binding_key, catalog, mut tables) in groups {
            tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let targets = tables
                .iter()
                .map(|(key, _, _)| key.publication_name())
                .collect::<Vec<_>>()
                .join(", ");
            let mut commits = Vec::with_capacity(tables.len());
            for (_key, base, actions) in tables {
                let plan = actions.commit_plan()?;
                let transaction = plan
                    .apply_to_transaction(Transaction::new(&base), base.metadata())?;
                match transaction.prepare(catalog.as_catalog())? {
                    PreparedTransaction::Noop(_) => {}
                    PreparedTransaction::Commit(commit) => commits.push(commit),
                }
            }
            if commits.is_empty() {
                continue;
            }
            let request = catalog.prepare_transaction_commit(commits)?;
            prepared.push(PreparedPublication { request, targets });
        }
        self.inner.borrow_mut().prepared = prepared;
        Ok(())
    }

    fn rollback_to_level(&self, level: i32) {
        for binding in self.inner.borrow_mut().catalogs.values_mut() {
            for state in binding.tables.values_mut() {
                state.rollback_to_level(level);
            }
        }
    }

    fn promote_to_level(&self, level: i32) {
        for binding in self.inner.borrow_mut().catalogs.values_mut() {
            for state in binding.tables.values_mut() {
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
        if self.has_actions() {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot PREPARE a transaction with staged writable Iceberg foreign-table changes",
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
            if let Err(error) = publication.request.publish() {
                if let Some(chain) =
                    error.source().and_then(error_source_chain_detail)
                {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg catalog publication for {} failed: {error}; {chain}; external table state is unknown",
                        publication.targets,
                    ));
                } else {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg catalog publication for {} failed: {error}; external table state is unknown",
                        publication.targets,
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
