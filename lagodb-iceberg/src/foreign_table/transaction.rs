//! Transaction-local state and post-commit publication for writable REST tables.

use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::catalog::rest::{PreparedRestCommit, RestCatalog};
use iceberg_lite::overlay::{DeleteFileIdentity, SnapshotDelta};
use iceberg_lite::spec::DataFile;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{
    PreparedTransaction, RowDeltaValidation, Transaction,
};
use lagodb_core::diag::{PgReportError, error_source_chain_detail, report_warning};
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::error::IcebergFdwError;
use super::options::{CatalogBindingKey, CatalogRuntimeConfig, ServerBindingKey};
use super::relation::{RemoteTableKey, ResolvedCatalogBinding, RestForeignTable};
use crate::engine::write::{
    ExclusiveTransactionAction, RelationRowRegistry, TableTransactionState,
};
use crate::error::{IcebergError, IcebergResult};

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
}

impl ForeignTableState {
    fn new(base: Table) -> Self {
        Self {
            base,
            transaction: ForeignTableTransaction::new(),
        }
    }
}

#[derive(Debug)]
struct CatalogBinding {
    catalog: RestCatalog,
    tables: HashMap<RemoteTableKey, ForeignTableState>,
}

#[derive(Debug, Default)]
struct ForeignTransactionInner {
    server_configs: HashMap<ServerBindingKey, CatalogRuntimeConfig>,
    // Multiple immutable catalog bindings are deliberately supported. Each
    // binding gets one atomic REST transaction request, but bindings publish
    // independently after PostgreSQL commits; a later publication failure does
    // not roll back an earlier catalog. This is part of the current best-effort
    // post-commit contract, not cross-catalog atomicity.
    catalogs: HashMap<CatalogBindingKey, CatalogBinding>,
    prepared: Vec<PreparedPublication>,
}

#[derive(Debug)]
struct PreparedPublication {
    catalog: RestCatalog,
    request: PreparedRestCommit,
    targets: String,
}

/// A table plus the file overlay visible to a statement in this backend.
pub(crate) struct ForeignTableView {
    pub(crate) key: RemoteTableKey,
    pub(crate) table: Table,
    pub(crate) delta: Option<Arc<SnapshotDelta>>,
}

/// One top-level PostgreSQL transaction's writable REST catalog state.
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

    /// Resolve an ordinary scan against transaction-local actions when present.
    pub(crate) fn scan_view(
        resolved: RestForeignTable,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            match tracker {
                Some(tracker) => tracker.overlay(resolved),
                None => {
                    let (key, _binding, table) = resolved.into_parts();
                    Ok(ForeignTableView {
                        key,
                        table,
                        delta: None,
                    })
                }
            }
        })
    }

    /// Enroll a writable table before any object is produced.
    pub(crate) fn begin_write(
        resolved: RestForeignTable,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        if !resolved.identity().mode().is_writable() {
            return Err(IcebergFdwError::ReadOnlyTable);
        }
        let tracker = Self::current();
        tracker.attach_write(resolved)
    }

    /// Reject a changed server/user-mapping binding before a catalog client is
    /// constructed or any remote table metadata is loaded.
    pub(crate) fn validate_runtime_binding(
        server_key: &ServerBindingKey,
        runtime_config: &CatalogRuntimeConfig,
    ) -> Result<(), IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            if let Some(tracker) = tracker
                && tracker
                    .inner
                    .borrow()
                    .server_configs
                    .get(server_key)
                    .is_some_and(|frozen| frozen != runtime_config)
            {
                return Err(IcebergFdwError::CatalogBindingChanged);
            }
            Ok(())
        })
    }

    fn attach_write(
        &self,
        resolved: RestForeignTable,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let (key, binding, table) = resolved.into_parts();
        let mut inner = self.inner.borrow_mut();
        if key.catalog_binding() != &binding.key {
            return Err(IcebergError::InvariantViolated(
                "foreign table and resolved catalog binding disagree",
            )
            .into());
        }
        Self::validate_binding(&inner, &binding)?;
        if !inner.server_configs.contains_key(&binding.server_key) {
            inner
                .server_configs
                .insert(binding.server_key.clone(), binding.runtime_config.clone());
        }
        inner
            .catalogs
            .entry(binding.key)
            .or_insert_with(|| CatalogBinding {
                catalog: binding.catalog,
                tables: HashMap::new(),
            })
            .tables
            .entry(key.clone())
            .or_insert_with(|| ForeignTableState::new(table.clone()));
        drop(inner);
        self.overlay_parts(key, table)
    }

    fn overlay(
        &self,
        resolved: RestForeignTable,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let (key, binding, table) = resolved.into_parts();
        Self::validate_binding(&self.inner.borrow(), &binding)?;
        self.overlay_parts(key, table)
    }

    fn validate_binding(
        inner: &ForeignTransactionInner,
        binding: &ResolvedCatalogBinding,
    ) -> Result<(), IcebergFdwError> {
        if inner
            .server_configs
            .get(&binding.server_key)
            .is_some_and(|config| config != &binding.runtime_config)
        {
            return Err(IcebergFdwError::CatalogBindingChanged);
        }
        Ok(())
    }

    fn overlay_parts(
        &self,
        key: RemoteTableKey,
        table: Table,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let actions = self
            .inner
            .borrow()
            .catalogs
            .get(key.catalog_binding())
            .and_then(|binding| binding.tables.get(&key))
            .map(|state| Rc::clone(&state.transaction.actions));
        let Some(actions) = actions else {
            return Ok(ForeignTableView {
                key,
                table,
                delta: None,
            });
        };
        let metadata = actions.overlay_metadata(table.metadata().clone())?;
        let delta = actions.combined_delta()?;
        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .metadata(metadata);
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location.to_owned());
        }
        Ok(ForeignTableView {
            key,
            table: builder.build()?,
            delta,
        })
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
            .map(|state| state.transaction.row_registry.clone())
            .ok_or(IcebergError::InvariantViolated(
                "foreign table was not enrolled before row identity registration",
            ))
    }

    pub(crate) fn stage_data_files(
        key: &RemoteTableKey,
        files: Vec<DataFile>,
    ) -> IcebergResult<()> {
        Self::stage(key, |transaction, level| {
            transaction.record_data_files(level, files)
        })
    }

    pub(crate) fn stage_position_delete_file(
        key: &RemoteTableKey,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        Self::stage(key, |transaction, level| {
            transaction.record_position_delete_file(
                level,
                delete_file,
                referenced_data_files,
            )
        })
    }

    pub(crate) fn stage_remove_delete_file(
        key: &RemoteTableKey,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        Self::stage(key, |transaction, level| {
            transaction.record_remove_delete_file(level, identity)
        })
    }

    pub(crate) fn stage_validation(
        key: &RemoteTableKey,
        validation: RowDeltaValidation,
    ) -> IcebergResult<()> {
        Self::stage(key, |transaction, level| {
            transaction.record_validation(level, validation)
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
                u32::from(left.0.server.server_oid),
                u32::from(left.0.server.effective_user),
                &left.0.catalog_name,
            )
                .cmp(&(
                    u32::from(right.0.server.server_oid),
                    u32::from(right.0.server.effective_user),
                    &right.0.catalog_name,
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
                match transaction.prepare(&catalog)? {
                    PreparedTransaction::Noop(_) => {}
                    PreparedTransaction::Commit(commit) => commits.push(commit),
                }
            }
            if commits.is_empty() {
                continue;
            }
            let request = catalog.prepare_transaction_commit(commits)?;
            prepared.push(PreparedPublication {
                catalog,
                request,
                targets,
            });
        }
        self.inner.borrow_mut().prepared = prepared;
        Ok(())
    }

    fn rollback_to_level(&self, level: i32) {
        for binding in self.inner.borrow_mut().catalogs.values_mut() {
            for state in binding.tables.values_mut() {
                state.transaction.rollback_to_level(level);
            }
        }
    }

    fn promote_to_level(&self, level: i32) {
        for binding in self.inner.borrow_mut().catalogs.values_mut() {
            for state in binding.tables.values_mut() {
                state.transaction.promote_to_level(level);
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
        // REST has no PostgreSQL-compatible prepare/commit/abort protocol.
        // Publication deliberately runs synchronously from XACT_EVENT_COMMIT,
        // after the local commit record and before PostgreSQL releases all
        // backend transaction resources and locks. A pending cancel or another
        // PostgreSQL ERROR raised by the transport can therefore terminate the
        // backend after local commit; a network failure is only observable and
        // cannot roll back PostgreSQL or another catalog publication. Changing
        // this contract requires a durable outbox/worker, not moving remote
        // publication before the local commit decision.
        let prepared = mem::take(&mut self.inner.borrow_mut().prepared);
        for publication in prepared {
            if let Err(error) = publication
                .catalog
                .send_prepared_commit(publication.request)
            {
                if let Some(chain) =
                    error.source().and_then(error_source_chain_detail)
                {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg REST catalog publication for {} failed: {error}; {chain}; external table state is unknown",
                        publication.targets,
                    ));
                } else {
                    report_warning(format_args!(
                        "local transaction committed but Iceberg REST catalog publication for {} failed: {error}; external table state is unknown",
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

    fn on_commit_sub(&self, current_nest_level: i32) {
        self.promote_to_level(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.rollback_to_level(current_nest_level);
    }
}
