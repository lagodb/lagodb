//! Distributed tablespace store reconciler.
//!
//! Pure Rust diff/apply logic. The desired state is a snapshot loaded from a
//! [`StoreCatalogSource`]; the actual state is `pg_lakebase_storage`'s
//! [`StoreRegistry`]. On every reconcile we rebuild the desired snapshot,
//! validate and de-duplicate it, compute add/remove/replace/no-op, and only
//! mutate the registry once every desired entry has passed validation.
//!
//! # Module boundary
//!
//! This module must not import `pgrx` or `pg_sys`. PostgreSQL FFI lives in
//! [`super::catalog`]. The split keeps reconcile logic unit-testable from a
//! plain `cargo test` without needing a PostgreSQL backend.

use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;

use pg_lakebase_storage::{StoreConfig, StoreId, StoreRegistry};

/// Desired-state record for one distributed tablespace.
///
/// Identity is `store_id`. The remaining fields are observable side data; any
/// difference between two specs with the same `store_id` causes a replace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TablespaceStoreSpec {
    pub store_id: StoreId,
    pub tablespace_name: String,
    pub object_namespace: String,
    pub base_url: String,
    pub config: StoreConfig,
}

/// Source of the desired tablespace-store snapshot.
///
/// Implementations are expected to do a full rescan of the authoritative
/// catalog on every `load()` call. The reconciler relies on the snapshot being
/// a complete picture, not a delta.
pub(crate) trait StoreCatalogSource {
    type Error: StdError + Send + Sync + 'static;

    fn load(&mut self) -> Result<Vec<TablespaceStoreSpec>, Self::Error>;
}

/// Errors raised by [`StoreCatalogReconciler::apply`].
#[derive(Debug)]
pub(crate) enum ReconcileError<E: StdError + Send + Sync + 'static> {
    /// The catalog source failed to produce a snapshot.
    Source(E),
    /// The desired snapshot contained two specs with the same `store_id`.
    DuplicateStoreId { store_id: StoreId },
    /// `register_config` rejected a spec.
    Register {
        store_id: StoreId,
        source: pg_lakebase_storage::StorageError,
    },
}

impl<E> fmt::Display for ReconcileError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => {
                write!(f, "failed to load tablespace store catalog: {error}")
            }
            Self::DuplicateStoreId { store_id } => write!(
                f,
                "duplicate distributed tablespace store id '{}' in desired snapshot",
                store_id.as_str()
            ),
            Self::Register { store_id, source } => write!(
                f,
                "failed to register store '{}' in storage registry: {source}",
                store_id.as_str()
            ),
        }
    }
}

impl<E> StdError for ReconcileError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Register { source, .. } => Some(source),
            Self::DuplicateStoreId { .. } => None,
        }
    }
}

/// Summary of a single reconcile pass. Useful for logging and tests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReconcileReport {
    pub added: usize,
    pub removed: usize,
    pub replaced: usize,
    pub unchanged: usize,
}

/// Drives `StoreRegistry` toward the desired snapshot returned by `S`.
pub(crate) struct StoreCatalogReconciler<S> {
    source: S,
    registry: StoreRegistry,
    /// Last successfully applied snapshot, keyed by `store_id`. The reconciler
    /// is the sole writer; it never observes mutations to `registry` made by
    /// anything else.
    applied: HashMap<StoreId, TablespaceStoreSpec>,
}

impl<S> StoreCatalogReconciler<S>
where
    S: StoreCatalogSource,
{
    pub(crate) fn new(source: S, registry: StoreRegistry) -> Self {
        Self {
            source,
            registry,
            applied: HashMap::new(),
        }
    }

    /// Reload the desired snapshot and bring `registry` into sync with it.
    ///
    /// This is a convenience wrapper used by tests; production code calls
    /// [`Self::load_desired`] (inside a PostgreSQL transaction) and
    /// [`Self::apply_desired`] (outside) separately so the transaction
    /// boundary stays narrow.
    #[cfg(test)]
    pub(crate) fn apply(
        &mut self,
    ) -> Result<ReconcileReport, ReconcileError<S::Error>> {
        let desired = self.load_desired()?;
        self.apply_desired(desired)
    }

    /// Load the desired snapshot from the catalog source.
    ///
    /// This is the only call that needs to run inside an active PostgreSQL
    /// transaction (when the source is the PostgreSQL catalog). Splitting it
    /// out lets the bgworker open the transaction only for the scan and
    /// release it before any [`StoreRegistry`] mutation happens.
    ///
    /// # Unwind safety contract
    ///
    /// The bgworker calls this method via
    /// `BackgroundWorker::transaction(AssertUnwindSafe(|| reconciler.load_desired()))`.
    /// That assertion is sound only if `load_desired` does not mutate any
    /// observable state before a PostgreSQL longjmp could fire. Today the
    /// method only reads from `self.source` and builds an owned
    /// `HashMap`; neither `self.applied` nor `self.registry` is touched.
    /// **Future maintainers must preserve this property** or stop using
    /// `AssertUnwindSafe` in the supervisor.
    pub(crate) fn load_desired(
        &mut self,
    ) -> Result<HashMap<StoreId, TablespaceStoreSpec>, ReconcileError<S::Error>>
    {
        let desired_vec = self.source.load().map_err(ReconcileError::Source)?;
        build_desired_map(desired_vec)
    }

    /// Apply a previously loaded desired snapshot to the registry.
    ///
    /// # Failure atomicity
    ///
    /// All desired specs are validated up front via
    /// [`StoreConfig::validate`]. After that point the per-spec
    /// `register_config` calls have no remaining failure modes (the registry
    /// re-validates internally for defense in depth), so a registry write
    /// loop that returns `Err` indicates a previously-undocumented failure
    /// path. We surface it as [`ReconcileError::Register`] but leave the
    /// already-applied entries in place: the next reconcile pass will
    /// reconcile them against a fresh desired snapshot.
    ///
    /// On a [`ReconcileError::Register`] from validation, the registry is
    /// untouched and `applied` is unchanged; the only mutations happen after
    /// validation succeeds for every spec.
    pub(crate) fn apply_desired(
        &mut self,
        desired: HashMap<StoreId, TablespaceStoreSpec>,
    ) -> Result<ReconcileReport, ReconcileError<S::Error>> {
        let mut report = ReconcileReport::default();

        // First pass: classify into adds, replaces, no-ops, and removals
        // *without* touching the registry.
        let mut to_register: Vec<&TablespaceStoreSpec> = Vec::new();
        let mut to_unregister: Vec<StoreId> = Vec::new();

        for (id, spec) in desired.iter() {
            match self.applied.get(id) {
                None => {
                    to_register.push(spec);
                    report.added += 1;
                }
                Some(existing) if existing != spec => {
                    to_register.push(spec);
                    report.replaced += 1;
                }
                Some(_) => {
                    report.unchanged += 1;
                }
            }
        }

        for id in self.applied.keys() {
            if !desired.contains_key(id) {
                to_unregister.push(id.clone());
                report.removed += 1;
            }
        }

        // Second pass: validate every spec we plan to register before
        // touching the registry. `StoreConfig::validate` is the only failure
        // mode `register_config` can surface for a syntactically-valid
        // store_id, so doing it up front gives us true all-or-nothing
        // semantics for the validation phase.
        for spec in &to_register {
            spec.config.validate().map_err(|source| {
                ReconcileError::Register {
                    store_id: spec.store_id.clone(),
                    source,
                }
            })?;
        }

        // Third pass: mutate the registry. After up-front validation the
        // per-spec `register_config` should never fail; if it does (e.g. an
        // unexpected internal error in the registry), we abort and report.
        // Already-registered entries in this same pass stay in the registry
        // and the next reconcile evaluates them against a fresh snapshot.
        for spec in &to_register {
            self.registry
                .register_config(spec.store_id.as_str(), spec.config.clone())
                .map_err(|source| ReconcileError::Register {
                    store_id: spec.store_id.clone(),
                    source,
                })?;
        }

        // TODO(per-store cache purge): when a tablespace disappears from the
        // catalog (DROP TABLESPACE) we currently only unregister the store.
        // The matching cache + staging directories under
        // `<cache_dir>/<store_id>` keep their data on disk until the operator
        // deletes them manually. Adding a cleanup hook here would close that
        // loop, but it is intentionally deferred — see "Distributed
        // tablespace reconciliation" / "Failure modes" in
        // pg-lakebase-core/src/worker/storage/README.md.
        for id in &to_unregister {
            self.registry.unregister(id);
        }

        // Update tracked state only after both registry passes succeed.
        for spec in to_register {
            self.applied.insert(spec.store_id.clone(), spec.clone());
        }
        for id in to_unregister {
            self.applied.remove(&id);
        }

        Ok(report)
    }

    #[cfg(test)]
    pub(crate) fn applied_len(&self) -> usize {
        self.applied.len()
    }
}

fn build_desired_map<E>(
    specs: Vec<TablespaceStoreSpec>,
) -> Result<HashMap<StoreId, TablespaceStoreSpec>, ReconcileError<E>>
where
    E: StdError + Send + Sync + 'static,
{
    let mut map = HashMap::with_capacity(specs.len());
    let mut seen = HashSet::with_capacity(specs.len());
    for spec in specs {
        if !seen.insert(spec.store_id.clone()) {
            return Err(ReconcileError::DuplicateStoreId {
                store_id: spec.store_id,
            });
        }
        map.insert(spec.store_id.clone(), spec);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_storage::S3StoreConfig;
    use std::cell::RefCell;
    use std::convert::Infallible;
    use std::rc::Rc;

    /// In-memory catalog source: hands back whatever snapshot the test wrote
    /// into the inner cell. Tests can also inject errors.
    #[derive(Clone, Default)]
    struct MockSource {
        inner: Rc<RefCell<MockSourceState>>,
    }

    #[derive(Default)]
    struct MockSourceState {
        snapshot: Vec<TablespaceStoreSpec>,
        next_error: Option<MockError>,
    }

    #[derive(Debug)]
    struct MockError(&'static str);

    impl fmt::Display for MockError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl StdError for MockError {}

    impl MockSource {
        fn set_snapshot(&self, snapshot: Vec<TablespaceStoreSpec>) {
            self.inner.borrow_mut().snapshot = snapshot;
        }

        fn fail_next(&self, message: &'static str) {
            self.inner.borrow_mut().next_error = Some(MockError(message));
        }
    }

    impl StoreCatalogSource for MockSource {
        type Error = MockError;

        fn load(&mut self) -> Result<Vec<TablespaceStoreSpec>, Self::Error> {
            let mut state = self.inner.borrow_mut();
            if let Some(err) = state.next_error.take() {
                return Err(err);
            }
            Ok(state.snapshot.clone())
        }
    }

    fn s3_spec(name: &str, bucket: &str, region: &str) -> TablespaceStoreSpec {
        let config = StoreConfig::S3(S3StoreConfig {
            region: Some(region.to_string()),
            ..S3StoreConfig::default()
        });
        TablespaceStoreSpec {
            store_id: StoreId::new(name).expect("test store id is valid"),
            tablespace_name: name.to_string(),
            object_namespace: bucket.to_string(),
            base_url: format!("s3://{bucket}"),
            config,
        }
    }

    fn invalid_spec(name: &str) -> TablespaceStoreSpec {
        // S3-compatible without an endpoint fails StoreConfig::validate.
        let config =
            StoreConfig::S3Compatible(pg_lakebase_storage::S3CompatibleStoreConfig {
                endpoint: String::new(),
                region: None,
                access_key_id: None,
                secret_access_key: None,
                token: None,
                allow_http: false,
                virtual_hosted_style_request: false,
                skip_signature: false,
            });
        TablespaceStoreSpec {
            store_id: StoreId::new(name).expect("test store id is valid"),
            tablespace_name: name.to_string(),
            object_namespace: String::new(),
            base_url: String::new(),
            config,
        }
    }

    fn registry_contains(registry: &StoreRegistry, name: &str) -> bool {
        registry.contains(&StoreId::new(name).unwrap())
    }

    #[test]
    fn add_remove_replace_noop_in_one_pass() {
        let source = MockSource::default();
        let registry = StoreRegistry::new();
        let mut reconciler =
            StoreCatalogReconciler::new(source.clone(), registry.clone());

        // Pass 1: register two stores.
        source.set_snapshot(vec![
            s3_spec("ts_a", "bucket-a", "us-east-1"),
            s3_spec("ts_b", "bucket-b", "us-east-2"),
        ]);
        let report = reconciler.apply().unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                added: 2,
                ..ReconcileReport::default()
            }
        );
        assert!(registry_contains(&registry, "ts_a"));
        assert!(registry_contains(&registry, "ts_b"));
        assert_eq!(reconciler.applied_len(), 2);

        // Pass 2: replace ts_a's region, drop ts_b, add ts_c, ts_a counts as
        // replaced because the StoreConfig now differs.
        source.set_snapshot(vec![
            s3_spec("ts_a", "bucket-a", "us-west-2"),
            s3_spec("ts_c", "bucket-c", "eu-west-1"),
        ]);
        let report = reconciler.apply().unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                added: 1,
                replaced: 1,
                removed: 1,
                unchanged: 0,
            }
        );
        assert!(registry_contains(&registry, "ts_a"));
        assert!(!registry_contains(&registry, "ts_b"));
        assert!(registry_contains(&registry, "ts_c"));
        assert_eq!(reconciler.applied_len(), 2);

        // Pass 3: identical snapshot → all unchanged.
        let report = reconciler.apply().unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                unchanged: 2,
                ..ReconcileReport::default()
            }
        );
    }

    #[test]
    fn duplicate_desired_is_rejected_without_touching_registry() {
        let source = MockSource::default();
        let registry = StoreRegistry::new();
        let mut reconciler =
            StoreCatalogReconciler::new(source.clone(), registry.clone());

        source.set_snapshot(vec![
            s3_spec("ts_a", "bucket-a", "us-east-1"),
            s3_spec("ts_a", "bucket-a", "us-east-1"),
        ]);
        let err = reconciler.apply().unwrap_err();
        assert!(matches!(
            err,
            ReconcileError::DuplicateStoreId { store_id } if store_id.as_str() == "ts_a"
        ));
        assert!(!registry_contains(&registry, "ts_a"));
        assert_eq!(reconciler.applied_len(), 0);
    }

    #[test]
    fn source_error_does_not_modify_registry() {
        let source = MockSource::default();
        let registry = StoreRegistry::new();
        let mut reconciler =
            StoreCatalogReconciler::new(source.clone(), registry.clone());

        // First successful pass.
        source.set_snapshot(vec![s3_spec("ts_a", "bucket-a", "us-east-1")]);
        reconciler.apply().unwrap();
        assert!(registry_contains(&registry, "ts_a"));

        // Next load fails: we keep ts_a registered, do not unregister, and do
        // not surface a partial state.
        source.fail_next("boom");
        let err = reconciler.apply().unwrap_err();
        assert!(matches!(err, ReconcileError::Source(_)));
        assert!(registry_contains(&registry, "ts_a"));
        assert_eq!(reconciler.applied_len(), 1);
    }

    #[test]
    fn invalid_spec_aborts_pass_and_keeps_existing_state() {
        let source = MockSource::default();
        let registry = StoreRegistry::new();
        let mut reconciler =
            StoreCatalogReconciler::new(source.clone(), registry.clone());

        // ts_a is valid and gets registered.
        source.set_snapshot(vec![s3_spec("ts_a", "bucket-a", "us-east-1")]);
        reconciler.apply().unwrap();

        // Next snapshot drops ts_a and adds an invalid spec. Up-front
        // validation rejects the new spec before any registry mutation
        // happens, so ts_a stays registered and ts_bad never appears.
        source.set_snapshot(vec![invalid_spec("ts_bad")]);
        let err = reconciler.apply().unwrap_err();
        assert!(matches!(err, ReconcileError::Register { .. }));
        assert!(registry_contains(&registry, "ts_a"));
        assert!(!registry_contains(&registry, "ts_bad"));
        assert_eq!(reconciler.applied_len(), 1);
    }

    /// Compile-time check: `Infallible` is a valid `StoreCatalogSource::Error`.
    #[allow(dead_code)]
    fn assert_infallible_source() {
        struct NoopSource;
        impl StoreCatalogSource for NoopSource {
            type Error = Infallible;
            fn load(&mut self) -> Result<Vec<TablespaceStoreSpec>, Self::Error> {
                Ok(Vec::new())
            }
        }
        let _: StoreCatalogReconciler<NoopSource> =
            StoreCatalogReconciler::new(NoopSource, StoreRegistry::new());
    }
}
