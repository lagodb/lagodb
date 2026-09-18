//! Pure-Rust storage-volume snapshot diff and registry publication.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;

use lagodb_core::diag::error_source_chain_detail;
use lagodb_storage::{ManagedStoreRegistry, StorageError, StoreConfig};

use super::volume_config::{CredentialConfig, StorageLocation, StorageVolumeError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumeStoreSpec {
    pub volume_id: u64,
    pub location: StorageLocation,
    pub credential: CredentialConfig,
    pub reload_on_force: bool,
}

impl VolumeStoreSpec {
    fn materialize_store_config(&self) -> Result<StoreConfig, StorageVolumeError> {
        let config = self.location.store_config(&self.credential)?;
        config.validate_for_bucket(self.location.namespace())?;
        Ok(config)
    }
}

pub(crate) trait StoreConfigSource {
    fn load(&mut self) -> Result<Vec<VolumeStoreSpec>, StorageVolumeError>;
}

#[derive(Debug)]
pub(crate) enum ReconcileError {
    Source(StorageVolumeError),
    DuplicateVolumeId(u64),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => {
                write!(f, "failed to load storage volume config: {error}")
            }
            Self::DuplicateVolumeId(id) => {
                write!(f, "duplicate storage volume id {id}")
            }
        }
    }
}

impl StdError for ReconcileError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::DuplicateVolumeId(_) => None,
        }
    }
}

impl ReconcileError {
    pub(crate) fn diagnostic_message(&self) -> String {
        match error_source_chain_detail(self) {
            Some(detail) => format!("{self}\n{detail}"),
            None => self.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumeApplyState {
    Stale,
    Unavailable,
}

impl VolumeApplyState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stale => "stale",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumeApplyFailure {
    pub volume_id: u64,
    pub state: VolumeApplyState,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReconcileReport {
    pub added: usize,
    pub removed: usize,
    pub replaced: usize,
    pub unchanged: usize,
    pub desired: usize,
    pub loaded: usize,
    pub stale: usize,
    pub unavailable: usize,
    pub failures: Vec<VolumeApplyFailure>,
}

#[derive(Debug)]
enum VolumeApplyError {
    Prepare(StorageVolumeError),
    Register(StorageError),
}

impl fmt::Display for VolumeApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepare(error) => {
                write!(f, "store configuration is invalid: {error}")
            }
            Self::Register(error) => {
                write!(f, "failed to register store configuration: {error}")
            }
        }
    }
}

impl StdError for VolumeApplyError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Prepare(error) => Some(error),
            Self::Register(error) => Some(error),
        }
    }
}

impl VolumeApplyError {
    fn diagnostic_message(&self) -> String {
        match error_source_chain_detail(self) {
            Some(detail) => format!("{self}\n{detail}"),
            None => self.to_string(),
        }
    }
}

pub(crate) struct StoreConfigReconciler<S> {
    source: S,
    registry: ManagedStoreRegistry,
    applied: HashMap<u64, VolumeStoreSpec>,
    rejected: HashMap<u64, VolumeStoreSpec>,
}

impl<S: StoreConfigSource> StoreConfigReconciler<S> {
    pub(crate) fn new(source: S, registry: ManagedStoreRegistry) -> Self {
        Self {
            source,
            registry,
            applied: HashMap::new(),
            rejected: HashMap::new(),
        }
    }

    pub(crate) fn load_desired(
        &mut self,
    ) -> Result<HashMap<u64, VolumeStoreSpec>, ReconcileError> {
        let mut desired = HashMap::new();
        for spec in self.source.load().map_err(ReconcileError::Source)? {
            let id = spec.volume_id;
            if desired.insert(id, spec).is_some() {
                return Err(ReconcileError::DuplicateVolumeId(id));
            }
        }
        Ok(desired)
    }

    /// Apply stores independently while retaining each store's last-known-good
    /// registration when its desired replacement cannot be materialized.
    pub(crate) fn apply_desired(
        &mut self,
        desired: HashMap<u64, VolumeStoreSpec>,
        force_default_chain: bool,
    ) -> Result<ReconcileReport, ReconcileError> {
        let mut report = ReconcileReport {
            desired: desired.len(),
            ..ReconcileReport::default()
        };

        let removed_applied: Vec<u64> = self
            .applied
            .keys()
            .filter(|id| !desired.contains_key(*id))
            .cloned()
            .collect();
        for id in removed_applied {
            self.registry.remove(id);
            self.applied.remove(&id);
            self.rejected.remove(&id);
            report.removed += 1;
        }

        let removed_rejected: Vec<u64> = self
            .rejected
            .keys()
            .filter(|id| !desired.contains_key(*id))
            .cloned()
            .collect();
        for id in removed_rejected {
            self.rejected.remove(&id);
            report.removed += 1;
        }

        for (id, spec) in desired {
            let rejected_same = self.rejected.get(&id) == Some(&spec);
            let applied_same = self.applied.get(&id) == Some(&spec);
            let force_reload = force_default_chain && spec.reload_on_force;

            if rejected_same && !force_reload {
                continue;
            }
            if applied_same && !rejected_same {
                self.rejected.remove(&id);
                if !force_reload {
                    report.unchanged += 1;
                    continue;
                }
            }

            let apply_result = spec
                .materialize_store_config()
                .map_err(VolumeApplyError::Prepare)
                .and_then(|config| {
                    let result = if force_reload {
                        self.registry.refresh_config(id, config)
                    } else {
                        self.registry.replace_config(id, config)
                    };
                    result.map(|_| ()).map_err(VolumeApplyError::Register)
                });
            match apply_result {
                Ok(()) => {
                    if self.applied.insert(id, spec).is_some() {
                        report.replaced += 1;
                    } else {
                        report.added += 1;
                    }
                    self.rejected.remove(&id);
                }
                Err(error) => {
                    let state = if self.applied.contains_key(&id) {
                        VolumeApplyState::Stale
                    } else {
                        VolumeApplyState::Unavailable
                    };
                    let message = error.diagnostic_message();
                    self.rejected.insert(id, spec);
                    report.failures.push(VolumeApplyFailure {
                        volume_id: id,
                        state,
                        message,
                    });
                }
            }
        }

        report.loaded = self.applied.len();
        for id in self.rejected.keys() {
            if self.applied.contains_key(id) {
                report.stale += 1;
            } else {
                report.unavailable += 1;
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmptySource;

    impl StoreConfigSource for EmptySource {
        fn load(&mut self) -> Result<Vec<VolumeStoreSpec>, StorageVolumeError> {
            Ok(Vec::new())
        }
    }

    struct ReconcileFixture {
        reconciler: StoreConfigReconciler<EmptySource>,
        registry: ManagedStoreRegistry,
    }

    impl ReconcileFixture {
        fn new() -> Self {
            let registry = ManagedStoreRegistry::new();
            Self {
                reconciler: StoreConfigReconciler::new(EmptySource, registry.clone()),
                registry,
            }
        }

        fn apply<const N: usize>(
            &mut self,
            specs: [VolumeStoreSpec; N],
            force_default_chain: bool,
        ) -> ReconcileReport {
            let desired = specs
                .into_iter()
                .map(|spec| (spec.volume_id, spec))
                .collect::<HashMap<_, _>>();
            self.reconciler
                .apply_desired(desired, force_default_chain)
                .unwrap()
        }

        fn valid_spec(volume_id: u64) -> VolumeStoreSpec {
            VolumeStoreSpec {
                volume_id,
                location: StorageLocation::S3 {
                    bucket: "bucket".to_owned(),
                    configured_root_prefix: String::new(),
                    region: Some("us-east-1".to_owned()),
                    endpoint: Some("http://127.0.0.1:9000".to_owned()),
                    allow_http: true,
                    virtual_hosted_style_request: false,
                },
                credential: CredentialConfig::Anonymous,
                reload_on_force: false,
            }
        }

        fn invalid_default_chain_spec(volume_id: u64) -> VolumeStoreSpec {
            VolumeStoreSpec {
                volume_id,
                location: StorageLocation::Azure {
                    container: "container".to_owned(),
                    configured_root_prefix: String::new(),
                    account: Some("invalid account".to_owned()),
                    endpoint: None,
                    allow_http: false,
                    use_emulator: false,
                },
                credential: CredentialConfig::DefaultChain,
                reload_on_force: true,
            }
        }

        fn valid_default_chain_spec(volume_id: u64) -> VolumeStoreSpec {
            let mut spec = Self::valid_spec(volume_id);
            spec.credential = CredentialConfig::DefaultChain;
            spec.reload_on_force = true;
            spec
        }
    }

    #[test]
    fn invalid_volume_does_not_block_valid_volume() {
        let mut fixture = ReconcileFixture::new();
        let valid = ReconcileFixture::valid_spec(1);
        let invalid = ReconcileFixture::invalid_default_chain_spec(2);

        let report = fixture.apply([valid.clone(), invalid.clone()], false);

        assert_eq!(report.desired, 2);
        assert_eq!(report.loaded, 1);
        assert_eq!(report.added, 1);
        assert_eq!(report.unavailable, 1);
        assert_eq!(report.stale, 0);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].state, VolumeApplyState::Unavailable);
        assert!(fixture.registry.contains(valid.volume_id));
        assert!(!fixture.registry.contains(invalid.volume_id));
    }

    #[test]
    fn rejected_spec_is_retried_only_after_force_or_change() {
        let mut fixture = ReconcileFixture::new();
        let invalid = ReconcileFixture::invalid_default_chain_spec(1);
        let initial = fixture.apply([invalid.clone()], false);
        assert_eq!(initial.failures.len(), 1);

        let periodic = fixture.apply([invalid.clone()], false);
        assert!(periodic.failures.is_empty());
        assert_eq!(periodic.unavailable, 1);

        let forced = fixture.apply([invalid.clone()], true);
        assert_eq!(forced.failures.len(), 1);
        assert_eq!(forced.unavailable, 1);

        let corrected = ReconcileFixture::valid_spec(1);
        let recovered = fixture.apply([corrected.clone()], false);
        assert_eq!(recovered.added, 1);
        assert_eq!(recovered.loaded, 1);
        assert_eq!(recovered.unavailable, 0);
        assert!(recovered.failures.is_empty());
        assert!(fixture.registry.contains(corrected.volume_id));
    }

    #[test]
    fn failed_replacement_keeps_last_known_good_store() {
        let mut fixture = ReconcileFixture::new();
        let valid = ReconcileFixture::valid_spec(1);
        let initial = fixture.apply([valid.clone()], false);
        assert_eq!(initial.added, 1);

        let invalid = ReconcileFixture::invalid_default_chain_spec(1);
        let degraded = fixture.apply([invalid], false);
        assert_eq!(degraded.loaded, 1);
        assert_eq!(degraded.stale, 1);
        assert_eq!(degraded.unavailable, 0);
        assert_eq!(degraded.failures[0].state, VolumeApplyState::Stale);
        assert!(fixture.registry.contains(valid.volume_id));

        let reverted = fixture.apply([valid], false);
        assert_eq!(reverted.unchanged, 1);
        assert_eq!(reverted.stale, 0);
        assert!(reverted.failures.is_empty());
    }

    #[test]
    fn forced_default_chain_reload_is_reported_as_a_replacement() {
        let mut fixture = ReconcileFixture::new();
        let spec = ReconcileFixture::valid_default_chain_spec(1);
        fixture.apply([spec.clone()], false);
        let report = fixture.apply([spec], true);

        assert_eq!(report.replaced, 1);
        assert!(fixture.registry.contains(1));
    }
}
