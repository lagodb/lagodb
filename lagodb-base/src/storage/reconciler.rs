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
#[path = "reconciler/tests.rs"]
mod tests;
