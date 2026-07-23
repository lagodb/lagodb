//! Pure-Rust storage-volume snapshot diff and registry publication.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;

use pg_lakebase_core::diag::error_source_chain_detail;
use pg_lakebase_storage::{StoreConfig, StoreId, StoreRegistry};

use super::volume_config::{CredentialConfig, StorageLocation, StorageVolumeError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumeStoreSpec {
    pub store_id: StoreId,
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
    DuplicateStoreId(StoreId),
    RemovedStore(StoreId),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => {
                write!(f, "failed to load storage volume config: {error}")
            }
            Self::DuplicateStoreId(id) => {
                write!(f, "duplicate storage volume store id {id}")
            }
            Self::RemovedStore(id) => {
                write!(f, "storage volume store {id} disappeared from config")
            }
        }
    }
}

impl StdError for ReconcileError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::DuplicateStoreId(_) | Self::RemovedStore(_) => None,
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
    pub store_id: StoreId,
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
    Register(pg_lakebase_storage::StorageError),
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
    registry: StoreRegistry,
    applied: HashMap<StoreId, VolumeStoreSpec>,
    rejected: HashMap<StoreId, VolumeStoreSpec>,
}

impl<S: StoreConfigSource> StoreConfigReconciler<S> {
    pub(crate) fn new(source: S, registry: StoreRegistry) -> Self {
        Self {
            source,
            registry,
            applied: HashMap::new(),
            rejected: HashMap::new(),
        }
    }

    pub(crate) fn load_desired(
        &mut self,
    ) -> Result<HashMap<StoreId, VolumeStoreSpec>, ReconcileError> {
        let mut desired = HashMap::new();
        for spec in self.source.load().map_err(ReconcileError::Source)? {
            let id = spec.store_id.clone();
            if desired.insert(id.clone(), spec).is_some() {
                return Err(ReconcileError::DuplicateStoreId(id));
            }
        }
        Ok(desired)
    }

    /// Apply stores independently while retaining each store's last-known-good
    /// registration when its desired replacement cannot be materialized.
    pub(crate) fn apply_desired(
        &mut self,
        desired: HashMap<StoreId, VolumeStoreSpec>,
        force_default_chain: bool,
    ) -> Result<ReconcileReport, ReconcileError> {
        for id in self.applied.keys() {
            if !desired.contains_key(id) {
                return Err(ReconcileError::RemovedStore(id.clone()));
            }
        }
        for id in self.rejected.keys() {
            if !self.applied.contains_key(id) && !desired.contains_key(id) {
                return Err(ReconcileError::RemovedStore(id.clone()));
            }
        }

        let mut report = ReconcileReport {
            desired: desired.len(),
            ..ReconcileReport::default()
        };
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
                    self.registry
                        .register_config(id.clone(), config)
                        .map(|_| ())
                        .map_err(VolumeApplyError::Register)
                });
            match apply_result {
                Ok(()) => {
                    if self.applied.insert(id.clone(), spec).is_some() {
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
                    self.rejected.insert(id.clone(), spec);
                    report.failures.push(VolumeApplyFailure {
                        store_id: id,
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
