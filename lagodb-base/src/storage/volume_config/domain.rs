use std::collections::{BTreeMap, HashSet};

use lagodb_core::options::TablespaceBinding;
use lagodb_core::storage::volume::StorageVolumeId;
use lagodb_core::storage::volume::StorageVolumeRoute;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::credential::CredentialConfig;
pub(crate) use super::error::StorageVolumeError;
use super::error::{LocationValidationError, SnapshotValidationError};
pub(crate) use super::lifecycle::{StorageVolumeLifecycle, UnixMillis};
pub(crate) use super::name::StorageVolumeName;

pub(crate) const FORMAT_VERSION: u32 = 2;
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "provider", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum StorageLocation {
    S3 {
        bucket: String,
        configured_root_prefix: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        virtual_hosted_style_request: bool,
    },
    Gcs {
        bucket: String,
        configured_root_prefix: String,
        #[serde(default)]
        base_url: Option<String>,
    },
    Azure {
        container: String,
        configured_root_prefix: String,
        #[serde(default)]
        account: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        use_emulator: bool,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3ProviderOptions {
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    allow_http: bool,
    #[serde(default)]
    virtual_hosted_style_request: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GcsProviderOptions {
    #[serde(default)]
    base_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AzureProviderOptions {
    #[serde(default)]
    account: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    allow_http: bool,
    #[serde(default)]
    use_emulator: bool,
}

impl StorageLocation {
    pub(crate) fn parse(
        location: &str,
        provider_options: Value,
    ) -> Result<Self, StorageVolumeError> {
        let (scheme, rest) = location
            .split_once("://")
            .ok_or(LocationValidationError::MissingScheme)?;
        let (namespace, raw_prefix) = rest.split_once('/').unwrap_or((rest, ""));
        validate_namespace(namespace)?;
        let configured_root_prefix = normalize_root_prefix(raw_prefix)?;
        let parsed = match scheme {
            "s3" => {
                let options: S3ProviderOptions =
                    serde_json::from_value(provider_options)
                        .map_err(LocationValidationError::ProviderOptions)?;
                Self::S3 {
                    bucket: namespace.to_owned(),
                    configured_root_prefix,
                    region: options.region,
                    endpoint: options.endpoint,
                    allow_http: options.allow_http,
                    virtual_hosted_style_request: options
                        .virtual_hosted_style_request,
                }
            }
            "gs" => {
                let options: GcsProviderOptions =
                    serde_json::from_value(provider_options)
                        .map_err(LocationValidationError::ProviderOptions)?;
                Self::Gcs {
                    bucket: namespace.to_owned(),
                    configured_root_prefix,
                    base_url: options.base_url,
                }
            }
            "az" => {
                let options: AzureProviderOptions =
                    serde_json::from_value(provider_options)
                        .map_err(LocationValidationError::ProviderOptions)?;
                Self::Azure {
                    container: namespace.to_owned(),
                    configured_root_prefix,
                    account: options.account,
                    endpoint: options.endpoint,
                    allow_http: options.allow_http,
                    use_emulator: options.use_emulator,
                }
            }
            _ => {
                return Err(LocationValidationError::UnsupportedProvider.into());
            }
        };
        parsed.validate()?;
        Ok(parsed)
    }

    pub(crate) fn provider(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "s3",
            Self::Gcs { .. } => "gcs",
            Self::Azure { .. } => "azure",
        }
    }

    pub(crate) fn scheme(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "s3",
            Self::Gcs { .. } => "gs",
            Self::Azure { .. } => "az",
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        match self {
            Self::S3 { bucket, .. } | Self::Gcs { bucket, .. } => bucket,
            Self::Azure { container, .. } => container,
        }
    }

    fn configured_root_prefix(&self) -> &str {
        match self {
            Self::S3 {
                configured_root_prefix,
                ..
            }
            | Self::Gcs {
                configured_root_prefix,
                ..
            }
            | Self::Azure {
                configured_root_prefix,
                ..
            } => configured_root_prefix,
        }
    }

    pub(crate) fn effective_root_for_compact_id(&self, compact_id: &str) -> String {
        if self.configured_root_prefix().is_empty() {
            format!("lagodb/{compact_id}")
        } else {
            format!("{}/lagodb/{compact_id}", self.configured_root_prefix())
        }
    }

    pub(crate) fn effective_location_for_compact_id(
        &self,
        compact_id: &str,
    ) -> String {
        format!(
            "{}://{}/{}",
            self.scheme(),
            self.namespace(),
            self.effective_root_for_compact_id(compact_id)
        )
    }

    fn validate(&self) -> Result<(), LocationValidationError> {
        validate_namespace(self.namespace())?;
        let normalized = normalize_root_prefix(self.configured_root_prefix())?;
        if normalized != self.configured_root_prefix() {
            return Err(LocationValidationError::InvalidRootPrefix);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageVolumeConfig {
    pub(crate) id: StorageVolumeId,
    pub(crate) created_at_ms: UnixMillis,
    pub(crate) location: StorageLocation,
    pub(crate) credential: CredentialConfig,
    pub(crate) lifecycle: StorageVolumeLifecycle,
}

impl StorageVolumeConfig {
    pub(crate) const fn tablespace_binding(&self) -> TablespaceBinding {
        TablespaceBinding::new(self.id)
    }

    pub(crate) fn route(&self) -> Result<StorageVolumeRoute, StorageVolumeError> {
        let compact_id = self.id.to_compact_string();
        StorageVolumeRoute::new(
            self.location.namespace(),
            self.location.effective_location_for_compact_id(&compact_id),
        )
        .map_err(|_| {
            StorageVolumeError::Invariant(
                "validated location produced an invalid route",
            )
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageVolumeSnapshot {
    pub(crate) format_version: u32,
    pub(crate) next_volume_id: u64,
    // Volume IDs remain monotonic even though expired Unbound and Retiring entries
    // are removed from this snapshot.
    pub(crate) volumes: BTreeMap<StorageVolumeName, StorageVolumeConfig>,
}

impl Default for StorageVolumeSnapshot {
    fn default() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            next_volume_id: 1,
            volumes: BTreeMap::new(),
        }
    }
}

impl StorageVolumeSnapshot {
    pub(crate) fn validate(&self) -> Result<(), SnapshotValidationError> {
        if self.format_version != FORMAT_VERSION {
            return Err(SnapshotValidationError::UnsupportedFormat(
                self.format_version,
            ));
        }
        if self.next_volume_id == 0
            || self.next_volume_id > StorageVolumeId::MAX.saturating_add(1)
        {
            return Err(SnapshotValidationError::InvalidNextVolumeId);
        }
        let mut ids = HashSet::with_capacity(self.volumes.len());
        let mut bound_tablespaces = HashSet::new();
        for volume in self.volumes.values() {
            if !volume.created_at_ms.is_positive() {
                return Err(SnapshotValidationError::InvalidCreatedAt(volume.id));
            }
            volume.location.validate().map_err(|source| {
                SnapshotValidationError::InvalidLocation {
                    volume_id: volume.id,
                    source,
                }
            })?;
            volume
                .credential
                .validate_for(&volume.location)
                .map_err(|source| SnapshotValidationError::InvalidCredential {
                    volume_id: volume.id,
                    source,
                })?;
            if !ids.insert(volume.id) {
                return Err(SnapshotValidationError::DuplicateId(volume.id));
            }
            if volume.id.get() >= self.next_volume_id {
                return Err(SnapshotValidationError::IdNotBelowNext(volume.id));
            }
            match &volume.lifecycle {
                StorageVolumeLifecycle::Unbound { expires_at_ms } => {
                    if expires_at_ms
                        .is_some_and(|expires_at| expires_at < volume.created_at_ms)
                    {
                        return Err(SnapshotValidationError::InvalidExpiration(
                            volume.id,
                        ));
                    }
                }
                StorageVolumeLifecycle::Bound { tablespace_oid } => {
                    if *tablespace_oid == pg_sys::InvalidOid.to_u32() {
                        return Err(SnapshotValidationError::InvalidTablespaceOid(
                            volume.id,
                        ));
                    }
                    if !bound_tablespaces.insert(*tablespace_oid) {
                        return Err(
                            SnapshotValidationError::DuplicateBoundTablespace(
                                *tablespace_oid,
                            ),
                        );
                    }
                }
                StorageVolumeLifecycle::Retiring {
                    former_tablespace_oid,
                    marked_at_ms,
                    purge_after_ms,
                } => {
                    if *former_tablespace_oid == pg_sys::InvalidOid.to_u32() {
                        return Err(SnapshotValidationError::InvalidTablespaceOid(
                            volume.id,
                        ));
                    }
                    if *marked_at_ms < volume.created_at_ms {
                        return Err(SnapshotValidationError::InvalidRetirementMark(
                            volume.id,
                        ));
                    }
                    if *purge_after_ms < *marked_at_ms {
                        return Err(SnapshotValidationError::InvalidRetirementPurge(
                            volume.id,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn find(
        &self,
        name: &StorageVolumeName,
    ) -> Result<&StorageVolumeConfig, StorageVolumeError> {
        self.volumes
            .get(name)
            .ok_or_else(|| StorageVolumeError::NotFound(name.as_str().to_owned()))
    }

    pub(crate) fn find_by_id(
        &self,
        id: StorageVolumeId,
    ) -> Option<&StorageVolumeConfig> {
        self.volumes.values().find(|volume| volume.id == id)
    }

    pub(crate) fn find_bound_by_tablespace_oid(
        &self,
        tablespace_oid: u32,
    ) -> Option<StorageVolumeId> {
        self.volumes.values().find_map(|volume| {
            (volume.lifecycle.bound_tablespace_oid() == Some(tablespace_oid))
                .then_some(volume.id)
        })
    }

    pub(crate) fn create(
        &mut self,
        name: StorageVolumeName,
        location: StorageLocation,
        credential: CredentialConfig,
        expires_after_seconds: Option<i64>,
    ) -> Result<(StorageVolumeId, bool), StorageVolumeError> {
        if let Some(seconds) = expires_after_seconds {
            UnixMillis::ttl_millis(seconds)?;
        }
        location.validate_for_persistence(&credential)?;
        if let Some(existing) = self.volumes.get(&name) {
            if existing.location == location && existing.credential == credential {
                // TTL is a creation-time property; idempotent create must not
                // mutate an existing lifecycle.
                return Ok((existing.id, false));
            }
            return Err(StorageVolumeError::NameConflict(name.as_str().to_owned()));
        }
        if self.next_volume_id > StorageVolumeId::MAX {
            return Err(StorageVolumeError::IdExhausted);
        }
        let created_at_ms = UnixMillis::now()?;
        let expires_at_ms = expires_after_seconds
            .map(|seconds| created_at_ms.checked_add_seconds(seconds))
            .transpose()?;
        let id = StorageVolumeId::new(self.next_volume_id).map_err(|_| {
            StorageVolumeError::Invariant(
                "validated next_volume_id is outside the allocation range",
            )
        })?;
        let volume = StorageVolumeConfig {
            id,
            created_at_ms,
            location,
            credential,
            lifecycle: StorageVolumeLifecycle::Unbound { expires_at_ms },
        };
        self.next_volume_id = self
            .next_volume_id
            .checked_add(1)
            .ok_or(StorageVolumeError::IdExhausted)?;
        self.volumes.insert(name, volume);
        Ok((id, true))
    }

    pub(crate) fn rename(
        &mut self,
        old: &StorageVolumeName,
        new: StorageVolumeName,
    ) -> Result<bool, StorageVolumeError> {
        if old == &new {
            self.find(old)?;
            return Ok(false);
        }
        if self.volumes.contains_key(&new) {
            return Err(StorageVolumeError::NameConflict(new.as_str().to_owned()));
        }
        if self.find(old)?.lifecycle.is_retiring() {
            return Err(StorageVolumeError::LifecycleOperation {
                operation: "renamed",
            });
        }
        let volume = self
            .volumes
            .remove(old)
            .ok_or_else(|| StorageVolumeError::NotFound(old.as_str().to_owned()))?;
        self.volumes.insert(new, volume);
        Ok(true)
    }

    pub(crate) fn remove(
        &mut self,
        name: &StorageVolumeName,
    ) -> Result<bool, StorageVolumeError> {
        if !self.find(name)?.lifecycle.is_unbound() {
            return Err(StorageVolumeError::LifecycleOperation {
                operation: "removed",
            });
        }
        self.volumes.remove(name);
        Ok(true)
    }

    pub(crate) fn update_credential(
        &mut self,
        name: &StorageVolumeName,
        credential: CredentialConfig,
    ) -> Result<bool, StorageVolumeError> {
        let volume = self
            .volumes
            .get_mut(name)
            .ok_or_else(|| StorageVolumeError::NotFound(name.as_str().to_owned()))?;
        credential.validate_for(&volume.location)?;
        volume.location.validate_for_persistence(&credential)?;
        if volume.credential == credential {
            return Ok(false);
        }
        volume.credential = credential;
        Ok(true)
    }

    pub(crate) fn bind(
        &mut self,
        id: StorageVolumeId,
        tablespace_oid: u32,
        now: UnixMillis,
    ) -> Result<bool, StorageVolumeError> {
        if self.find_by_id(id).is_none() {
            return Err(StorageVolumeError::NotFoundId(id));
        }
        if let Some(existing_id) = self.find_bound_by_tablespace_oid(tablespace_oid)
            && existing_id != id
        {
            return Err(StorageVolumeError::TablespaceAlreadyBound(tablespace_oid));
        }
        let volume = self
            .volumes
            .values_mut()
            .find(|volume| volume.id == id)
            .ok_or(StorageVolumeError::NotFoundId(id))?;
        volume.lifecycle.bind(tablespace_oid, now)
    }

    pub(crate) fn retire(
        &mut self,
        id: StorageVolumeId,
        tablespace_oid: u32,
        marked_at_ms: UnixMillis,
        retirement_grace_ms: u64,
    ) -> Result<bool, StorageVolumeError> {
        let volume = self
            .volumes
            .values_mut()
            .find(|volume| volume.id == id)
            .ok_or(StorageVolumeError::NotFoundId(id))?;
        volume.lifecycle.retire(
            tablespace_oid,
            volume.created_at_ms,
            marked_at_ms,
            retirement_grace_ms,
        )
    }

    pub(crate) fn repair(
        &mut self,
        name: &StorageVolumeName,
        marked_at_ms: UnixMillis,
        retirement_grace_ms: u64,
    ) -> Result<bool, StorageVolumeError> {
        let volume = self
            .volumes
            .get_mut(name)
            .ok_or_else(|| StorageVolumeError::NotFound(name.as_str().to_owned()))?;
        let Some(tablespace_oid) = volume.lifecycle.bound_tablespace_oid() else {
            return Err(StorageVolumeError::NotBound);
        };
        volume.lifecycle.retire(
            tablespace_oid,
            volume.created_at_ms,
            marked_at_ms,
            retirement_grace_ms,
        )
    }

    pub(crate) fn sweep_due(&mut self, now: UnixMillis) -> bool {
        let before = self.volumes.len();
        self.volumes.retain(|_, volume| {
            let due = volume
                .lifecycle
                .expires_at_ms()
                .or_else(|| volume.lifecycle.purge_after_ms())
                .is_some_and(|deadline| deadline <= now);
            !due
        });
        self.volumes.len() != before
    }
}

fn validate_namespace(namespace: &str) -> Result<(), LocationValidationError> {
    if namespace.is_empty()
        || namespace.contains('/')
        || namespace.contains('\\')
        || namespace.as_bytes().contains(&0)
    {
        return Err(LocationValidationError::InvalidNamespace);
    }
    Ok(())
}

fn normalize_root_prefix(prefix: &str) -> Result<String, LocationValidationError> {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        return Ok(String::new());
    }
    if prefix.starts_with('/')
        || prefix.contains('\\')
        || prefix.contains("://")
        || prefix.as_bytes().contains(&0)
        || prefix
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(LocationValidationError::InvalidRootPrefix);
    }
    Ok(prefix.to_owned())
}
