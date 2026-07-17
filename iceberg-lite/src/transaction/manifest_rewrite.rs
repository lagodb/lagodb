//! Manifest consolidation planning independent of snapshot production.

use std::collections::{HashMap, HashSet};

use crate::spec::{ManifestContentType, ManifestFile};
use crate::{Error, ErrorKind, Result};

type ManifestGroup = (i32, ManifestContentType);

pub(super) struct ManifestRewritePlan {
    groups: HashMap<ManifestGroup, Vec<ManifestFile>>,
    selected: HashSet<String>,
}

impl ManifestRewritePlan {
    pub(super) fn build(
        manifests: &[ManifestFile],
        min_count_to_merge: usize,
        target_size_bytes: u64,
    ) -> Result<Self> {
        if target_size_bytes == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "manifest rewrite target size must be greater than zero",
            ));
        }
        let mut groups: HashMap<ManifestGroup, Vec<ManifestFile>> = HashMap::new();
        for manifest in manifests {
            if manifest.has_added_files() || manifest.has_existing_files() {
                groups
                    .entry((manifest.partition_spec_id, manifest.content))
                    .or_default()
                    .push(manifest.clone());
            }
        }

        let mut selected = HashSet::new();
        for manifests in groups.values() {
            let total_bytes =
                manifests.iter().try_fold(0_u64, |total, manifest| {
                    let length =
                        u64::try_from(manifest.manifest_length).map_err(|_| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "negative manifest length",
                            )
                        })?;
                    total.checked_add(length).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "manifest byte count overflow",
                        )
                    })
                })?;
            let estimated_outputs = total_bytes.div_ceil(target_size_bytes).max(1);
            let input_count = u64::try_from(manifests.len()).map_err(|_| {
                Error::new(ErrorKind::DataInvalid, "manifest count does not fit u64")
            })?;
            if manifests.len() >= min_count_to_merge
                && estimated_outputs < input_count
            {
                selected.extend(
                    manifests
                        .iter()
                        .map(|manifest| manifest.manifest_path.clone()),
                );
            }
        }
        Ok(Self { groups, selected })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }

    pub(super) fn into_parts(
        self,
    ) -> (HashMap<ManifestGroup, Vec<ManifestFile>>, HashSet<String>) {
        (self.groups, self.selected)
    }
}
