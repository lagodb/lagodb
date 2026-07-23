use std::collections::HashMap;

use pg_lakebase_storage::{StoreId, StoreRegistry};

use super::{
    ReconcileReport, StoreConfigReconciler, StoreConfigSource, VolumeApplyState,
    VolumeStoreSpec,
};
use crate::storage::volume_config::{
    CredentialConfig, StorageLocation, StorageVolumeError,
};

struct EmptySource;

impl StoreConfigSource for EmptySource {
    fn load(&mut self) -> Result<Vec<VolumeStoreSpec>, StorageVolumeError> {
        Ok(Vec::new())
    }
}

struct ReconcileFixture {
    reconciler: StoreConfigReconciler<EmptySource>,
    registry: StoreRegistry,
}

impl ReconcileFixture {
    fn new() -> Self {
        let registry = StoreRegistry::new();
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
            .map(|spec| (spec.store_id.clone(), spec))
            .collect::<HashMap<_, _>>();
        self.reconciler
            .apply_desired(desired, force_default_chain)
            .unwrap()
    }

    fn valid_spec(id: &str) -> VolumeStoreSpec {
        VolumeStoreSpec {
            store_id: StoreId::new(id).unwrap(),
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

    fn invalid_default_chain_spec(id: &str) -> VolumeStoreSpec {
        VolumeStoreSpec {
            store_id: StoreId::new(id).unwrap(),
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
}

#[test]
fn invalid_volume_does_not_block_valid_volume() {
    let mut fixture = ReconcileFixture::new();
    let valid = ReconcileFixture::valid_spec("valid");
    let invalid = ReconcileFixture::invalid_default_chain_spec("invalid");

    let report = fixture.apply([valid.clone(), invalid.clone()], false);

    assert_eq!(report.desired, 2);
    assert_eq!(report.loaded, 1);
    assert_eq!(report.added, 1);
    assert_eq!(report.unavailable, 1);
    assert_eq!(report.stale, 0);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].state, VolumeApplyState::Unavailable);
    assert!(fixture.registry.contains(&valid.store_id));
    assert!(!fixture.registry.contains(&invalid.store_id));
}

#[test]
fn rejected_spec_is_retried_only_after_force_or_change() {
    let mut fixture = ReconcileFixture::new();
    let invalid = ReconcileFixture::invalid_default_chain_spec("volume");
    let initial = fixture.apply([invalid.clone()], false);
    assert_eq!(initial.failures.len(), 1);

    let periodic = fixture.apply([invalid.clone()], false);
    assert!(periodic.failures.is_empty());
    assert_eq!(periodic.unavailable, 1);

    let forced = fixture.apply([invalid.clone()], true);
    assert_eq!(forced.failures.len(), 1);
    assert_eq!(forced.unavailable, 1);

    let corrected = ReconcileFixture::valid_spec("volume");
    let recovered = fixture.apply([corrected.clone()], false);
    assert_eq!(recovered.added, 1);
    assert_eq!(recovered.loaded, 1);
    assert_eq!(recovered.unavailable, 0);
    assert!(recovered.failures.is_empty());
    assert!(fixture.registry.contains(&corrected.store_id));
}

#[test]
fn failed_replacement_keeps_last_known_good_store() {
    let mut fixture = ReconcileFixture::new();
    let valid = ReconcileFixture::valid_spec("volume");
    let initial = fixture.apply([valid.clone()], false);
    assert_eq!(initial.added, 1);

    let invalid = ReconcileFixture::invalid_default_chain_spec("volume");
    let degraded = fixture.apply([invalid], false);
    assert_eq!(degraded.loaded, 1);
    assert_eq!(degraded.stale, 1);
    assert_eq!(degraded.unavailable, 0);
    assert_eq!(degraded.failures[0].state, VolumeApplyState::Stale);
    assert!(fixture.registry.contains(&valid.store_id));

    let reverted = fixture.apply([valid], false);
    assert_eq!(reverted.unchanged, 1);
    assert_eq!(reverted.stale, 0);
    assert!(reverted.failures.is_empty());
}
