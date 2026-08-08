use std::collections::HashMap;
use std::sync::Arc;

use pg_lakebase_storage::ManagedStoreRegistry;

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
    assert!(fixture.registry.resolve(valid.volume_id).is_ok());
    assert!(fixture.registry.resolve(invalid.volume_id).is_err());
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
    assert!(fixture.registry.resolve(corrected.volume_id).is_ok());
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
    assert!(fixture.registry.resolve(valid.volume_id).is_ok());

    let reverted = fixture.apply([valid], false);
    assert_eq!(reverted.unchanged, 1);
    assert_eq!(reverted.stale, 0);
    assert!(reverted.failures.is_empty());
}

#[test]
fn forced_default_chain_reload_publishes_a_fresh_backend() {
    let mut fixture = ReconcileFixture::new();
    let spec = ReconcileFixture::valid_default_chain_spec(1);
    fixture.apply([spec.clone()], false);
    let slot = fixture.registry.resolve(spec.volume_id).unwrap();
    let before = slot.backend();

    let report = fixture.apply([spec], true);
    let after = slot.backend();

    assert_eq!(report.replaced, 1);
    assert!(!Arc::ptr_eq(&before, &after));
}
