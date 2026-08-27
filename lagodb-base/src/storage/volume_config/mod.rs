mod backend;
mod binding;
mod control;
mod credential;
mod domain;
mod error;
mod lifecycle;
mod name;
mod retirement;
mod service_account;
mod sql_api;
mod store;

use lagodb_core::storage::volume::{StorageVolumeId, StorageVolumeRoute};

pub(crate) use binding::{handles_utility, utility_post, utility_pre};
pub(crate) use credential::CredentialConfig;
pub(crate) use domain::{StorageLocation, StorageVolumeError, UnixMillis};
pub(crate) use retirement::on_object_access;
pub(crate) use store::StorageVolumeConfigStore;

pub(crate) fn resolve_route(
    id: StorageVolumeId,
) -> Result<Option<StorageVolumeRoute>, StorageVolumeError> {
    let snapshot = StorageVolumeConfigStore::for_current_data_directory().read()?;
    snapshot
        .find_by_id(id)
        .map(domain::StorageVolumeConfig::route)
        .transpose()
}
