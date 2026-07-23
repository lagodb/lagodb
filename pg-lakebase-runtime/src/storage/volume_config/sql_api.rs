//! PostgreSQL SQL API adapters for Storage Volume administration and inspection.

use std::ffi::CStr;

use pg_lakebase_core::diag::{PgReportError, SqlStateError};
use pg_lakebase_core::options::{TablespaceCacheError, get_tablespace};
use pg_lakebase_core::storage_service::BackendStorageService;
use pgrx::datum::JsonB;
use pgrx::prelude::*;

use super::control::StorageVolumeControl;
use super::credential::CredentialConfig;
use super::domain::{StorageLocation, StorageVolumeError, StorageVolumeName};

#[derive(Debug, thiserror::Error)]
enum StorageVolumeSqlError {
    #[error("storage volume administration requires superuser")]
    RequiresSuperuser,
    #[error(transparent)]
    Domain(#[from] StorageVolumeError),
    #[error(transparent)]
    Tablespace(#[from] TablespaceCacheError),
}

impl SqlStateError for StorageVolumeSqlError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::RequiresSuperuser => PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            Self::Domain(error) => error.sql_error_code(),
            Self::Tablespace(error) => error.sql_error_code(),
        }
    }
}

impl StorageVolumeSqlError {
    fn report(self) -> ! {
        PgReportError::from_domain_error(self).report()
    }
}

fn ensure_admin_access() -> Result<(), StorageVolumeSqlError> {
    if !unsafe { pg_sys::superuser() } {
        return Err(StorageVolumeSqlError::RequiresSuperuser);
    }
    crate::ensure_runtime_preloaded();
    Ok(())
}

fn ensure_mutation_context() -> Result<(), StorageVolumeSqlError> {
    ensure_admin_access()?;
    // A scalar SQL function has no ProcessUtility top-level context. Passing
    // `true` preserves the supported standalone SELECT API while PostgreSQL
    // rejects explicit blocks, subtransactions and an established pipeline,
    // and commits the implicit transaction as soon as the statement succeeds.
    unsafe {
        pg_sys::PreventInTransactionBlock(true, c"storage volume mutation".as_ptr())
    };
    Ok(())
}

#[pg_schema]
mod lakebase {
    use super::*;

    #[pg_extern]
    fn create_storage_volume(
        storage_volume_name: &str,
        location: &str,
        credentials: default!(JsonB, "'{\"type\":\"default_chain\"}'::jsonb"),
        provider_options: default!(JsonB, "'{}'::jsonb"),
    ) -> String {
        let result = (|| -> Result<String, StorageVolumeSqlError> {
            ensure_mutation_context()?;
            let name = StorageVolumeName::new(storage_volume_name)?;
            let location = StorageLocation::parse(location, provider_options.0)?;
            let credential = CredentialConfig::parse(credentials.0, &location)?;
            StorageVolumeControl::current().create(&name, location, credential)?;
            Ok(name.as_str().to_owned())
        })();
        result.unwrap_or_else(|error| error.report())
    }

    #[pg_extern]
    fn rename_storage_volume(storage_volume_name: &str, new_name: &str) {
        let result = (|| -> Result<(), StorageVolumeSqlError> {
            ensure_mutation_context()?;
            let old = StorageVolumeName::new(storage_volume_name)?;
            let new = StorageVolumeName::new(new_name)?;
            StorageVolumeControl::current().rename(&old, new)?;
            Ok(())
        })();
        result.unwrap_or_else(|error| error.report())
    }

    #[pg_extern]
    fn update_storage_volume_credentials(
        storage_volume_name: &str,
        credentials: JsonB,
    ) {
        let result = (|| -> Result<(), StorageVolumeSqlError> {
            ensure_mutation_context()?;
            let name = StorageVolumeName::new(storage_volume_name)?;
            StorageVolumeControl::current()
                .update_credential(&name, credentials.0)?;
            Ok(())
        })();
        result.unwrap_or_else(|error| error.report())
    }

    #[pg_extern]
    fn reload_storage_volumes() {
        ensure_admin_access().unwrap_or_else(|error| error.report());
        StorageVolumeControl::request_reload(true);
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn probe_storage_volume(
        storage_volume_name: &str,
    ) -> TableIterator<
        'static,
        (
            name!(storage_volume_name, String),
            name!(internal_store_id, String),
            name!(object_namespace, String),
            name!(list_succeeded, bool),
            name!(write_succeeded, bool),
            name!(read_succeeded, bool),
            name!(delete_succeeded, bool),
            name!(succeeded, bool),
            name!(error, Option<String>),
        ),
    > {
        ensure_admin_access().unwrap_or_else(|error| error.report());
        let metadata = (|| -> Result<_, StorageVolumeSqlError> {
            let name = StorageVolumeName::new(storage_volume_name)?;
            let snapshot = StorageVolumeControl::current().snapshot()?;
            let volume = snapshot.find(&name)?;
            let store_id = volume.store_id();
            let namespace = volume.location.namespace().to_owned();
            let root_prefix = volume.location.effective_root_for_store_id(&store_id);
            Ok((
                name.as_str().to_owned(),
                store_id.as_str().to_owned(),
                namespace,
                root_prefix,
            ))
        })()
        .unwrap_or_else(|error| error.report());
        let (name, store_id, namespace, root_prefix) = metadata;

        let endpoint = crate::storage::resolved_endpoint();
        let probe = BackendStorageService::from_endpoint(&endpoint).probe_store(
            store_id.as_str(),
            namespace.as_str(),
            root_prefix.as_str(),
        );
        let row = match probe {
            Ok(result) => (
                name,
                store_id,
                namespace,
                result.list_succeeded(),
                result.write_succeeded(),
                result.read_succeeded(),
                result.delete_succeeded(),
                result.succeeded(),
                result.error().map(str::to_owned),
            ),
            Err(error) => (
                name,
                store_id,
                namespace,
                false,
                false,
                false,
                false,
                false,
                Some(error.wire_message()),
            ),
        };
        TableIterator::new(std::iter::once(row))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn storage_volumes_internal() -> TableIterator<
        'static,
        (
            name!(storage_volume_name, String),
            name!(provider, &'static str),
            name!(effective_location, String),
            name!(credential_type, &'static str),
            name!(bound_tablespace_oid, Option<pg_sys::Oid>),
            name!(bound_tablespace_name, Option<String>),
            name!(internal_store_id, String),
            name!(internal_volume_id, i64),
        ),
    > {
        ensure_admin_access().unwrap_or_else(|error| error.report());
        let snapshot = StorageVolumeControl::current()
            .snapshot()
            .map_err(StorageVolumeSqlError::from)
            .unwrap_or_else(|error| error.report());
        let rows = snapshot.volumes.into_iter().map(|(name, volume)| {
            let store_id = volume.id.to_store_id();
            let tablespace_oid = volume.bound_tablespace_oid.map(pg_sys::Oid::from);
            let tablespace_name = tablespace_oid.and_then(|oid| {
                let binding_matches = get_tablespace(oid)
                    .unwrap_or_else(|error| {
                        StorageVolumeSqlError::from(error).report()
                    })
                    .is_some_and(|options| options.volume_id() == volume.id);
                if !binding_matches {
                    return None;
                }
                // SAFETY: get_tablespace_name returns null or a palloc'd C string
                // valid in the current query context; copy it immediately.
                let name = unsafe { pg_sys::get_tablespace_name(oid) };
                (!name.is_null()).then(|| unsafe {
                    CStr::from_ptr(name).to_string_lossy().into_owned()
                })
            });
            (
                name.as_str().to_owned(),
                volume.location.provider(),
                volume.location.effective_location_for_store_id(&store_id),
                volume.credential.credential_type(),
                tablespace_oid,
                tablespace_name,
                store_id.as_str().to_owned(),
                volume.id.as_i64(),
            )
        });
        TableIterator::new(rows)
    }
}
