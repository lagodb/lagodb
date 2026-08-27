//! Consumer-side API for the runtime-owned storage service.
//!
//! The `lagodb_base` runtime extension is the only crate that registers storage
//! GUC backing statics and the static storage background worker. Access-method
//! crates use this module only to discover the service endpoint that PostgreSQL
//! has already registered globally.

use std::ffi::CStr;
use std::path::{Path, PathBuf};

use lagodb_storage::{StorageError, StorageResult};
use pgrx::pg_sys;

mod backend;
mod connection;
mod injection_points;
mod socket_wait;

pub use backend::BackendStorageService;

const ENABLED_GUC: &CStr = c"lagodb.storage_server_enabled";
const SOCKET_PATH_GUC: &CStr = c"lagodb.storage_server_socket_path";
const CACHE_DIR_GUC: &CStr = c"lagodb.storage_server_cache_dir";
const MAX_IDLE_CONNECTIONS_GUC: &CStr =
    c"lagodb.storage_backend_max_idle_connections";
const DEFAULT_SOCKET_FILE: &str = "storage.sock";
const DEFAULT_CACHE_DIR: &str = "storage-cache";

/// Resolved endpoint for the cluster-local LagoDB storage service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageEndpoint {
    enabled: bool,
    socket_path: PathBuf,
    cache_dir: PathBuf,
    max_idle_connections: usize,
}

impl StorageEndpoint {
    /// Resolve storage service settings from PostgreSQL's global GUC registry.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the runtime extension did not
    /// register the storage GUCs or when PostgreSQL returns an unexpected GUC
    /// value.
    pub fn from_pg_gucs() -> StorageResult<Self> {
        Self::from_config(
            read_bool_guc(ENABLED_GUC)?,
            read_optional_path_guc(SOCKET_PATH_GUC)?,
            read_optional_path_guc(CACHE_DIR_GUC)?,
            read_positive_usize_guc(MAX_IDLE_CONNECTIONS_GUC)?,
        )
    }

    /// Resolve an endpoint from already-read storage service settings.
    ///
    /// Callers that own the GUC backing statics can use this path to avoid
    /// re-reading PostgreSQL's global GUC registry while still sharing the
    /// runtime default path rules.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when PostgreSQL's `DataDir` has not been
    /// initialized and a default path is required.
    pub fn from_config(
        enabled: bool,
        socket_path: Option<PathBuf>,
        cache_dir: Option<PathBuf>,
        max_idle_connections: usize,
    ) -> StorageResult<Self> {
        if max_idle_connections == 0 {
            return Err(StorageError::configuration(
                "storage backend max idle connections must be positive",
            ));
        }
        let (socket_path, cache_dir) = match (socket_path, cache_dir) {
            (Some(socket_path), Some(cache_dir)) => (socket_path, cache_dir),
            (socket_path, cache_dir) => {
                let base = data_dir_base()?;
                (
                    socket_path.unwrap_or_else(|| base.join(DEFAULT_SOCKET_FILE)),
                    cache_dir.unwrap_or_else(|| base.join(DEFAULT_CACHE_DIR)),
                )
            }
        };

        Ok(Self {
            enabled,
            socket_path,
            cache_dir,
            max_idle_connections,
        })
    }

    /// Require the storage service to be enabled before returning the endpoint.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when
    /// `lagodb.storage_server_enabled = off`.
    pub fn require_enabled(self) -> StorageResult<Self> {
        if self.enabled {
            Ok(self)
        } else {
            Err(StorageError::configuration(
                "LagoDB storage server is disabled",
            ))
        }
    }

    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    pub const fn max_idle_connections(&self) -> usize {
        self.max_idle_connections
    }

    pub fn into_parts(self) -> (bool, PathBuf, PathBuf) {
        (self.enabled, self.socket_path, self.cache_dir)
    }
}

fn read_bool_guc(name: &CStr) -> StorageResult<bool> {
    let value = read_required_guc(name)?;
    match value.as_str() {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        _ => Err(StorageError::configuration(format!(
            "unexpected boolean value '{}' for {}",
            value,
            name.to_string_lossy(),
        ))),
    }
}

fn read_optional_path_guc(name: &CStr) -> StorageResult<Option<PathBuf>> {
    read_required_guc(name).map(|value| {
        if value.is_empty() {
            None
        } else {
            Some(PathBuf::from(value))
        }
    })
}

fn read_positive_usize_guc(name: &CStr) -> StorageResult<usize> {
    let value = read_required_guc(name)?;
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            StorageError::configuration(format!(
                "unexpected positive integer value '{}' for {}",
                value,
                name.to_string_lossy(),
            ))
        })
}

fn read_required_guc(name: &CStr) -> StorageResult<String> {
    read_guc(name)?.ok_or_else(|| {
        StorageError::configuration(format!(
            "{} is not registered; preload the lagodb_base runtime extension",
            name.to_string_lossy(),
        ))
    })
}

fn read_guc(name: &CStr) -> StorageResult<Option<String>> {
    // SAFETY: `name` is a static NUL-terminated GUC name. PostgreSQL returns a
    // palloc'd string when the GUC exists and NULL when `missing_ok` is true and
    // the GUC is missing; we copy it into Rust-owned memory before pfree.
    let raw = unsafe {
        pg_sys::GetConfigOptionByName(name.as_ptr(), std::ptr::null_mut(), true)
    };
    if raw.is_null() {
        return Ok(None);
    }

    let flags = unsafe { pg_sys::GetConfigOptionFlags(name.as_ptr(), true) };
    if ((flags as u32) & pg_sys::GUC_CUSTOM_PLACEHOLDER) != 0 {
        // SAFETY: `raw` is the palloc'd string returned above.
        unsafe { pg_sys::pfree(raw.cast()) };
        return Err(StorageError::configuration(format!(
            "{} is only a PostgreSQL custom GUC placeholder; preload the lagodb_base runtime extension",
            name.to_string_lossy(),
        )));
    }

    // SAFETY: `GetConfigOptionByName` returned a valid NUL-terminated string.
    let value = unsafe { CStr::from_ptr(raw) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: `ShowGUCOption`, called by `GetConfigOptionByName`, returns a
    // palloc'd buffer owned by the current PostgreSQL memory context.
    unsafe { pg_sys::pfree(raw.cast()) };

    Ok(Some(value))
}

fn data_dir_base() -> StorageResult<PathBuf> {
    // SAFETY: PostgreSQL initializes `DataDir` before extensions and background
    // workers can resolve GUC-backed runtime paths.
    let data_dir = unsafe {
        if pg_sys::DataDir.is_null() {
            return Err(StorageError::configuration(
                "PostgreSQL DataDir is not initialized",
            ));
        }
        CStr::from_ptr(pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    };
    Ok(PathBuf::from(data_dir).join("lagodb"))
}
