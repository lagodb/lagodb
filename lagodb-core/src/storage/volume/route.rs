//! Immutable object-routing descriptors for storage volumes.

use thiserror::Error;

/// Immutable object-routing descriptor resolved from the Volume config.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageVolumeRoute {
    object_namespace: String,
    effective_base_uri: String,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StorageVolumeRouteError {
    #[error("invalid storage volume object namespace")]
    InvalidNamespace,
    #[error("storage volume effective base URI has no scheme")]
    MissingScheme,
    #[error("storage volume effective base URI uses an unsupported scheme")]
    UnsupportedScheme,
    #[error("storage volume effective base URI has no root key")]
    MissingRoot,
    #[error("storage volume effective base URI namespace does not match")]
    NamespaceMismatch,
    #[error("storage volume effective base URI root is not canonical")]
    InvalidRoot,
}

impl StorageVolumeRoute {
    pub fn new(
        object_namespace: impl Into<String>,
        effective_base_uri: impl Into<String>,
    ) -> Result<Self, StorageVolumeRouteError> {
        let route = Self {
            object_namespace: object_namespace.into(),
            effective_base_uri: effective_base_uri.into(),
        };
        route.validate()?;
        Ok(route)
    }

    pub fn object_namespace(&self) -> &str {
        &self.object_namespace
    }

    pub fn effective_base_uri(&self) -> &str {
        &self.effective_base_uri
    }

    pub fn effective_root_key(&self) -> &str {
        self.effective_base_uri
            .split_once("://")
            .expect("validated effective base URI contains a scheme")
            .1
            .split_once('/')
            .expect("validated effective base URI contains a root key")
            .1
    }

    fn validate(&self) -> Result<(), StorageVolumeRouteError> {
        if self.object_namespace.is_empty()
            || self.object_namespace.contains('/')
            || self.object_namespace.contains('\\')
            || self.object_namespace.as_bytes().contains(&0)
        {
            return Err(StorageVolumeRouteError::InvalidNamespace);
        }
        let (scheme, remainder) = self
            .effective_base_uri
            .split_once("://")
            .ok_or(StorageVolumeRouteError::MissingScheme)?;
        if !matches!(scheme, "s3" | "gs" | "az") {
            return Err(StorageVolumeRouteError::UnsupportedScheme);
        }
        let (namespace, root) = remainder
            .split_once('/')
            .ok_or(StorageVolumeRouteError::MissingRoot)?;
        if namespace != self.object_namespace {
            return Err(StorageVolumeRouteError::NamespaceMismatch);
        }
        if root.is_empty()
            || root.ends_with('/')
            || root.contains('\\')
            || root.as_bytes().contains(&0)
            || root.split('/').any(|segment| {
                segment.is_empty() || segment == "." || segment == ".."
            })
        {
            return Err(StorageVolumeRouteError::InvalidRoot);
        }
        Ok(())
    }
}
