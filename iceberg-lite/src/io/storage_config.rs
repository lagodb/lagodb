//! Configuration passed to a [`StorageFactory`](super::StorageFactory).

use std::collections::HashMap;
use std::fmt;

use serde_derive::{Deserialize, Serialize};

/// Credential scoped to an object-location prefix.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageCredential {
    prefix: String,
    config: HashMap<String, String>,
}

impl StorageCredential {
    /// Creates a scoped credential.
    pub fn new(prefix: impl Into<String>, config: HashMap<String, String>) -> Self {
        Self {
            prefix: prefix.into(),
            config,
        }
    }

    /// Returns the URI prefix this credential applies to.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Returns the provider-specific credential properties.
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Consumes the credential into its prefix and provider properties.
    pub fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.prefix, self.config)
    }
}

impl fmt::Debug for StorageCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCredential")
            .field("prefix", &self.prefix)
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Owned configuration used to construct one table's storage implementation.
#[derive(Clone)]
pub struct StorageConfig {
    location: String,
    properties: HashMap<String, String>,
    credentials: Vec<StorageCredential>,
}

impl StorageConfig {
    /// Creates storage configuration from base properties and scoped credentials.
    pub fn new(
        location: impl Into<String>,
        properties: HashMap<String, String>,
        credentials: Vec<StorageCredential>,
    ) -> Self {
        Self {
            location: location.into(),
            properties,
            credentials,
        }
    }

    /// Returns the table metadata or warehouse location used to select a provider.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Returns base storage properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Returns scoped credentials in server-provided order.
    pub fn credentials(&self) -> &[StorageCredential] {
        &self.credentials
    }

    /// Consumes the configuration into its owned parts.
    pub fn into_parts(
        self,
    ) -> (String, HashMap<String, String>, Vec<StorageCredential>) {
        (self.location, self.properties, self.credentials)
    }
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageConfig")
            .field("property_keys", &self.properties.keys().collect::<Vec<_>>())
            .field("credentials", &self.credentials)
            .finish()
    }
}
