// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Encryption manager for file-level encryption and two-layer envelope key management.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use chrono::Utc;
use moka::sync::Cache;
use uuid::Uuid;

use super::crypto::{AesGcmCipher, AesKeySize, SecureKey};
use super::io::EncryptedOutputFile;
use super::key_metadata::StandardKeyMetadata;
use super::kms::KeyManagementClient;
use crate::io::OutputFile;
use crate::sensitive::SensitiveBytes;
use crate::spec::{EncryptedKey, FormatVersion, TableMetadataRef};
use crate::{Error, ErrorKind, Result};

const MILLIS_IN_DAY: i64 = 24 * 60 * 60 * 1000;

pub const KEK_CREATED_AT_PROPERTY: &str = "KEY_TIMESTAMP";
const DEFAULT_KEK_LIFESPAN_DAYS: i64 = 730;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);
const AAD_PREFIX_LENGTH: usize = 16;

#[derive(typed_builder::TypedBuilder)]
#[builder(mutators(
    pub fn add_encryption_key(&mut self, key: EncryptedKey) {
        self.encryption_keys
            .write()
            .expect("encryption_keys lock poisoned")
            .insert(key.key_id().to_string(), key);
    }

    pub fn encryption_keys(&mut self, keys: HashMap<String, EncryptedKey>) {
        self.encryption_keys = RwLock::new(keys);
    }
))]
pub struct EncryptionManager {
    kms_client: Arc<dyn KeyManagementClient>,
    #[builder(
        default = Cache::builder().time_to_live(DEFAULT_CACHE_TTL).build(),
        setter(skip)
    )]
    kek_cache: Cache<String, SensitiveBytes>,
    #[builder(default = AesKeySize::default())]
    key_size: AesKeySize,
    #[builder(setter(into))]
    table_key_id: String,
    #[builder(default = RwLock::new(HashMap::new()), via_mutators)]
    encryption_keys: RwLock<HashMap<String, EncryptedKey>>,
}

impl fmt::Debug for EncryptionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionManager")
            .field("key_size", &self.key_size)
            .field("table_key_id", &self.table_key_id)
            .finish_non_exhaustive()
    }
}

impl EncryptionManager {
    pub(crate) fn from_table_metadata(
        kms_client: Option<&Arc<dyn KeyManagementClient>>,
        metadata: &TableMetadataRef,
    ) -> Result<Option<Arc<Self>>> {
        if metadata.format_version() < FormatVersion::V3 {
            return Ok(None);
        }

        let table_properties = metadata.table_properties()?;
        let Some(table_key_id) = table_properties.encryption_key_id else {
            if kms_client.is_some() {
                tracing::warn!(
                    "KeyManagementClient provided but table does not have encryption.key-id set"
                );
            }
            return Ok(None);
        };

        let kms_client = kms_client.ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "Table has encryption.key-id set but no KeyManagementClient was provided to TableBuilder",
            )
        })?;

        let em = EncryptionManager::builder()
            .kms_client(Arc::clone(kms_client))
            .table_key_id(table_key_id)
            .encryption_keys(metadata.encryption_keys.clone())
            .key_size(AesKeySize::from_key_length(
                table_properties.encryption_data_key_length,
            )?)
            .build();
        Ok(Some(Arc::new(em)))
    }

    /// Generates fresh key metadata for one encrypted file.
    pub fn generate_key_metadata(&self) -> StandardKeyMetadata {
        let dek = SecureKey::generate(self.key_size);
        let aad_prefix = Self::generate_aad_prefix();
        StandardKeyMetadata::from(dek).with_aad_prefix(&aad_prefix)
    }

    pub fn encrypt(&self, raw_output: OutputFile) -> EncryptedOutputFile {
        EncryptedOutputFile::new(raw_output, self.generate_key_metadata())
    }

    pub fn encrypt_manifest_list_key_metadata(
        &self,
        key_metadata: &StandardKeyMetadata,
    ) -> Result<String> {
        let kek = match self.find_active_kek()? {
            Some(existing) => existing,
            None => self.create_kek()?,
        };

        let kek_bytes = self.unwrap_key_encryption_key(&kek)?;
        let aad = Self::kek_timestamp_aad(&kek)?;
        let serialized = key_metadata.encode()?;
        let wrapped_metadata =
            self.wrap_dek_with_kek(&serialized, &kek_bytes, Some(aad))?;

        let wrapped_key = EncryptedKey::builder()
            .key_id(Uuid::new_v4().to_string())
            .encrypted_key_metadata(wrapped_metadata)
            .encrypted_by_id(kek.key_id())
            .build();

        let wrapped_key_id = wrapped_key.key_id().to_string();
        self.insert_encryption_key(wrapped_key);
        Ok(wrapped_key_id)
    }

    pub fn decrypt_manifest_list_key_metadata(
        &self,
        encryption_key_id: &str,
    ) -> Result<StandardKeyMetadata> {
        let encrypted_key = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned")
            .get(encryption_key_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Encryption key '{encryption_key_id}' not found"),
                )
            })?;

        let kek_key_id = encrypted_key.encrypted_by_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "EncryptedKey '{}' has no encrypted_by_id",
                    encrypted_key.key_id()
                ),
            )
        })?;

        let bytes =
            self.decrypt_dek(kek_key_id, encrypted_key.encrypted_key_metadata())?;

        StandardKeyMetadata::decode(bytes.as_bytes())
    }

    pub fn with_encryption_keys<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<String, EncryptedKey>) -> R,
    {
        let keys = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned");
        f(&keys)
    }

    fn insert_encryption_key(&self, key: EncryptedKey) {
        self.encryption_keys
            .write()
            .expect("encryption_keys lock poisoned")
            .insert(key.key_id().to_string(), key);
    }

    fn create_kek(&self) -> Result<EncryptedKey> {
        let (plaintext_kek, wrapped_kek) =
            if self.kms_client.supports_key_generation() {
                let result = self.kms_client.generate_key(&self.table_key_id)?;
                (result.key().clone(), result.wrapped_key().to_vec())
            } else {
                let plaintext_key = SecureKey::generate(self.key_size);
                let wrapped = self
                    .kms_client
                    .wrap_key(plaintext_key.as_bytes(), &self.table_key_id)?;

                (SensitiveBytes::new(plaintext_key.as_bytes()), wrapped)
            };

        let key_id = Uuid::new_v4().to_string();
        let now_ms = Utc::now().timestamp_millis();

        let mut properties = HashMap::new();
        properties.insert(KEK_CREATED_AT_PROPERTY.to_string(), now_ms.to_string());

        self.kek_cache.insert(key_id.clone(), plaintext_kek);

        let kek = EncryptedKey::builder()
            .key_id(key_id)
            .encrypted_key_metadata(wrapped_kek)
            .encrypted_by_id(&self.table_key_id)
            .properties(properties)
            .build();

        self.insert_encryption_key(kek.clone());
        Ok(kek)
    }

    fn is_kek_expired(&self, kek: &EncryptedKey) -> bool {
        let created_at_ms = match kek
            .properties()
            .get(KEK_CREATED_AT_PROPERTY)
            .and_then(|ts| ts.parse::<i64>().ok())
        {
            Some(ts) => ts,
            None => return true,
        };

        let now_ms = Utc::now().timestamp_millis();
        let lifespan_ms = DEFAULT_KEK_LIFESPAN_DAYS * MILLIS_IN_DAY;
        (now_ms - created_at_ms) >= lifespan_ms
    }

    fn find_active_kek(&self) -> Result<Option<EncryptedKey>> {
        let keys = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned");
        Ok(keys
            .values()
            .filter(|kek| {
                kek.encrypted_by_id()
                    .map(|id| id == self.table_key_id)
                    .unwrap_or(false)
                    && !self.is_kek_expired(kek)
            })
            .max_by_key(|kek| {
                kek.properties()
                    .get(KEK_CREATED_AT_PROPERTY)
                    .and_then(|ts| ts.parse::<i64>().ok())
                    .unwrap_or(0)
            })
            .cloned())
    }

    fn unwrap_key_encryption_key(
        &self,
        kek: &EncryptedKey,
    ) -> Result<SensitiveBytes> {
        let cache_key = kek.key_id().to_string();

        if let Some(cached) = self.kek_cache.get(&cache_key) {
            return Ok(cached);
        }

        let master_key_id = kek.encrypted_by_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("KEK '{}' has no encrypted_by_id", kek.key_id()),
            )
        })?;

        let plaintext = self
            .kms_client
            .unwrap_key(kek.encrypted_key_metadata(), master_key_id)?;

        self.kek_cache.insert(cache_key, plaintext.clone());

        Ok(plaintext)
    }

    fn decrypt_dek(
        &self,
        kek_key_id: &str,
        wrapped_dek: &[u8],
    ) -> Result<SensitiveBytes> {
        let kek = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned")
            .get(kek_key_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("KEK not found in encryption keys: {kek_key_id}"),
                )
            })?;

        let aad = Self::kek_timestamp_aad(&kek)?;
        let kek_bytes = self.unwrap_key_encryption_key(&kek)?;
        self.unwrap_dek_with_kek(wrapped_dek, &kek_bytes, Some(aad))
            .map_err(|e| {
                Error::new(
                    e.kind(),
                    format!("Failed to unwrap key metadata with KEK '{kek_key_id}'"),
                )
                .with_source(e)
            })
    }

    fn kek_timestamp_aad(kek: &EncryptedKey) -> Result<&[u8]> {
        kek.properties()
            .get(KEK_CREATED_AT_PROPERTY)
            .map(|ts| ts.as_bytes())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "KEK '{}' is missing required '{}' property",
                        kek.key_id(),
                        KEK_CREATED_AT_PROPERTY
                    ),
                )
            })
    }

    fn generate_aad_prefix() -> Box<[u8]> {
        let mut prefix = vec![0u8; AAD_PREFIX_LENGTH];
        OsRng.fill_bytes(&mut prefix);
        prefix.into_boxed_slice()
    }

    fn wrap_dek_with_kek(
        &self,
        dek: &[u8],
        kek: &SensitiveBytes,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let key = SecureKey::try_from(kek.clone())?;
        let cipher = AesGcmCipher::new(key);
        cipher.encrypt(dek, aad)
    }

    fn unwrap_dek_with_kek(
        &self,
        wrapped_dek: &[u8],
        kek: &SensitiveBytes,
        aad: Option<&[u8]>,
    ) -> Result<SensitiveBytes> {
        let key = SecureKey::try_from(kek.clone())?;
        let cipher = AesGcmCipher::new(key);
        cipher.decrypt(wrapped_dek, aad).map(SensitiveBytes::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::kms::MemoryKeyManagementClient;

    fn create_test_kms() -> Arc<dyn KeyManagementClient> {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        Arc::new(kms)
    }

    fn create_test_manager() -> EncryptionManager {
        EncryptionManager::builder()
            .kms_client(create_test_kms())
            .table_key_id("master-1")
            .build()
    }

    fn sample_key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::try_new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!")
    }

    #[test]
    fn test_wrap_unwrap_key_metadata_roundtrip() {
        let mgr = create_test_manager();
        let plaintext = sample_key_metadata();

        let key_id = mgr.encrypt_manifest_list_key_metadata(&plaintext).unwrap();
        assert_eq!(mgr.with_encryption_keys(|k| k.len()), 2);

        let decrypted = mgr.decrypt_manifest_list_key_metadata(&key_id).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_kek_reuse_when_not_expired() {
        let mgr = create_test_manager();
        let _id1 = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .unwrap();
        let kek_id = mgr.with_encryption_keys(|keys| {
            keys.values()
                .find(|k| k.encrypted_by_id() == Some("master-1"))
                .unwrap()
                .key_id()
                .to_string()
        });

        let id2 = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .unwrap();
        let entry2 =
            mgr.with_encryption_keys(|keys| keys.get(&id2).cloned().unwrap());
        assert_eq!(entry2.encrypted_by_id(), Some(kek_id.as_str()));
    }
}
