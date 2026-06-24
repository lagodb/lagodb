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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use super::{GeneratedKey, KeyManagementClient};
use crate::encryption::{AesGcmCipher, AesKeySize, SecureKey, SensitiveBytes};
use crate::{Error, ErrorKind, Result};

#[derive(Clone)]
pub struct MemoryKeyManagementClient {
    master_keys: Arc<RwLock<HashMap<String, SensitiveBytes>>>,
    master_key_size: AesKeySize,
}

impl Default for MemoryKeyManagementClient {
    fn default() -> Self {
        Self {
            master_keys: Arc::new(RwLock::new(HashMap::new())),
            master_key_size: AesKeySize::default(),
        }
    }
}

impl fmt::Debug for MemoryKeyManagementClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryKeyManagementClient")
            .field("master_key_size", &self.master_key_size)
            .field("key_count", &self.key_count())
            .finish()
    }
}

impl MemoryKeyManagementClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_master_key_size(master_key_size: AesKeySize) -> Self {
        Self {
            master_keys: Arc::new(RwLock::new(HashMap::new())),
            master_key_size,
        }
    }

    pub fn add_master_key(&self, key_id: impl Into<String>) -> Result<()> {
        let key = SecureKey::generate(self.master_key_size);
        self.insert_key(key_id.into(), SensitiveBytes::new(key.as_bytes()))
    }

    pub fn add_master_key_bytes(
        &self,
        key_id: impl Into<String>,
        key_bytes: SensitiveBytes,
    ) -> Result<()> {
        SecureKey::new(key_bytes.as_bytes())?;
        self.insert_key(key_id.into(), key_bytes)
    }

    fn insert_key(&self, key_id: String, key: SensitiveBytes) -> Result<()> {
        let mut keys = self.master_keys.write().map_err(|_| {
            Error::new(ErrorKind::Unexpected, "master_keys lock poisoned")
        })?;

        if keys.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Master key already exists: {key_id}"),
            ));
        }

        keys.insert(key_id, key);
        Ok(())
    }

    fn get_master_key(&self, key_id: &str) -> Result<SensitiveBytes> {
        let keys = self.master_keys.read().map_err(|_| {
            Error::new(ErrorKind::Unexpected, "master_keys lock poisoned")
        })?;

        keys.get(key_id).cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Master key not found: {key_id}"),
            )
        })
    }

    pub fn key_count(&self) -> usize {
        self.master_keys.read().map(|keys| keys.len()).unwrap_or(0)
    }

    pub fn has_key(&self, key_id: &str) -> bool {
        self.master_keys
            .read()
            .map(|keys| keys.contains_key(key_id))
            .unwrap_or(false)
    }
}

impl KeyManagementClient for MemoryKeyManagementClient {
    fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>> {
        let master_key_bytes = self.get_master_key(wrapping_key_id)?;
        let master_key = SecureKey::new(master_key_bytes.as_bytes())?;
        let cipher = AesGcmCipher::new(master_key);

        cipher.encrypt(key, None)
    }

    fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> Result<SensitiveBytes> {
        let master_key_bytes = self.get_master_key(wrapping_key_id)?;
        let master_key = SecureKey::new(master_key_bytes.as_bytes())?;
        let cipher = AesGcmCipher::new(master_key);

        Ok(SensitiveBytes::new(cipher.decrypt(wrapped_key, None)?))
    }

    fn supports_key_generation(&self) -> bool {
        false
    }

    fn generate_key(&self, _wrapping_key_id: &str) -> Result<GeneratedKey> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "MemoryKeyManagementClient does not support server-side key generation",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_unwrap_roundtrip() {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        let dek = vec![0u8; 16];

        let wrapped = kms.wrap_key(&dek, "master-1").unwrap();
        let unwrapped = kms.unwrap_key(&wrapped, "master-1").unwrap();
        assert_eq!(unwrapped.as_bytes(), dek.as_slice());
    }
}
