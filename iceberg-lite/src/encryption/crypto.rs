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

//! Core cryptographic operations for Iceberg encryption.

use std::str::FromStr;

use crate::sensitive::SensitiveBytes;
use crate::{Error, ErrorKind, Result};
use aes_gcm::aead::generic_array::typenum::U12;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, Nonce};

type Aes192Gcm = AesGcm<aes_gcm::aes::Aes192, U12>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AesKeySize {
    #[default]
    Bits128 = 128,
    Bits192 = 192,
    Bits256 = 256,
}

impl AesKeySize {
    pub fn key_length(&self) -> usize {
        match self {
            Self::Bits128 => 16,
            Self::Bits192 => 24,
            Self::Bits256 => 32,
        }
    }

    pub fn from_key_length(len: usize) -> Result<Self> {
        match len {
            16 => Ok(Self::Bits128),
            24 => Ok(Self::Bits192),
            32 => Ok(Self::Bits256),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported data key length: {len} (must be 16, 24, or 32)"),
            )),
        }
    }
}

impl FromStr for AesKeySize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "128" | "AES_GCM_128" | "AES128_GCM" => Ok(Self::Bits128),
            "192" | "AES_GCM_192" | "AES192_GCM" => Ok(Self::Bits192),
            "256" | "AES_GCM_256" | "AES256_GCM" => Ok(Self::Bits256),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported AES key size: {s}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecureKey {
    key: SensitiveBytes,
    key_size: AesKeySize,
}

impl SecureKey {
    pub fn new(key: &[u8]) -> Result<Self> {
        let key_size = AesKeySize::from_key_length(key.len())?;
        Ok(Self {
            key: SensitiveBytes::new(key),
            key_size,
        })
    }

    pub fn generate(key_size: AesKeySize) -> Self {
        let mut key = vec![0u8; key_size.key_length()];
        OsRng.fill_bytes(&mut key);
        Self {
            key: SensitiveBytes::new(key),
            key_size,
        }
    }

    pub fn key_size(&self) -> AesKeySize {
        self.key_size
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

impl TryFrom<SensitiveBytes> for SecureKey {
    type Error = Error;

    fn try_from(key: SensitiveBytes) -> Result<Self> {
        let key_size = AesKeySize::from_key_length(key.len())?;
        Ok(Self { key, key_size })
    }
}

pub struct AesGcmCipher {
    key: SensitiveBytes,
    key_size: AesKeySize,
}

impl AesGcmCipher {
    pub const NONCE_LEN: usize = 12;
    pub const TAG_LEN: usize = 16;

    pub fn new(key: SecureKey) -> Self {
        Self {
            key: SensitiveBytes::new(key.as_bytes()),
            key_size: key.key_size(),
        }
    }

    pub fn encrypt(&self, plaintext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>> {
        match self.key_size {
            AesKeySize::Bits128 => {
                encrypt_aes_gcm::<Aes128Gcm>(self.key.as_bytes(), plaintext, aad)
            }
            AesKeySize::Bits192 => {
                encrypt_aes_gcm::<Aes192Gcm>(self.key.as_bytes(), plaintext, aad)
            }
            AesKeySize::Bits256 => {
                encrypt_aes_gcm::<Aes256Gcm>(self.key.as_bytes(), plaintext, aad)
            }
        }
    }

    pub fn decrypt(&self, ciphertext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>> {
        if ciphertext.len() < Self::NONCE_LEN + Self::TAG_LEN {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Ciphertext too short: expected at least {} bytes, got {}",
                    Self::NONCE_LEN + Self::TAG_LEN,
                    ciphertext.len()
                ),
            ));
        }

        match self.key_size {
            AesKeySize::Bits128 => {
                decrypt_aes_gcm::<Aes128Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
            AesKeySize::Bits192 => {
                decrypt_aes_gcm::<Aes192Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
            AesKeySize::Bits256 => {
                decrypt_aes_gcm::<Aes256Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
        }
    }
}

fn encrypt_aes_gcm<C>(
    key_bytes: &[u8],
    plaintext: &[u8],
    aad: Option<&[u8]>,
) -> Result<Vec<u8>>
where
    C: Aead + AeadCore + KeyInit,
{
    let cipher = C::new_from_slice(key_bytes).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Invalid AES key")
            .with_source(anyhow::anyhow!(e))
    })?;
    let nonce = C::generate_nonce(&mut OsRng);

    let ciphertext = if let Some(aad) = aad {
        cipher.encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
    } else {
        cipher.encrypt(&nonce, plaintext.as_ref())
    }
    .map_err(|e| {
        Error::new(ErrorKind::Unexpected, "AES-GCM encryption failed")
            .with_source(anyhow::anyhow!(e))
    })?;

    let mut result = Vec::with_capacity(nonce.len() + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

fn decrypt_aes_gcm<C>(
    key_bytes: &[u8],
    ciphertext: &[u8],
    aad: Option<&[u8]>,
) -> Result<Vec<u8>>
where
    C: Aead + AeadCore + KeyInit,
{
    let cipher = C::new_from_slice(key_bytes).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Invalid AES key")
            .with_source(anyhow::anyhow!(e))
    })?;

    let nonce = Nonce::from_slice(&ciphertext[..AesGcmCipher::NONCE_LEN]);
    let encrypted_data = &ciphertext[AesGcmCipher::NONCE_LEN..];

    let plaintext = if let Some(aad) = aad {
        cipher.decrypt(
            nonce,
            Payload {
                msg: encrypted_data,
                aad,
            },
        )
    } else {
        cipher.decrypt(nonce, encrypted_data)
    }
    .map_err(|e| {
        Error::new(ErrorKind::Unexpected, "AES-GCM decryption failed")
            .with_source(anyhow::anyhow!(e))
    })?;

    Ok(plaintext)
}
