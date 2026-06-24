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

//! Encrypted file wrappers for InputFile / OutputFile.

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;

use super::crypto::{AesGcmCipher, SecureKey};
use super::key_metadata::StandardKeyMetadata;
use super::stream::{AesGcmFileRead, AesGcmFileWrite};
use crate::Result;
use crate::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile};

pub struct EncryptedInputFile {
    inner: InputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedInputFile {
    pub fn new(inner: InputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    pub fn location(&self) -> &str {
        self.inner.location()
    }

    pub fn exists(&self) -> Result<bool> {
        self.inner.exists()
    }

    pub fn metadata(&self) -> Result<FileMetadata> {
        let raw_meta = self.inner.metadata()?;
        let plaintext_size =
            AesGcmFileRead::calculate_plaintext_length(raw_meta.size)?;
        Ok(FileMetadata {
            size: plaintext_size,
        })
    }

    pub fn read(&self) -> Result<Bytes> {
        self.reader()?.read_all()
    }

    pub fn reader(&self) -> Result<Box<dyn FileRead>> {
        let opened = self.inner.open_reader()?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> =
            self.key_metadata.aad_prefix().unwrap_or_default().into();
        let decrypting = AesGcmFileRead::new(
            opened.reader,
            cipher,
            aad_prefix,
            opened.metadata.size,
        )?;
        Ok(Box::new(decrypting))
    }

    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    pub fn into_inner(self) -> InputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedInputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedInputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

pub struct EncryptedOutputFile {
    inner: OutputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedOutputFile {
    pub fn new(inner: OutputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    pub fn location(&self) -> &str {
        self.inner.location()
    }

    pub fn writer(&self) -> Result<Box<dyn FileWrite>> {
        let raw_writer = self.inner.create_file_writer()?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> =
            self.key_metadata.aad_prefix().unwrap_or_default().into();
        Ok(Box::new(AesGcmFileWrite::new(
            raw_writer, cipher, aad_prefix,
        )))
    }

    pub fn write(&self, bs: Bytes) -> Result<()> {
        let mut writer = self.writer()?;
        writer.write_all(&bs)?;
        writer.close()
    }

    pub fn delete(&self) -> Result<()> {
        self.inner.delete()
    }

    pub fn into_inner(self) -> OutputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedOutputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedOutputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

fn build_cipher(metadata: &StandardKeyMetadata) -> Result<Arc<AesGcmCipher>> {
    let key = SecureKey::new(metadata.encryption_key().as_bytes())?;
    Ok(Arc::new(AesGcmCipher::new(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;

    fn key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::new(b"0123456789abcdef")
            .with_aad_prefix(b"test-aad-prefix!")
    }

    #[test]
    fn test_write_read_roundtrip() {
        let fileio = FileIO::memory();
        let path = "memory:///test/io_roundtrip.bin";
        let plaintext = b"Hello from EncryptedInputFile/EncryptedOutputFile!";

        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        output.write(Bytes::from(plaintext.to_vec())).unwrap();

        let input =
            EncryptedInputFile::new(fileio.new_input(path).unwrap(), key_metadata());
        let content = input.read().unwrap();
        assert_eq!(&content[..], plaintext);
    }
}
