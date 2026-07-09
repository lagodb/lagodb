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

//! AGS1 stream encryption/decryption for Iceberg.

use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::AesGcmCipher;
use crate::io::{FileRead, FileWrite};
use crate::{Error, ErrorKind, Result};

pub const PLAIN_BLOCK_SIZE: u32 = 1024 * 1024;
pub const NONCE_LENGTH: u32 = 12;
pub const GCM_TAG_LENGTH: u32 = 16;
pub const CIPHER_BLOCK_SIZE: u32 = PLAIN_BLOCK_SIZE + NONCE_LENGTH + GCM_TAG_LENGTH;
pub const GCM_STREAM_MAGIC: [u8; 4] = *b"AGS1";
pub const GCM_STREAM_HEADER_LENGTH: u32 = 8;

pub(crate) fn stream_block_aad(aad_prefix: &[u8], block_index: u32) -> Vec<u8> {
    let index_bytes = block_index.to_le_bytes();
    if aad_prefix.is_empty() {
        index_bytes.to_vec()
    } else {
        let mut aad = Vec::with_capacity(aad_prefix.len() + 4);
        aad.extend_from_slice(aad_prefix);
        aad.extend_from_slice(&index_bytes);
        aad
    }
}

pub struct AesGcmFileRead {
    inner: Box<dyn FileRead>,
    cipher: Arc<AesGcmCipher>,
    aad_prefix: Box<[u8]>,
    plain_stream_size: u64,
    num_blocks: u64,
    last_cipher_block_size: u32,
    position: u64,
}

impl AesGcmFileRead {
    pub fn new(
        inner: Box<dyn FileRead>,
        cipher: Arc<AesGcmCipher>,
        aad_prefix: Box<[u8]>,
        encrypted_file_length: u64,
    ) -> Result<Self> {
        let plain_stream_size =
            Self::calculate_plaintext_length(encrypted_file_length)?;
        let stream_length = encrypted_file_length - GCM_STREAM_HEADER_LENGTH as u64;

        if stream_length == 0 {
            return Ok(Self {
                inner,
                cipher,
                aad_prefix,
                plain_stream_size: 0,
                num_blocks: 0,
                last_cipher_block_size: 0,
                position: 0,
            });
        }

        let num_full_blocks = stream_length / CIPHER_BLOCK_SIZE as u64;
        let cipher_bytes_in_last_block =
            (stream_length % CIPHER_BLOCK_SIZE as u64) as u32;
        let full_blocks_only = cipher_bytes_in_last_block == 0;

        let num_blocks = if full_blocks_only {
            num_full_blocks
        } else {
            num_full_blocks + 1
        };

        if num_blocks > u32::MAX as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "AGS1 format supports at most {} blocks, but file requires {num_blocks} blocks",
                    u32::MAX
                ),
            ));
        }

        let last_cipher_block_size = if full_blocks_only {
            CIPHER_BLOCK_SIZE
        } else {
            cipher_bytes_in_last_block
        };

        Ok(Self {
            inner,
            cipher,
            aad_prefix,
            plain_stream_size,
            num_blocks,
            last_cipher_block_size,
            position: 0,
        })
    }

    pub fn plaintext_length(&self) -> u64 {
        self.plain_stream_size
    }

    pub fn calculate_plaintext_length(encrypted_file_length: u64) -> Result<u64> {
        if encrypted_file_length < GCM_STREAM_HEADER_LENGTH as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Encrypted file too short: {encrypted_file_length} bytes (minimum {GCM_STREAM_HEADER_LENGTH})"
                ),
            ));
        }

        let stream_length = encrypted_file_length - GCM_STREAM_HEADER_LENGTH as u64;

        if stream_length == 0 {
            return Ok(0);
        }

        let num_full_blocks = stream_length / CIPHER_BLOCK_SIZE as u64;
        let cipher_bytes_in_last_block = stream_length % CIPHER_BLOCK_SIZE as u64;
        let full_blocks_only = cipher_bytes_in_last_block == 0;

        let plain_bytes_in_last_block = if full_blocks_only {
            0
        } else {
            if cipher_bytes_in_last_block < (NONCE_LENGTH + GCM_TAG_LENGTH) as u64 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Truncated encrypted file: last block is {} bytes (minimum {})",
                        cipher_bytes_in_last_block,
                        NONCE_LENGTH + GCM_TAG_LENGTH
                    ),
                ));
            }
            cipher_bytes_in_last_block - NONCE_LENGTH as u64 - GCM_TAG_LENGTH as u64
        };

        Ok(num_full_blocks * PLAIN_BLOCK_SIZE as u64 + plain_bytes_in_last_block)
    }

    fn encrypted_block_offset(block_index: u64) -> u64 {
        block_index * CIPHER_BLOCK_SIZE as u64 + GCM_STREAM_HEADER_LENGTH as u64
    }

    fn cipher_block_size(&self, block_index: u64) -> u32 {
        if block_index == self.num_blocks - 1 {
            self.last_cipher_block_size
        } else {
            CIPHER_BLOCK_SIZE
        }
    }
}

impl FileRead for AesGcmFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start == range.end {
            return Ok(Bytes::new());
        }

        if range.start > range.end {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid read range: start ({}) is greater than end ({})",
                    range.start, range.end
                ),
            ));
        }

        if range.end > self.plain_stream_size {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Read range {}..{} exceeds plaintext size {}",
                    range.start, range.end, self.plain_stream_size
                ),
            ));
        }

        if self.num_blocks == 0 {
            return Ok(Bytes::new());
        }

        let first_block = range.start / PLAIN_BLOCK_SIZE as u64;
        let last_block = (range.end - 1) / PLAIN_BLOCK_SIZE as u64;
        let encrypted_start = Self::encrypted_block_offset(first_block);
        let encrypted_end = Self::encrypted_block_offset(last_block)
            + self.cipher_block_size(last_block) as u64;

        let all_encrypted = self.inner.read_range(encrypted_start..encrypted_end)?;

        let result_len = (range.end - range.start) as usize;
        let mut result = BytesMut::with_capacity(result_len);
        let mut encrypted_offset = 0usize;

        for block_idx in first_block..=last_block {
            let block_size = self.cipher_block_size(block_idx) as usize;
            let end = encrypted_offset + block_size;
            if end > all_encrypted.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Encrypted AGS1 block read returned fewer bytes than expected",
                ));
            }

            let cipher_block = &all_encrypted[encrypted_offset..end];
            encrypted_offset = end;

            let aad = stream_block_aad(&self.aad_prefix, block_idx as u32);
            let decrypted = self.cipher.decrypt(cipher_block, Some(&aad))?;

            let block_plain_start = block_idx * PLAIN_BLOCK_SIZE as u64;
            let slice_start = if block_idx == first_block {
                (range.start - block_plain_start) as usize
            } else {
                0
            };
            let slice_end = if block_idx == last_block {
                (range.end - block_plain_start) as usize
            } else {
                decrypted.len()
            };

            result.extend_from_slice(&decrypted[slice_start..slice_end]);
        }

        Ok(result.freeze())
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range(0..self.plain_stream_size)
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        Ok(Box::new(Self {
            inner: self.inner.try_clone()?,
            cipher: Arc::clone(&self.cipher),
            aad_prefix: self.aad_prefix.clone(),
            plain_stream_size: self.plain_stream_size,
            num_blocks: self.num_blocks,
            last_cipher_block_size: self.last_cipher_block_size,
            position: self.position,
        }))
    }
}

impl Read for AesGcmFileRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.plain_stream_size {
            return Ok(0);
        }

        let end =
            std::cmp::min(self.position + buf.len() as u64, self.plain_stream_size);
        let content = self.read_range(self.position..end).map_err(to_io_error)?;
        let len = content.len();
        buf[..len].copy_from_slice(&content);
        self.position += len as u64;
        Ok(len)
    }
}

impl Seek for AesGcmFileRead {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => self.plain_stream_size as i128 + offset as i128,
            SeekFrom::Current(offset) => self.position as i128 + offset as i128,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        self.position = new_pos as u64;
        Ok(self.position)
    }
}

pub struct AesGcmFileWrite {
    inner: Box<dyn FileWrite>,
    cipher: Arc<AesGcmCipher>,
    aad_prefix: Box<[u8]>,
    buffer: Vec<u8>,
    block_index: u32,
    header_written: bool,
    closed: bool,
    poisoned: bool,
}

impl AesGcmFileWrite {
    pub fn new(
        inner: Box<dyn FileWrite>,
        cipher: Arc<AesGcmCipher>,
        aad_prefix: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            inner,
            cipher,
            aad_prefix: aad_prefix.into(),
            buffer: Vec::new(),
            block_index: 0,
            header_written: false,
            closed: false,
            poisoned: false,
        }
    }

    fn write_header(&mut self) -> Result<()> {
        let mut header = Vec::with_capacity(GCM_STREAM_HEADER_LENGTH as usize);
        header.extend_from_slice(&GCM_STREAM_MAGIC);
        header.extend_from_slice(&PLAIN_BLOCK_SIZE.to_le_bytes());
        if let Err(e) = self.inner.write_all(&header) {
            self.poisoned = true;
            return Err(e.into());
        }
        self.header_written = true;
        Ok(())
    }

    fn encrypt_and_write_block(&mut self, block_data: &[u8]) -> Result<()> {
        let aad = stream_block_aad(&self.aad_prefix, self.block_index);
        let encrypted = self.cipher.encrypt(block_data, Some(&aad))?;
        if let Err(e) = self.inner.write_all(&encrypted) {
            self.poisoned = true;
            return Err(e.into());
        }
        self.block_index = self.block_index.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "AGS1 block index overflow: file exceeds the maximum supported size",
            )
        })?;
        Ok(())
    }

    fn encrypt_and_drain_block(&mut self) -> Result<()> {
        let aad = stream_block_aad(&self.aad_prefix, self.block_index);
        let encrypted = self
            .cipher
            .encrypt(&self.buffer[..PLAIN_BLOCK_SIZE as usize], Some(&aad))?;
        if let Err(e) = self.inner.write_all(&encrypted) {
            self.poisoned = true;
            return Err(e.into());
        }
        self.block_index = self.block_index.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "AGS1 block index overflow: file exceeds the maximum supported size",
            )
        })?;
        self.buffer.drain(..PLAIN_BLOCK_SIZE as usize);
        Ok(())
    }

    fn write_plaintext(&mut self, bs: &[u8]) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot write to a closed AesGcmFileWrite",
            ));
        }
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite is in a poisoned state due to a previous write failure",
            ));
        }

        if !self.header_written {
            self.write_header()?;
        }

        self.buffer.extend_from_slice(bs);
        while self.buffer.len() >= PLAIN_BLOCK_SIZE as usize {
            self.encrypt_and_drain_block()?;
        }

        Ok(())
    }
}

impl Write for AesGcmFileWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_plaintext(buf).map_err(to_io_error)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl FileWrite for AesGcmFileWrite {
    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite already closed",
            ));
        }
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite is in a poisoned state due to a previous write failure",
            ));
        }

        if !self.header_written {
            self.write_header()?;
        }

        if !self.buffer.is_empty() || self.block_index == 0 {
            let final_block = std::mem::take(&mut self.buffer);
            self.encrypt_and_write_block(&final_block)?;
        }
        self.closed = true;

        self.inner.close()
    }
}

fn to_io_error(error: Error) -> std::io::Error {
    std::io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{AesGcmCipher, SecureKey};

    fn make_cipher(key: &[u8]) -> Arc<AesGcmCipher> {
        Arc::new(AesGcmCipher::new(SecureKey::new(key).unwrap()))
    }

    #[test]
    fn calculate_plaintext_length_rejects_short_stream() {
        assert!(AesGcmFileRead::calculate_plaintext_length(7).is_err());
    }

    #[test]
    fn stream_block_aad_appends_little_endian_index() {
        assert_eq!(
            stream_block_aad(b"aad", 0x01020304),
            b"aad\x04\x03\x02\x01".to_vec()
        );
    }

    #[test]
    fn empty_file_roundtrip_with_memory_writer() {
        let cipher = make_cipher(b"0123456789abcdef");
        let encrypted = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let writer = Box::new(VecFileWrite(std::sync::Arc::clone(&encrypted)));
            let mut writer =
                AesGcmFileWrite::new(writer, Arc::clone(&cipher), b"aad".as_slice());
            writer.close().unwrap();
        }
        let encrypted = encrypted.lock().unwrap().clone();
        let reader = Box::new(BytesFileRead {
            data: Bytes::from(encrypted.clone()),
            position: 0,
        });
        let reader = AesGcmFileRead::new(
            reader,
            cipher,
            b"aad".as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();
        assert_eq!(reader.read_all().unwrap(), Bytes::new());
    }

    struct VecFileWrite(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for VecFileWrite {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl FileWrite for VecFileWrite {
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct BytesFileRead {
        data: Bytes,
        position: usize,
    }

    impl FileRead for BytesFileRead {
        fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }

        fn read_all(&self) -> Result<Bytes> {
            Ok(self.data.clone())
        }

        fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
            Ok(Box::new(Self {
                data: self.data.clone(),
                position: self.position,
            }))
        }
    }

    impl Read for BytesFileRead {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let remaining = self.data.len().saturating_sub(self.position);
            let len = remaining.min(buf.len());
            buf[..len]
                .copy_from_slice(&self.data[self.position..self.position + len]);
            self.position += len;
            Ok(len)
        }
    }

    impl Seek for BytesFileRead {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            let new_pos = match pos {
                SeekFrom::Start(offset) => offset as i64,
                SeekFrom::End(offset) => self.data.len() as i64 + offset,
                SeekFrom::Current(offset) => self.position as i64 + offset,
            };
            self.position = new_pos as usize;
            Ok(self.position as u64)
        }
    }
}
