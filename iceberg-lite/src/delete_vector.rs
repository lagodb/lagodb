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

use std::io::Cursor;
use std::ops::BitOrAssign;

use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::{Error, ErrorKind, Result};

#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    const PUFFIN_V1_MAGIC: u32 = 1681511377;
    const PUFFIN_V1_LENGTH_SIZE: usize = 4;
    const PUFFIN_V1_MAGIC_SIZE: usize = 4;
    const PUFFIN_V1_CRC_SIZE: usize = 4;
    const PUFFIN_V1_MIN_SIZE: usize = Self::PUFFIN_V1_LENGTH_SIZE
        + Self::PUFFIN_V1_MAGIC_SIZE
        + Self::PUFFIN_V1_CRC_SIZE;

    #[allow(unused)]
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    #[allow(dead_code)]
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    pub fn cardinality(&self) -> u64 {
        self.len()
    }

    pub fn merge_ref(&mut self, other: &DeleteVector) {
        self.inner.bitor_assign(&other.inner);
    }

    /// Serialize this delete vector as Iceberg Puffin `deletion-vector-v1` bytes.
    ///
    /// The inner bitmap bytes are the portable 64-bit RoaringTreemap format.
    /// Iceberg wraps that payload with a big-endian length, little-endian
    /// magic number, and big-endian CRC32 over magic + payload.
    pub fn to_puffin_v1_bytes(&self) -> Result<Vec<u8>> {
        let roaring_size = self.inner.serialized_size();
        let content_length = Self::PUFFIN_V1_MAGIC_SIZE
            .checked_add(roaring_size)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector serialized length overflow",
                )
            })?;
        let content_length_u32 = u32::try_from(content_length).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "deletion vector serialized length does not fit u32",
            )
        })?;

        let mut bytes = Vec::with_capacity(
            Self::PUFFIN_V1_LENGTH_SIZE + content_length + Self::PUFFIN_V1_CRC_SIZE,
        );
        bytes.extend_from_slice(&content_length_u32.to_be_bytes());
        bytes.extend_from_slice(&Self::PUFFIN_V1_MAGIC.to_le_bytes());
        self.inner.serialize_into(&mut bytes).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to serialize deletion vector roaring bitmap",
            )
            .with_source(err)
        })?;

        let crc = crc32fast::hash(&bytes[Self::PUFFIN_V1_LENGTH_SIZE..]);
        bytes.extend_from_slice(&crc.to_be_bytes());
        Ok(bytes)
    }

    /// Deserialize Iceberg Puffin `deletion-vector-v1` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the byte envelope is malformed, the CRC does not
    /// match, the Roaring bitmap is invalid, or the decoded cardinality does
    /// not match `expected_cardinality`.
    pub fn from_puffin_v1_bytes(
        bytes: &[u8],
        expected_cardinality: u64,
    ) -> Result<Self> {
        if bytes.len() < Self::PUFFIN_V1_MIN_SIZE {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector blob is too small",
            ));
        }

        let content_length = Self::read_u32_be(
            &bytes[..Self::PUFFIN_V1_LENGTH_SIZE],
            "deletion vector content length",
        )? as usize;
        let expected_blob_len = Self::PUFFIN_V1_LENGTH_SIZE
            .checked_add(content_length)
            .and_then(|len| len.checked_add(Self::PUFFIN_V1_CRC_SIZE))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector blob length overflow",
                )
            })?;
        if expected_blob_len != bytes.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector blob length mismatch: header says {expected_blob_len}, actual {}",
                    bytes.len()
                ),
            ));
        }

        let content_start = Self::PUFFIN_V1_LENGTH_SIZE;
        let content_end = content_start + content_length;
        let content = &bytes[content_start..content_end];
        if content.len() < Self::PUFFIN_V1_MAGIC_SIZE {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector content is too small",
            ));
        }

        let actual_crc =
            Self::read_u32_be(&bytes[content_end..], "deletion vector crc")?;
        let expected_crc = crc32fast::hash(content);
        if actual_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector crc mismatch",
            ));
        }

        let magic = Self::read_u32_le(
            &content[..Self::PUFFIN_V1_MAGIC_SIZE],
            "deletion vector magic",
        )?;
        if magic != Self::PUFFIN_V1_MAGIC {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector magic mismatch: expected {}, actual {magic}",
                    Self::PUFFIN_V1_MAGIC
                ),
            ));
        }

        let roaring_bytes = &content[Self::PUFFIN_V1_MAGIC_SIZE..];
        let mut cursor = Cursor::new(roaring_bytes);
        let inner = RoaringTreemap::deserialize_from(&mut cursor).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to deserialize deletion vector roaring bitmap",
            )
            .with_source(err)
        })?;
        if cursor.position() != roaring_bytes.len() as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector roaring bitmap has trailing bytes",
            ));
        }

        let delete_vector = DeleteVector { inner };
        if delete_vector.cardinality() != expected_cardinality {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector cardinality mismatch: expected {expected_cardinality}, actual {}",
                    delete_vector.cardinality()
                ),
            ));
        }

        Ok(delete_vector)
    }

    fn read_u32_be(bytes: &[u8], name: &str) -> Result<u32> {
        let raw = <[u8; 4]>::try_from(bytes).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("{name} must contain exactly 4 bytes"),
            )
        })?;
        Ok(u32::from_be_bytes(raw))
    }

    fn read_u32_le(bytes: &[u8], name: &str) -> Result<u32> {
        let raw = <[u8; 4]>::try_from(bytes).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("{name} must contain exactly 4 bytes"),
            )
        })?;
        Ok(u32::from_le_bytes(raw))
    }
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner {
            if let Some(inner_next) = inner.bitmap_iter.next() {
                return Some(
                    u64::from(inner.high_bits) << 32 | u64::from(inner_next),
                );
            }
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

impl BitOrAssign<&DeleteVector> for DeleteVector {
    fn bitor_assign(&mut self, other: &DeleteVector) {
        self.merge_ref(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    #[test]
    fn test_puffin_v1_round_trip() {
        let mut dv = DeleteVector::default();
        for pos in [0, 1, 42, u64::from(u32::MAX) + 3, 1 << 40] {
            dv.insert(pos);
        }

        let bytes = dv.to_puffin_v1_bytes().unwrap();
        let decoded =
            DeleteVector::from_puffin_v1_bytes(&bytes, dv.cardinality()).unwrap();

        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            dv.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_puffin_v1_rejects_bad_crc() {
        let mut dv = DeleteVector::default();
        dv.insert(42);
        let mut bytes = dv.to_puffin_v1_bytes().unwrap();
        let payload_index = DeleteVector::PUFFIN_V1_LENGTH_SIZE
            + DeleteVector::PUFFIN_V1_MAGIC_SIZE
            + 1;
        bytes[payload_index] ^= 0x01;

        let err =
            DeleteVector::from_puffin_v1_bytes(&bytes, dv.cardinality()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_puffin_v1_rejects_cardinality_mismatch() {
        let mut dv = DeleteVector::default();
        dv.insert(42);
        let bytes = dv.to_puffin_v1_bytes().unwrap();

        let err = DeleteVector::from_puffin_v1_bytes(&bytes, 2).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
