use std::mem::size_of;

use arrow_schema::SchemaRef;
use bincode::Options;
use serde::Serialize;

use super::super::{
    TaskRange, WorkerFileRecord, WorkerTaskMetadata, WorkerTaskMetadataCodec, codec,
    codec_error,
};
use super::{
    DESCRIPTOR_BYTES, HEADER_BYTES, MAGIC, PayloadLayout, RANGE_BYTES, SPLIT,
};
use crate::scan::ScanError;

pub(in super::super) struct FlatPayloadEncoder {
    bytes: Vec<u8>,
    base: usize,
    ranges: usize,
}

impl FlatPayloadEncoder {
    pub(in super::super) fn encode(
        prefix: &[u8],
        schema: &SchemaRef,
        metadata: &[WorkerTaskMetadata],
        files: &[WorkerFileRecord],
        ranges: &[TaskRange],
        groups: &[Box<[u32]>],
    ) -> Result<Box<[u8]>, ScanError> {
        let metadata_directory = HEADER_BYTES;
        let files_directory = PayloadLayout::section_end(
            metadata_directory,
            metadata.len(),
            DESCRIPTOR_BYTES,
        )?;
        let ranges_offset = PayloadLayout::section_end(
            files_directory,
            files.len(),
            DESCRIPTOR_BYTES,
        )?;
        let groups_directory =
            PayloadLayout::section_end(ranges_offset, ranges.len(), RANGE_BYTES)?;
        let blob_offset = PayloadLayout::section_end(
            groups_directory,
            groups.len(),
            DESCRIPTOR_BYTES,
        )?;
        let base = prefix.len();
        let total = base.checked_add(blob_offset).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker payload length overflowed usize".to_owned(),
            )
        })?;
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(prefix);
        bytes.resize(total, 0);
        let mut encoder = Self {
            bytes,
            base,
            ranges: ranges_offset,
        };
        encoder.bytes[base..base + MAGIC.len()].copy_from_slice(MAGIC);
        encoder.put_count(24, metadata.len(), "metadata")?;
        encoder.put_count(28, files.len(), "file")?;
        encoder.put_count(32, ranges.len(), "range")?;
        encoder.put_count(36, groups.len(), "group")?;
        encoder.put_u64(40, metadata_directory as u64);
        encoder.put_u64(48, files_directory as u64);
        encoder.put_u64(56, ranges_offset as u64);
        encoder.put_u64(64, groups_directory as u64);

        let descriptor = encoder.append_binary(schema)?;
        encoder.put_descriptor(8, descriptor);
        for (index, metadata) in metadata.iter().enumerate() {
            let descriptor = encoder.append_metadata(metadata)?;
            encoder.put_descriptor(
                metadata_directory + index * DESCRIPTOR_BYTES,
                descriptor,
            );
        }
        for (index, file) in files.iter().enumerate() {
            let descriptor = encoder.append_binary(file)?;
            encoder.put_descriptor(
                files_directory + index * DESCRIPTOR_BYTES,
                descriptor,
            );
        }
        for (index, range) in ranges.iter().enumerate() {
            encoder.put_range(index, range);
        }
        for (index, group) in groups.iter().enumerate() {
            let start = encoder.bytes.len() - encoder.base;
            let bytes =
                group.len().checked_mul(size_of::<u32>()).ok_or_else(|| {
                    ScanError::WorkerPayload(
                        "Iceberg group byte length overflowed usize".to_owned(),
                    )
                })?;
            encoder.bytes.reserve(bytes);
            for range_id in group.iter().copied() {
                encoder.bytes.extend_from_slice(&range_id.to_le_bytes());
            }
            encoder.put_descriptor(
                groups_directory + index * DESCRIPTOR_BYTES,
                (start, bytes),
            );
        }
        Ok(encoder.bytes.into_boxed_slice())
    }

    fn put_count(
        &mut self,
        offset: usize,
        value: usize,
        kind: &'static str,
    ) -> Result<(), ScanError> {
        let value = u32::try_from(value).map_err(|_| {
            ScanError::WorkerPayload(format!("Iceberg {kind} inventory exceeds u32"))
        })?;
        self.put_u32(offset, value);
        Ok(())
    }

    fn append_binary<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(usize, usize), ScanError> {
        let offset = self.bytes.len() - self.base;
        codec()
            .serialize_into(&mut self.bytes, value)
            .map_err(codec_error)?;
        Ok((offset, self.bytes.len() - self.base - offset))
    }

    fn append_metadata(
        &mut self,
        metadata: &WorkerTaskMetadata,
    ) -> Result<(usize, usize), ScanError> {
        let offset = self.bytes.len() - self.base;
        WorkerTaskMetadataCodec::encode_into(&mut self.bytes, metadata)?;
        Ok((offset, self.bytes.len() - self.base - offset))
    }

    fn put_range(&mut self, index: usize, range: &TaskRange) {
        let offset = self.ranges + index * RANGE_BYTES;
        let mut flags = u32::from(range.record_count.is_some());
        if range.split {
            flags |= SPLIT;
        }
        self.put_u32(offset, range.file_id);
        self.put_u32(offset + 4, flags);
        self.put_u64(offset + 8, range.start);
        self.put_u64(offset + 16, range.length);
        self.put_u64(offset + 24, range.record_count.unwrap_or_default());
    }

    fn put_descriptor(&mut self, offset: usize, descriptor: (usize, usize)) {
        self.put_u64(offset, descriptor.0 as u64);
        self.put_u64(offset + 8, descriptor.1 as u64);
    }

    fn put_u32(&mut self, offset: usize, value: u32) {
        let offset = self.base + offset;
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64(&mut self, offset: usize, value: u64) {
        let offset = self.base + offset;
        self.bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
}
