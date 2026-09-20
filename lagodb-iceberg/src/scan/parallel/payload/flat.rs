use std::mem::size_of;
use std::ptr::NonNull;

use arrow_schema::SchemaRef;
use bincode::Options;
use iceberg_lite::scan::FileScanTask;
use serde::de::DeserializeOwned;

use super::{
    TaskRange, WorkerFileRecord, WorkerTaskMetadata, WorkerTaskMetadataCodec, codec,
};
use crate::scan::ScanError;

mod encode;

pub(super) use encode::FlatPayloadEncoder;

const MAGIC: &[u8; 8] = b"LAGOINV\0";
const HEADER_BYTES: usize = 72;
const DESCRIPTOR_BYTES: usize = 16;
const RANGE_BYTES: usize = 32;
const RECORD_COUNT_PRESENT: u32 = 1;
const SPLIT: u32 = 2;

struct InventoryStorage {
    data: NonNull<u8>,
    len: usize,
}

// SAFETY: this view is created only for a DSM range whose owner remains
// attached until the worker source is released, as required by the decode
// contract. The bytes are immutable after worker activation.
unsafe impl Send for InventoryStorage {}
// SAFETY: no method mutates the backing bytes, and all decoded values are owned.
unsafe impl Sync for InventoryStorage {}

impl InventoryStorage {
    fn as_slice(&self) -> &[u8] {
        // SAFETY: construction validates this immutable DSM range and the
        // worker-source lifecycle keeps it mapped for `self`.
        unsafe { core::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

#[derive(Clone, Copy)]
struct PayloadLayout {
    metadata_count: usize,
    files_count: usize,
    range_count: usize,
    group_count: usize,
    metadata_directory: usize,
    files_directory: usize,
    ranges: usize,
    groups_directory: usize,
}

impl PayloadLayout {
    fn section_end(
        start: usize,
        count: usize,
        width: usize,
    ) -> Result<usize, ScanError> {
        count
            .checked_mul(width)
            .and_then(|bytes| start.checked_add(bytes))
            .ok_or_else(|| {
                ScanError::WorkerPayload(
                    "Iceberg worker inventory layout overflowed usize".to_owned(),
                )
            })
    }
}

pub(in super::super) struct DecodedTaskInventory {
    pub(in super::super) schema: SchemaRef,
    metadata: Box<[WorkerTaskMetadata]>,
    storage: InventoryStorage,
    layout: PayloadLayout,
    consumed_groups: Box<[bool]>,
}

impl DecodedTaskInventory {
    /// # Safety
    ///
    /// `data` must remain mapped and immutable until this inventory is dropped.
    pub(super) unsafe fn decode_shared(data: &[u8]) -> Result<Self, ScanError> {
        let len = data.len();
        let data = NonNull::new(data.as_ptr().cast_mut()).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker inventory has a null base".to_owned(),
            )
        })?;
        Self::decode_storage(InventoryStorage { data, len })
    }

    fn decode_storage(storage: InventoryStorage) -> Result<Self, ScanError> {
        let view = PayloadView::new(storage.as_slice())?;
        let layout = view.layout()?;
        view.validate_blob_layout(layout)?;
        let schema: SchemaRef = view.decode_binary(view.schema_descriptor()?)?;
        let metadata = (0..layout.metadata_count)
            .map(|index| {
                view.decode_metadata(
                    view.descriptor(layout.metadata_directory, index)?,
                )
            })
            .collect::<Result<Vec<WorkerTaskMetadata>, _>>()?
            .into_boxed_slice();
        Ok(Self {
            schema,
            metadata,
            storage,
            layout,
            consumed_groups: vec![false; layout.group_count].into_boxed_slice(),
        })
    }

    pub(in super::super) fn take_tasks(
        &mut self,
        work_id: u32,
    ) -> Result<Vec<FileScanTask>, ScanError> {
        let group_count = self.layout.group_count;
        let consumed =
            self.consumed_groups
                .get_mut(work_id as usize)
                .ok_or_else(|| {
                    ScanError::WorkerPayload(format!(
                        "work id {work_id} is outside {group_count} groups"
                    ))
                })?;
        if std::mem::replace(consumed, true) {
            return Err(ScanError::WorkerPayload(format!(
                "work id {work_id} was already consumed"
            )));
        }
        let view = PayloadView::new(self.storage.as_slice())?;
        let group =
            view.descriptor(self.layout.groups_directory, work_id as usize)?;
        let range_ids = view.range_ids(group)?;
        let mut tasks = Vec::with_capacity(range_ids.len());
        for range_id in range_ids {
            let range = view.range(self.layout, range_id as usize)?;
            let file: WorkerFileRecord = view.decode_binary(
                view.descriptor(self.layout.files_directory, range.file_id as usize)?,
            )?;
            let metadata =
                self.metadata
                    .get(file.metadata_id as usize)
                    .ok_or_else(|| {
                        ScanError::WorkerPayload(format!(
                            "file {} references missing metadata {}",
                            range.file_id, file.metadata_id,
                        ))
                    })?;
            tasks.push(file.into_task(range, metadata));
        }
        Ok(tasks)
    }
}

struct PayloadView<'a> {
    bytes: &'a [u8],
}

impl<'a> PayloadView<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, ScanError> {
        if bytes.len() < HEADER_BYTES || bytes.get(..MAGIC.len()) != Some(MAGIC) {
            return Err(ScanError::WorkerPayload(
                "Iceberg worker inventory has an invalid header".to_owned(),
            ));
        }
        Ok(Self { bytes })
    }

    fn layout(&self) -> Result<PayloadLayout, ScanError> {
        let layout = PayloadLayout {
            metadata_count: self.count(24)?,
            files_count: self.count(28)?,
            range_count: self.count(32)?,
            group_count: self.count(36)?,
            metadata_directory: self.offset(40)?,
            files_directory: self.offset(48)?,
            ranges: self.offset(56)?,
            groups_directory: self.offset(64)?,
        };
        if layout.group_count == 0 {
            return Err(ScanError::WorkerPayload(
                "Iceberg worker inventory has no work groups".to_owned(),
            ));
        }
        let files = PayloadLayout::section_end(
            HEADER_BYTES,
            layout.metadata_count,
            DESCRIPTOR_BYTES,
        )?;
        let ranges =
            PayloadLayout::section_end(files, layout.files_count, DESCRIPTOR_BYTES)?;
        let groups =
            PayloadLayout::section_end(ranges, layout.range_count, RANGE_BYTES)?;
        let blobs =
            PayloadLayout::section_end(groups, layout.group_count, DESCRIPTOR_BYTES)?;
        if layout.metadata_directory != HEADER_BYTES
            || layout.files_directory != files
            || layout.ranges != ranges
            || layout.groups_directory != groups
            || blobs > self.bytes.len()
        {
            return Err(ScanError::WorkerPayload(
                "Iceberg worker inventory has an invalid section layout".to_owned(),
            ));
        }
        Ok(layout)
    }

    fn validate_blob_layout(&self, layout: PayloadLayout) -> Result<(), ScanError> {
        let mut cursor =
            layout.groups_directory + layout.group_count * DESCRIPTOR_BYTES;
        for descriptor in std::iter::once(self.schema_descriptor())
            .chain(
                (0..layout.metadata_count)
                    .map(|index| self.descriptor(layout.metadata_directory, index)),
            )
            .chain(
                (0..layout.files_count)
                    .map(|index| self.descriptor(layout.files_directory, index)),
            )
            .chain(
                (0..layout.group_count)
                    .map(|index| self.descriptor(layout.groups_directory, index)),
            )
        {
            let (offset, len) = descriptor?;
            if offset != cursor {
                return Err(ScanError::WorkerPayload(
                    "Iceberg worker inventory blobs are not contiguous".to_owned(),
                ));
            }
            cursor = cursor.checked_add(len).ok_or_else(|| {
                ScanError::WorkerPayload(
                    "Iceberg worker inventory blob extent overflowed usize"
                        .to_owned(),
                )
            })?;
        }
        if cursor != self.bytes.len() {
            return Err(ScanError::WorkerPayload(
                "Iceberg worker inventory has trailing or missing bytes".to_owned(),
            ));
        }
        for index in 0..layout.group_count {
            let (_, len) = self.descriptor(layout.groups_directory, index)?;
            if len % size_of::<u32>() != 0 {
                return Err(ScanError::WorkerPayload(format!(
                    "work group {index} has a partial range id"
                )));
            }
        }
        Ok(())
    }

    fn schema_descriptor(&self) -> Result<(usize, usize), ScanError> {
        Ok((self.offset(8)?, self.offset(16)?))
    }

    fn descriptor(
        &self,
        directory: usize,
        index: usize,
    ) -> Result<(usize, usize), ScanError> {
        let offset = index
            .checked_mul(DESCRIPTOR_BYTES)
            .and_then(|offset| directory.checked_add(offset))
            .ok_or_else(|| {
                ScanError::WorkerPayload(
                    "Iceberg worker descriptor offset overflowed usize".to_owned(),
                )
            })?;
        Ok((self.offset(offset)?, self.offset(offset + 8)?))
    }

    fn range(
        &self,
        layout: PayloadLayout,
        index: usize,
    ) -> Result<TaskRange, ScanError> {
        if index >= layout.range_count {
            return Err(ScanError::WorkerPayload(format!(
                "work group references missing range {index}"
            )));
        }
        let offset = layout.ranges + index * RANGE_BYTES;
        let flags = self.u32(offset + 4)?;
        if flags & !(RECORD_COUNT_PRESENT | SPLIT) != 0 {
            return Err(ScanError::WorkerPayload(format!(
                "range {index} has unknown flags"
            )));
        }
        let file_id = self.u32(offset)?;
        if file_id as usize >= layout.files_count {
            return Err(ScanError::WorkerPayload(format!(
                "range {index} references missing file {file_id}"
            )));
        }
        Ok(TaskRange {
            file_id,
            start: self.u64(offset + 8)?,
            length: self.u64(offset + 16)?,
            record_count: (flags & RECORD_COUNT_PRESENT != 0)
                .then(|| self.u64(offset + 24))
                .transpose()?,
            split: flags & SPLIT != 0,
        })
    }

    fn range_ids(
        &self,
        descriptor: (usize, usize),
    ) -> Result<impl ExactSizeIterator<Item = u32> + 'a, ScanError> {
        let bytes = self.slice(descriptor)?;
        if bytes.len() % size_of::<u32>() != 0 {
            return Err(ScanError::WorkerPayload(
                "Iceberg work group has a partial range id".to_owned(),
            ));
        }
        Ok(bytes.chunks_exact(size_of::<u32>()).map(|value| {
            u32::from_le_bytes(value.try_into().expect("four-byte chunk"))
        }))
    }

    fn decode_binary<T: DeserializeOwned>(
        &self,
        descriptor: (usize, usize),
    ) -> Result<T, ScanError> {
        codec()
            .deserialize(self.slice(descriptor)?)
            .map_err(super::codec_error)
    }

    fn decode_metadata(
        &self,
        descriptor: (usize, usize),
    ) -> Result<WorkerTaskMetadata, ScanError> {
        WorkerTaskMetadataCodec::decode(self.slice(descriptor)?)
    }

    fn slice(&self, (offset, len): (usize, usize)) -> Result<&'a [u8], ScanError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker blob extent overflowed usize".to_owned(),
            )
        })?;
        self.bytes.get(offset..end).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker blob lies outside its payload".to_owned(),
            )
        })
    }

    fn count(&self, offset: usize) -> Result<usize, ScanError> {
        Ok(self.u32(offset)? as usize)
    }

    fn offset(&self, offset: usize) -> Result<usize, ScanError> {
        usize::try_from(self.u64(offset)?).map_err(|_| {
            ScanError::WorkerPayload(
                "Iceberg worker offset exceeds this platform".to_owned(),
            )
        })
    }

    fn u32(&self, offset: usize) -> Result<u32, ScanError> {
        let bytes = self.bytes.get(offset..offset + 4).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker inventory is truncated".to_owned(),
            )
        })?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("four-byte slice"),
        ))
    }

    fn u64(&self, offset: usize) -> Result<u64, ScanError> {
        let bytes = self.bytes.get(offset..offset + 8).ok_or_else(|| {
            ScanError::WorkerPayload(
                "Iceberg worker inventory is truncated".to_owned(),
            )
        })?;
        Ok(u64::from_le_bytes(
            bytes.try_into().expect("eight-byte slice"),
        ))
    }
}
