//! Row-location-bearing cursor for writable foreign-table target scans.

use arrow_array::{Int64Array, RecordBatch};
use lagodb_core::batch::{AmScanBatchSource, BatchRowDecoder};
use lagodb_core::fdw::{ForeignScanError, ScanSlotWriter};
use lagodb_core::handles::ValidItemPointer;
use pg_arrow_conv::{ArrowColumnDecoder, BoundBatch};

use crate::engine::scan::MutationScanInput;
use crate::engine::scan::batch::{
    IcebergArrowBatchSource, RowLocationLayout, position_unchecked,
};
use crate::engine::write::{IcebergFileId, IcebergRowIdentity, RelationRowRegistry};

struct MutationBoundBatch {
    decoded: BoundBatch,
    positions: Int64Array,
    file_id: IcebergFileId,
}

pub(super) struct ForeignMutationCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    registry: RelationRowRegistry,
    current: Option<MutationBoundBatch>,
    row_location_layout: Option<RowLocationLayout>,
    row_index: usize,
    last_file: Option<(Box<str>, IcebergFileId)>,
}

impl ForeignMutationCursor {
    pub(super) fn new(
        input: MutationScanInput,
        registry: RelationRowRegistry,
    ) -> Self {
        Self {
            source: input.source,
            decoder: input.decoder,
            registry,
            current: None,
            row_location_layout: None,
            row_index: 0,
            last_file: None,
        }
    }

    pub(super) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_index < self.decoder.num_rows(&bound.decoded)
            {
                let row_index = self.row_index;
                let mut columns = unsafe { output.datum_columns() };
                unsafe {
                    self.decoder.write_row_unchecked(
                        &bound.decoded,
                        row_index,
                        &mut columns,
                    )
                }?;
                let position =
                    unsafe { position_unchecked(&bound.positions, row_index) };
                let identity = IcebergRowIdentity::encode(bound.file_id, position)?;
                let valid = unsafe {
                    ValidItemPointer::new_unchecked(
                        identity.block_number,
                        identity.offset,
                    )
                };
                output.write_item_pointer(&valid);
                self.row_index += 1;
                return Ok(true);
            }

            self.current = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(false);
            };
            let Some(bound) = self.bind_batch(batch)? else {
                continue;
            };
            self.current = Some(bound);
            self.row_index = 0;
        }
    }

    fn bind_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<Option<MutationBoundBatch>, ForeignScanError> {
        let layout = match self.row_location_layout {
            Some(layout) => layout,
            None => {
                let layout = RowLocationLayout::try_new(&batch)?;
                self.row_location_layout = Some(layout);
                layout
            }
        };
        let locations = unsafe { layout.bind(&batch) }?;
        let Some(locations) = locations else {
            let _ = self.decoder.bind(batch)?;
            return Ok(None);
        };
        let file_id = self.register_file(locations.file_path())?;
        let positions = locations.into_positions();
        let decoded = self.decoder.bind(batch)?;
        Ok(Some(MutationBoundBatch {
            decoded,
            positions,
            file_id,
        }))
    }

    fn register_file(
        &mut self,
        path: &str,
    ) -> Result<IcebergFileId, ForeignScanError> {
        if let Some((cached, file_id)) = self.last_file.as_ref()
            && cached.as_ref() == path
        {
            return Ok(*file_id);
        }
        let file_id = self.registry.register_file(path)?;
        self.last_file = Some((path.into(), file_id));
        Ok(file_id)
    }
}
