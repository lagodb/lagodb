//! Query-only Iceberg cursor shared by TableAM, CustomScan, and FDW scans.

use pg_arrow_conv::{ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::batch::{AmScanBatchSource, BatchRowDecoder};
use pg_lakebase_core::prelude::{AmResult, SlotColumns};

use super::batch::IcebergArrowBatchSource;

pub(crate) struct IcebergQueryCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<BoundBatch>,
    row_index: usize,
}

impl IcebergQueryCursor {
    pub(crate) fn new(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
    ) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_index: 0,
        }
    }

    pub(crate) fn next_into_slot(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        self.next_with(|decoder, bound, row_index| {
            // SAFETY: ScanColumns compiled the decoder from the relation
            // layout used by this cursor and validated every destination
            // against the same slot width.
            unsafe { decoder.write_row_unchecked(bound, row_index, out) }?;
            Ok(())
        })
    }

    /// Emit one row through a lazily-created destination. `emit` is not called
    /// at end-of-scan, so an FDW does not touch its output slot for EOF.
    pub(crate) fn next_with<F>(&mut self, mut emit: F) -> AmResult<bool>
    where
        F: FnMut(&ArrowColumnDecoder, &BoundBatch, usize) -> AmResult<()>,
    {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_index < self.decoder.num_rows(bound)
            {
                let row_index = self.row_index;
                emit(&self.decoder, bound, row_index)?;
                self.row_index += 1;
                return Ok(true);
            }

            self.current = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(false);
            };
            self.current = Some(self.decoder.bind(batch)?);
            self.row_index = 0;
        }
    }
}
