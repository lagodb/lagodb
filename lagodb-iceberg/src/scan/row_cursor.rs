//! PostgreSQL row-materialization cursor shared by scan adapters.

use arrow_array::RecordBatch;
use lagodb_arrow::{ArrowColumnDecoder, BoundBatch};
use lagodb_core::batch::{AmScanBatchSource, BatchRowDecoder};
use lagodb_core::prelude::{AmResult, SlotDatumWriter};

use super::batch::ScanBatchSource;

pub(crate) struct PgRowCursor<S = ScanBatchSource>
where
    S: AmScanBatchSource<Batch = RecordBatch>,
{
    source: S,
    decoder: ArrowColumnDecoder,
    current: Option<BoundBatch>,
    row_index: usize,
}

impl<S> PgRowCursor<S>
where
    S: AmScanBatchSource<Batch = RecordBatch>,
{
    /// # Safety
    /// Every batch from `source` must satisfy `decoder`'s complete projected
    /// schema, as required by `ArrowColumnDecoder::bind_batch_unchecked`.
    pub(crate) unsafe fn new(source: S, decoder: ArrowColumnDecoder) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_index: 0,
        }
    }

    /// Advance the shared forward-only source. Table AM adapters handle
    /// PostgreSQL direction requests before calling this cursor.
    pub(crate) fn next_into_slot(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<bool> {
        self.next_with(|decoder, bound, row_index| {
            // SAFETY: PgReadPlan compiled the decoder from the relation
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
            // SAFETY: the constructor pairs this source with its decoder.
            // Iceberg performs file-to-scan schema conversion before emission.
            self.current = Some(unsafe { self.decoder.bind_batch_unchecked(batch) }?);
            self.row_index = 0;
        }
    }
}
