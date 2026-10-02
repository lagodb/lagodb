//! PostgreSQL row-materialization cursor shared by scan adapters.

use arrow_array::RecordBatch;
use lagodb_arrow::{ArrowColumnDecoder, BoundBatch};
use lagodb_core::batch::{AmScanBatchSource, BatchRowDecoder, ScanBatchDriver};
use lagodb_core::prelude::{AmResult, ScanDirection, SlotColumns};

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
    pub(crate) fn new(source: S, decoder: ArrowColumnDecoder) -> Self {
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
            self.current = Some(self.decoder.bind(batch)?);
            self.row_index = 0;
        }
    }
}

impl<S> ScanBatchDriver for PgRowCursor<S>
where
    S: AmScanBatchSource<Batch = RecordBatch>,
{
    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        if direction != ScanDirection::Forward {
            return lagodb_core::api::unsupported_callback(
                "non-forward Iceberg scan",
            );
        }
        PgRowCursor::next_into_slot(self, out)
    }
}
