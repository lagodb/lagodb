//! Arrow batch iterator adapter for the core access-method scan loop.

use arrow_array::RecordBatch;
use lagodb_core::batch::AmScanBatchSource;
use lagodb_core::diag::PgReportError;

/// Thin wrapper over a fallible Arrow batch iterator. It owns no buffer: the
/// cursor holds the one live batch, so the source just forwards `next()` and
/// lifts the iterator's error to the callback boundary.
///
/// Batch *production* (opening files, reading metadata, IO) is the producing
/// crate's domain, not a value conversion, so the iterator's error only has to
/// map into [`PgReportError`] and carries its own SQLSTATE classification
/// straight through. This layer never reclassifies a producer error as an
/// [`ArrowConversionError`](crate::ArrowConversionError).
pub struct ArrowBatchSource<I> {
    iter: I,
}

impl<I> ArrowBatchSource<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I, E> AmScanBatchSource for ArrowBatchSource<I>
where
    I: Iterator<Item = Result<RecordBatch, E>>,
    E: Into<PgReportError>,
{
    type Batch = RecordBatch;

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, PgReportError> {
        self.iter.next().transpose().map_err(Into::into)
    }
}
