//! Spill runs grouped by bucket and read back one bucket range at a time.
//!
//! A run is one temporary file: Arrow IPC segments of each bucket in bucket
//! order, then a footer of `buckets + 1` little-endian start offsets.
//! Every segment carries its own dictionaries, so a bucket range decodes with
//! only the schema message shared by the runs of one input. The footer stays on
//! disk and a reader fetches the two offsets around its range, in one read
//! when they are adjacent, so a run holds no memory however many buckets it
//! has.
//!
//! A range is read whole, its batches sharing one buffer of exactly its
//! bytes, or chunk by chunk, as DataFusion reads its own spill files.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_schema::{DataType, SchemaRef};
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    write_message,
};
use datafusion::common::Result;

use super::{BackendTempFile, SPILL_CHUNK_BYTES};

const OFFSET_BYTES: u64 = size_of::<u64>() as u64;

/// IPC encoding shared by the runs of one input.
pub(in crate::datafusion) struct BucketRunFormat {
    schema: SchemaRef,
    schema_message: Buffer,
    options: IpcWriteOptions,
    generator: IpcDataGenerator,
    /// Dictionary ids follow schema order from zero, so a segment's own
    /// tracker reproduces them by drawing this many.
    dictionaries: usize,
}

impl BucketRunFormat {
    pub(in crate::datafusion) fn try_new(schema: SchemaRef) -> Result<Self> {
        // Decimal128 and view arrays need 16-byte buffers; a segment decoded
        // from an aligned read buffer then keeps every body buffer in place.
        let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5)?;
        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let encoded = generator.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut tracker,
            &options,
        );
        let dictionaries = tracker.dict_id().len();
        let mut schema_message = Vec::new();
        write_message(&mut schema_message, encoded, &options)?;
        Ok(Self {
            schema,
            schema_message: Buffer::from_vec(schema_message),
            options,
            generator,
            dictionaries,
        })
    }

    pub(in crate::datafusion) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Append `batch` as one self-contained segment.
    fn encode(
        &self,
        batch: &RecordBatch,
        context: &mut IpcWriteContext,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let batch = Self::compact_views(batch)?;
        let mut tracker = DictionaryTracker::new(false);
        for _ in 0..self.dictionaries {
            tracker.next_dict_id();
        }
        let (dictionaries, encoded) =
            self.generator
                .encode(&batch, &mut tracker, &self.options, context)?;
        for dictionary in dictionaries {
            write_message(&mut *out, dictionary, &self.options)?;
        }
        write_message(&mut *out, encoded, &self.options)?;
        Ok(())
    }

    /// IPC writes every data buffer a view array references, and a segment is
    /// gathered from whole input batches, so views are rewritten over their
    /// own bytes first.
    fn compact_views(batch: &RecordBatch) -> Result<RecordBatch> {
        if !batch.schema().fields().iter().any(|field| {
            matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView)
        }) {
            return Ok(batch.clone());
        }
        let columns = batch
            .columns()
            .iter()
            .map(|column| match column.data_type() {
                DataType::Utf8View => {
                    Arc::new(column.as_string_view().gc()) as ArrayRef
                }
                DataType::BinaryView => {
                    Arc::new(column.as_binary_view().gc()) as ArrayRef
                }
                _ => Arc::clone(column),
            })
            .collect();
        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    /// A decoder given the schema message. `StreamDecoder` completes a
    /// message only once a byte after it arrives, so the schema decodes with
    /// the first segment and the decoder is not at a message boundary before
    /// then: one is made only for a range that holds segments.
    fn decoder(&self) -> Result<StreamDecoder> {
        let mut decoder = StreamDecoder::new();
        let mut schema = self.schema_message.clone();
        decoder.decode(&mut schema)?;
        Ok(decoder)
    }
}

/// Writes one run. Rows arrive in non-decreasing bucket order.
pub(in crate::datafusion) struct BucketRunWriter<'a> {
    format: &'a BucketRunFormat,
    file: Arc<BackendTempFile>,
    /// Encoded bytes not yet appended; the file has one writer at a time.
    pending: Vec<u8>,
    /// Reuses the FlatBuffer builder across segments; each encoded body owns
    /// a separate buffer.
    context: IpcWriteContext,
    /// Start offset of each bucket reached so far.
    starts: Vec<u64>,
}

impl<'a> BucketRunWriter<'a> {
    pub(in crate::datafusion) fn create(format: &'a BucketRunFormat) -> Result<Self> {
        Ok(Self {
            format,
            file: Arc::new(BackendTempFile::create()?),
            pending: Vec::with_capacity(SPILL_CHUNK_BYTES),
            context: IpcWriteContext::default(),
            starts: Vec::new(),
        })
    }

    fn position(&self) -> u64 {
        self.file.written() + self.pending.len() as u64
    }

    /// Append rows of `bucket`, which is not below any bucket written before,
    /// returning the bytes a reader of the bucket will load for them.
    pub(in crate::datafusion) fn write(
        &mut self,
        bucket: usize,
        batch: &RecordBatch,
    ) -> Result<usize> {
        let position = self.position();
        self.starts.resize(bucket + 1, position);
        let pending = self.pending.len();
        self.format
            .encode(batch, &mut self.context, &mut self.pending)?;
        let written = self.pending.len() - pending;
        if self.pending.len() >= SPILL_CHUNK_BYTES {
            self.flush()?;
        }
        Ok(written)
    }

    fn flush(&mut self) -> Result<()> {
        self.file.append(&self.pending)?;
        self.pending.clear();
        Ok(())
    }

    /// Write the footer for `buckets` buckets and close the run.
    pub(in crate::datafusion) fn finish(
        mut self,
        buckets: usize,
    ) -> Result<BucketRun> {
        let footer = self.position();
        self.starts.resize(buckets + 1, footer);
        for start in &self.starts {
            self.pending.extend_from_slice(&start.to_le_bytes());
        }
        self.flush()?;
        Ok(BucketRun {
            file: self.file,
            footer,
        })
    }
}

/// A finished run. Dropping it closes and removes the file.
pub(in crate::datafusion) struct BucketRun {
    file: Arc<BackendTempFile>,
    footer: u64,
}

impl BucketRun {
    /// Bytes of the file, footer included.
    pub(in crate::datafusion) fn bytes(&self) -> u64 {
        self.file.written()
    }

    /// Decode the rows of `buckets` into `out` from one read of the range.
    pub(in crate::datafusion) fn read(
        &self,
        format: &BucketRunFormat,
        buckets: Range<usize>,
        out: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        let range = self.range(buckets)?;
        if range.is_empty() {
            return Ok(());
        }
        let mut data = Self::read_at(&self.file, range)?;
        let mut decoder = format.decoder()?;
        while !data.is_empty() {
            if let Some(batch) = decoder.decode(&mut data)? {
                out.push(batch);
            }
        }
        decoder.finish()?;
        Ok(())
    }

    /// The rows of `buckets`, decoded one chunk of the range at a time, or
    /// `None` when the run holds none of them, as `read` decodes nothing for
    /// an empty range.
    pub(in crate::datafusion) fn chunks(
        &self,
        format: &BucketRunFormat,
        buckets: Range<usize>,
    ) -> Result<Option<BucketChunks>> {
        let range = self.range(buckets)?;
        if range.is_empty() {
            return Ok(None);
        }
        Ok(Some(BucketChunks {
            file: Arc::clone(&self.file),
            range,
            decoder: format.decoder()?,
            chunk: Buffer::default(),
        }))
    }

    /// Arrow's buffer is 64-byte aligned and segments start 16-byte aligned,
    /// so the decoded arrays keep their body buffers in place instead of
    /// realigning them.
    fn read_at(file: &BackendTempFile, range: Range<u64>) -> Result<Buffer> {
        let mut data =
            MutableBuffer::from_len_zeroed((range.end - range.start) as usize);
        file.read_exact_at(data.as_slice_mut(), range.start)?;
        Ok(data.into())
    }

    /// One bucket, as a probe group or a bucket split again is read, has its
    /// two offsets next to each other, so they come in one read.
    fn range(&self, buckets: Range<usize>) -> Result<Range<u64>> {
        if buckets.len() == 1 {
            let [start, end] = self.offsets(buckets.start)?;
            return Ok(start..end);
        }
        let [start] = self.offsets(buckets.start)?;
        let [end] = self.offsets(buckets.end)?;
        Ok(start..end)
    }

    /// `N` consecutive offsets from that of `bucket`.
    fn offsets<const N: usize>(&self, bucket: usize) -> Result<[u64; N]> {
        let mut bytes = [[0; OFFSET_BYTES as usize]; N];
        self.file.read_exact_at(
            bytes.as_flattened_mut(),
            self.footer + bucket as u64 * OFFSET_BYTES,
        )?;
        Ok(bytes.map(u64::from_le_bytes))
    }
}

/// Batches of one non-empty bucket range of a run, read a chunk at a time: a batch
/// whose segment lies within one chunk shares it, and one that crosses chunks
/// is copied out by the decoder.
pub(in crate::datafusion) struct BucketChunks {
    file: Arc<BackendTempFile>,
    /// Bytes not read yet.
    range: Range<u64>,
    decoder: StreamDecoder,
    /// Bytes read but not decoded yet.
    chunk: Buffer,
}

impl BucketChunks {
    fn try_next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.chunk.is_empty() {
                if self.range.is_empty() {
                    self.decoder.finish()?;
                    return Ok(None);
                }
                let end = self
                    .range
                    .end
                    .min(self.range.start + SPILL_CHUNK_BYTES as u64);
                self.chunk = BucketRun::read_at(&self.file, self.range.start..end)?;
                self.range.start = end;
            }
            if let Some(batch) = self.decoder.decode(&mut self.chunk)? {
                return Ok(Some(batch));
            }
        }
    }
}

impl Iterator for BucketChunks {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
