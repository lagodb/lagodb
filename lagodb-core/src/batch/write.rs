//! Write side: buffering lifecycle shared by batch writers.

/// Common lifecycle for buffering AM-specific batches.
///
/// This trait intentionally does not define an append method. Provider-owned
/// buffers bind their own source ownership rules (relation slot views or
/// slot-free datum streams), but share flush, clear, length, and memory
/// accounting behavior.
pub trait BatchBuffer {
    /// The completed batch type consumed by the writer.
    type Batch;
    /// Error raised while appending rows or finishing the batch.
    type Error;

    /// Finish the current batch and reset the buffer for reuse.
    fn finish_batch(&mut self) -> Result<Self::Batch, Self::Error>;

    /// Drop all buffered data.
    fn clear(&mut self);

    /// Number of rows currently buffered.
    fn len(&self) -> usize;

    /// Estimated memory footprint of buffered data in bytes.
    fn estimated_size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` when the caller should flush the buffered batch.
    ///
    /// The default checks whether the estimated footprint has reached
    /// `max_bytes`. Implementations may also impose a row-count limit so
    /// values with little or no estimated payload still trigger a flush.
    ///
    /// This is a **memory-pressure** signal, not a target file-size signal:
    /// `estimated_size` reflects the in-memory representation of buffered
    /// data, which has only a loose relationship to the size of any file that
    /// may eventually be produced from it (Parquet, for example, is
    /// column-encoded, dictionary-compressed, and Snappy/ZSTD-compressed). Concrete output file sizing belongs to
    /// the writer layer (e.g. the rolling file writer's target file size).
    fn should_flush(&self, max_bytes: usize) -> bool {
        self.estimated_size() >= max_bytes
    }
}
