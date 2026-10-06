//! Bounded NDJSON framing across one ordered object set.

use std::io::{BufRead, BufReader};
use std::num::NonZeroUsize;

use crate::storage::InputFile;

use crate::error::ConnectorError;
use crate::storage::{ObjectFiles, ProgressReader, ReadProgress};

use super::super::{StreamCompressionOptions, StreamDecoder};

pub(super) struct JsonLineReader<R> {
    input: R,
    record: Vec<u8>,
    logical_line: u64,
    max_record_bytes: NonZeroUsize,
}

impl<R> JsonLineReader<R>
where
    R: BufRead,
{
    pub(super) fn new(input: R, max_record_bytes: NonZeroUsize) -> Self {
        Self {
            input,
            record: Vec::new(),
            logical_line: 0,
            max_record_bytes,
        }
    }

    pub(super) fn read_next(&mut self) -> Result<bool, ConnectorError> {
        loop {
            self.record.clear();
            let mut complete = false;
            while !complete {
                let available =
                    self.input.fill_buf().map_err(ConnectorError::json_io)?;
                if available.is_empty() {
                    if self.record.is_empty() {
                        return Ok(false);
                    }
                    break;
                }
                let newline = available.iter().position(|byte| *byte == b'\n');
                let payload_len = newline.unwrap_or(available.len());
                let next_len = self.record.len().saturating_add(payload_len);
                if next_len > self.max_record_bytes.get() {
                    let line = self.logical_line + 1;
                    self.discard_record()?;
                    self.logical_line = line;
                    return Err(ConnectorError::JsonRecordTooLarge {
                        line,
                        max_bytes: self.max_record_bytes.get(),
                    });
                }
                self.record.extend_from_slice(&available[..payload_len]);
                let consumed = payload_len + usize::from(newline.is_some());
                self.input.consume(consumed);
                complete = newline.is_some();
            }
            if self.record.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            self.logical_line += 1;
            return Ok(true);
        }
    }

    #[inline]
    pub(super) fn record(&self) -> &[u8] {
        &self.record
    }

    #[inline]
    pub(super) const fn logical_line(&self) -> u64 {
        self.logical_line
    }

    /// Discard the rest of an oversized logical record so `ON_ERROR IGNORE`
    /// can advance exactly once instead of repeatedly observing the same
    /// unread buffer. An I/O error remains fatal and supersedes the row error.
    fn discard_record(&mut self) -> Result<(), ConnectorError> {
        loop {
            let available = self.input.fill_buf().map_err(ConnectorError::json_io)?;
            if available.is_empty() {
                return Ok(());
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map_or(available.len(), |index| index + 1);
            self.input.consume(consumed);
            if newline.is_some() {
                return Ok(());
            }
        }
    }
}

type ObjectDecoder =
    JsonLineReader<BufReader<StreamDecoder<ProgressReader<InputFile>>>>;

pub(in crate::format) struct JsonRecordStream {
    files: ObjectFiles,
    compression: StreamCompressionOptions,
    max_record_bytes: NonZeroUsize,
    reader: Option<ObjectDecoder>,
    track_progress: bool,
    completed_bytes: u64,
    current_progress: Option<ReadProgress>,
}

impl JsonRecordStream {
    pub(in crate::format) fn new(
        files: ObjectFiles,
        compression: StreamCompressionOptions,
        max_record_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            files,
            compression,
            max_record_bytes,
            reader: None,
            track_progress: false,
            completed_bytes: 0,
            current_progress: None,
        }
    }

    pub(in crate::format) fn with_progress(
        files: ObjectFiles,
        compression: StreamCompressionOptions,
        max_record_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            files,
            compression,
            max_record_bytes,
            reader: None,
            track_progress: true,
            completed_bytes: 0,
            current_progress: None,
        }
    }

    pub(in crate::format) fn next_record(
        &mut self,
    ) -> Result<Option<(u64, &[u8])>, ConnectorError> {
        loop {
            if self.reader.is_none() && !self.open_next()? {
                return Ok(None);
            }
            let has_record = self
                .reader
                .as_mut()
                .expect("an opened JSON object owns a line reader")
                .read_next()?;
            if has_record {
                let reader = self
                    .reader
                    .as_ref()
                    .expect("the JSON line reader still owns the current record");
                return Ok(Some((reader.logical_line(), reader.record())));
            }
            self.finish_current_reader();
            self.reader = None;
        }
    }

    pub(in crate::format) fn reset(&mut self) {
        self.reader = None;
        self.completed_bytes = 0;
        self.current_progress = None;
        self.files.reset();
    }

    pub(in crate::format) fn close(&mut self) {
        self.reader = None;
        self.current_progress = None;
    }

    pub(in crate::format) fn bytes_consumed(&self) -> u64 {
        self.completed_bytes.saturating_add(
            self.current_progress
                .as_ref()
                .map_or(0, ReadProgress::bytes),
        )
    }

    fn open_next(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let file = file?;
        let compression = self.compression.for_file(&file);
        let progress = self.track_progress.then(ReadProgress::default);
        let input = StreamDecoder::new(
            ProgressReader::new(file, progress.clone()),
            compression,
        )
        .map_err(ConnectorError::json_io)?;
        self.current_progress = progress;
        self.reader = Some(JsonLineReader::new(
            BufReader::new(input),
            self.max_record_bytes,
        ));
        Ok(true)
    }

    fn finish_current_reader(&mut self) {
        if let Some(progress) = self.current_progress.take() {
            self.completed_bytes =
                self.completed_bytes.saturating_add(progress.bytes());
        }
    }
}
