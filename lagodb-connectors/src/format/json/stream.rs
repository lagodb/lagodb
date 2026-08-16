//! Bounded NDJSON framing across one ordered object set.

use std::io::{self, BufRead, BufReader, Read};
use std::num::NonZeroUsize;

use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::storage::ObjectFiles;

use super::super::{StreamCompression, StreamDecoder};

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
                    return Err(ConnectorError::JsonRecordTooLarge {
                        line: self.logical_line + 1,
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
}

type ObjectDecoder = JsonLineReader<BufReader<StreamDecoder<ObjectReader>>>;

pub(in crate::format) struct JsonRecordStream {
    files: ObjectFiles,
    compression: StreamCompression,
    max_record_bytes: NonZeroUsize,
    reader: Option<ObjectDecoder>,
}

impl JsonRecordStream {
    pub(in crate::format) fn new(
        files: ObjectFiles,
        compression: StreamCompression,
        max_record_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            files,
            compression,
            max_record_bytes,
            reader: None,
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
            self.reader = None;
        }
    }

    pub(in crate::format) fn reset(&mut self) {
        self.reader = None;
        self.files.reset();
    }

    pub(in crate::format) fn close(&mut self) {
        self.reader = None;
    }

    fn open_next(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let input =
            StreamDecoder::new(ObjectReader { file: file? }, self.compression)
                .map_err(ConnectorError::json_io)?;
        self.reader = Some(JsonLineReader::new(
            BufReader::new(input),
            self.max_record_bytes,
        ));
        Ok(true)
    }
}

struct ObjectReader {
    file: StorageFile,
}

impl Read for ObjectReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.file.read_into(output).map_err(io::Error::other)
    }
}
