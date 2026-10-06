//! Random-access adapter from connector input files to Parquet's reader.

use std::io::{self, BufReader, Read};
use std::rc::Rc;

use bytes::Bytes;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

use crate::storage::{InputFile, ReadProgress};

pub(crate) struct ParquetObjectReader {
    file: Rc<InputFile>,
    progress: Option<ReadProgress>,
}

// SAFETY: The connector only constructs, uses, and drops this adapter on the
// PostgreSQL backend thread. The synchronous Parquet readers used here do not
// move it to a worker thread or invoke its read methods concurrently. The
// underlying InputFile intentionally remains !Send and !Sync; these impls
// are confined to this crate-private PostgreSQL adapter boundary.
unsafe impl Send for ParquetObjectReader {}
unsafe impl Sync for ParquetObjectReader {}

impl ParquetObjectReader {
    pub(crate) fn new(file: InputFile) -> Self {
        Self {
            file: Rc::new(file),
            progress: None,
        }
    }

    pub(crate) fn with_progress(file: InputFile) -> (Self, ReadProgress) {
        let mut reader = Self::new(file);
        let progress = ReadProgress::default();
        reader.progress = Some(progress.clone());
        (reader, progress)
    }
}

impl Length for ParquetObjectReader {
    fn len(&self) -> u64 {
        self.file.size()
    }
}

impl ChunkReader for ParquetObjectReader {
    type T = BufReader<ParquetObjectRange>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len() {
            return Err(ParquetError::EOF(format!(
                "read offset {start} exceeds object size {}",
                self.len()
            )));
        }
        // Thrift reads page headers a byte at a time. Buffer this sequential
        // view as parquet's File adapter does; get_bytes keeps bulk range I/O.
        Ok(BufReader::new(ParquetObjectRange {
            file: Rc::clone(&self.file),
            position: start,
            progress: self.progress.clone(),
        }))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length_u64 = u64::try_from(length)?;
        let end = start.checked_add(length_u64).ok_or_else(|| {
            ParquetError::EOF("requested Parquet range overflows u64".to_owned())
        })?;
        if end > self.len() {
            return Err(ParquetError::EOF(format!(
                "requested range {start}..{end} exceeds object size {}",
                self.len()
            )));
        }
        // InputFile fills initialized caller-owned buffers directly for both
        // mediated and direct I/O paths.
        let mut data = vec![0_u8; length];
        let mut position = start;
        let mut written = 0;
        while written < length {
            let remaining = length - written;
            let request_len = remaining.min(u32::MAX as usize);
            let read = self
                .file
                .read_at_into(position, &mut data[written..written + request_len])
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if read == 0 {
                return Err(ParquetError::EOF(format!(
                    "object ended while reading range {start}..{end}"
                )));
            }
            if let Some(progress) = &self.progress {
                progress.record(read);
            }
            position += read as u64;
            written += read;
        }
        Ok(Bytes::from(data))
    }
}

pub(crate) struct ParquetObjectRange {
    file: Rc<InputFile>,
    position: u64,
    progress: Option<ReadProgress>,
}

impl Read for ParquetObjectRange {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self
            .file
            .read_at_into(self.position, output)
            .map_err(io::Error::other)?;
        if let Some(progress) = &self.progress {
            progress.record(read);
        }
        self.position += read as u64;
        Ok(read)
    }
}
