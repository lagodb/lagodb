use std::any::Any;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use super::file_io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use crate::error::Result;
use crate::{Error, ErrorKind};

#[derive(Debug)]
pub struct MemoryStorage {
    fs: Arc<RwLock<HashMap<String, Bytes>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            fs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn delete(&self, path: &str) -> Result<()> {
        let mut fs = self
            .fs
            .write()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Lock poisoned"))?;
        fs.remove(path);
        Ok(())
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let mut fs = self
            .fs
            .write()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Lock poisoned"))?;
        let keys: Vec<String> = fs.keys().cloned().collect();
        for key in keys {
            if key.starts_with(path) {
                fs.remove(&key);
            }
        }
        Ok(())
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        let fs = self
            .fs
            .read()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Lock poisoned"))?;
        Ok(fs.get(path).map(|data| FileMetadata {
            size: data.len() as u64,
        }))
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let fs = self
            .fs
            .read()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Lock poisoned"))?;
        if let Some(data) = fs.get(path) {
            Ok(OpenedFile {
                metadata: FileMetadata {
                    size: data.len() as u64,
                },
                reader: Box::new(MemoryFileRead {
                    data: data.clone(),
                    position: 0,
                }),
            })
        } else {
            Err(Error::new(ErrorKind::IoError, format!("file not found: {path:?}")))
        }
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(MemoryFileWrite {
            fs: self.fs.clone(),
            path: path.to_string(),
            buffer: Vec::new(),
        }))
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        "memory"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MemoryFileRead {
    data: Bytes,
    position: usize,
}

impl FileRead for MemoryFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        let start = range.start as usize;
        let end = range.end as usize;
        if start > self.data.len() || end > self.data.len() {
            return Err(Error::new(ErrorKind::Unexpected, "Read out of range"));
        }
        Ok(self.data.slice(start..end))
    }

    fn read_all(&self) -> Result<Bytes> {
        Ok(self.data.clone())
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        Ok(Box::new(MemoryFileRead {
            data: self.data.clone(),
            position: self.position,
        }))
    }
}

impl Read for MemoryFileRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.data.len().saturating_sub(self.position);
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = std::cmp::min(remaining, buf.len());
        buf[..to_read].copy_from_slice(&self.data[self.position..self.position + to_read]);
        self.position += to_read;
        Ok(to_read)
    }
}

impl Seek for MemoryFileRead {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.data.len() as i64 + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as usize;
        Ok(self.position as u64)
    }
}

pub struct MemoryFileWrite {
    fs: Arc<RwLock<HashMap<String, Bytes>>>,
    path: String,
    buffer: Vec<u8>,
}

impl std::io::Write for MemoryFileWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Memory storage buffers until close, no-op for flush
        Ok(())
    }
}

impl FileWrite for MemoryFileWrite {
    fn close(&mut self) -> Result<()> {
        let mut fs = self
            .fs
            .write()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Lock poisoned"))?;
        fs.insert(
            self.path.clone(),
            Bytes::from(std::mem::take(&mut self.buffer)),
        );
        Ok(())
    }
}
