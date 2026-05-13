use std::any::Any;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;

use bytes::Bytes;

use super::file_io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use crate::error::Result;

#[derive(Debug, Default)]
pub struct LocalStorage;

impl Storage for LocalStorage {
    fn delete(&self, path: &str) -> Result<()> {
        match fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let p = Path::new(path);
        if p.exists() {
            if p.is_dir() {
                fs::remove_dir_all(p)?;
            } else {
                fs::remove_file(p)?;
            }
        }
        Ok(())
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        match fs::metadata(path) {
            Ok(metadata) => Ok(Some(FileMetadata {
                size: metadata.len(),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        Ok(OpenedFile {
            metadata: FileMetadata {
                size: metadata.len(),
            },
            reader: Box::new(LocalFileRead { file }),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Box::new(LocalFileWrite { file }))
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        "file"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct LocalFileRead {
    file: File,
}

impl FileRead for LocalFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(range.start))?;
        let len = (range.end - range.start) as usize;
        let mut buffer = vec![0; len];
        file.read_exact(&mut buffer)?;
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(Bytes::from(buffer))
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        Ok(Box::new(LocalFileRead {
            file: self.file.try_clone()?,
        }))
    }
}

impl Read for LocalFileRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for LocalFileRead {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

pub struct LocalFileWrite {
    file: File,
}

impl Write for LocalFileWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl FileWrite for LocalFileWrite {
    fn close(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }
}
