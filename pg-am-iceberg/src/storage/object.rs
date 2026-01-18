use iceberg_lite::Result;
use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, Storage};
use std::any::Any;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ObjectStorage {
    scheme: String,
}

impl ObjectStorage {
    pub fn new(scheme: impl Into<String>) -> Self {
        Self {
            scheme: scheme.into(),
        }
    }
}

impl Default for ObjectStorage {
    fn default() -> Self {
        Self::new("s3")
    }
}

impl Storage for ObjectStorage {
    fn exists(&self, _path: &str) -> Result<bool> {
        unimplemented!("ObjectStorage::exists")
    }

    fn delete(&self, _path: &str) -> Result<()> {
        unimplemented!("ObjectStorage::delete")
    }

    fn remove_dir_all(&self, _path: &str) -> Result<()> {
        unimplemented!("ObjectStorage::remove_dir_all")
    }

    fn metadata(&self, _path: &str) -> Result<FileMetadata> {
        unimplemented!("ObjectStorage::metadata")
    }

    fn reader(&self, _path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(ObjectReader))
    }

    fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(ObjectWriter))
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        // Props are used by the actual implementation to configure
        // the connection to the object store (e.g., S3 credentials)
        Ok(())
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct ObjectReader;

impl std::io::Read for ObjectReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!("ObjectReader::read")
    }
}

impl std::io::Seek for ObjectReader {
    fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
        unimplemented!("ObjectReader::seek")
    }
}

impl FileRead for ObjectReader {
    fn read_range(&self, _range: std::ops::Range<u64>) -> Result<bytes::Bytes> {
        unimplemented!("ObjectReader::read_range")
    }

    fn read_all(&self) -> Result<bytes::Bytes> {
        unimplemented!("ObjectReader::read_all")
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        Ok(Box::new(ObjectReader))
    }
}

pub struct ObjectWriter;

impl std::io::Write for ObjectWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!("ObjectWriter::write")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!("ObjectWriter::flush")
    }
}

impl FileWrite for ObjectWriter {
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
