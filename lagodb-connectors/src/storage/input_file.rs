//! One format-reader surface for local files and storage-service objects.

use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;

use lagodb_storage::{StorageFile, StorageResult};

use super::local::LocalFile;

pub(crate) struct InputFile {
    inner: InputFileKind,
    name: Box<str>,
}

enum InputFileKind {
    Object(StorageFile),
    Local(LocalFile),
    Closed,
}

impl InputFile {
    pub(super) fn object(file: StorageFile, name: &str) -> Self {
        Self {
            inner: InputFileKind::Object(file),
            name: name.into(),
        }
    }

    pub(super) fn local(path: &Path) -> StorageResult<Self> {
        Ok(Self {
            inner: InputFileKind::Local(LocalFile::open(path)?),
            name: path.to_string_lossy().into_owned().into_boxed_str(),
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Length reported by the storage object or local file metadata.
    pub(crate) fn size(&self) -> u64 {
        match &self.inner {
            InputFileKind::Object(file) => file.size(),
            InputFileKind::Local(file) => file.size,
            InputFileKind::Closed => {
                unreachable!("format readers retain an open input file")
            }
        }
    }

    pub(crate) fn read_into(&mut self, output: &mut [u8]) -> StorageResult<usize> {
        match &mut self.inner {
            InputFileKind::Object(file) => file.read_into(output),
            InputFileKind::Local(file) => file.file.read(output).map_err(Into::into),
            InputFileKind::Closed => {
                unreachable!("format readers retain an open input file")
            }
        }
    }

    pub(crate) fn read_at_into(
        &self,
        offset: u64,
        output: &mut [u8],
    ) -> StorageResult<usize> {
        match &self.inner {
            InputFileKind::Object(file) => file.read_at_into(offset, output),
            InputFileKind::Local(file) => {
                file.file.read_at(output, offset).map_err(Into::into)
            }
            InputFileKind::Closed => {
                unreachable!("format readers retain an open input file")
            }
        }
    }

    pub(crate) fn read_at(&self, offset: u64, length: u32) -> StorageResult<Vec<u8>> {
        match &self.inner {
            InputFileKind::Object(file) => file.read_at(offset, length),
            InputFileKind::Local(_) => {
                let remaining = self.size().saturating_sub(offset);
                let length = u64::from(length).min(remaining) as usize;
                let mut output = vec![0; length];
                let mut read = 0;
                while read < length {
                    let count =
                        self.read_at_into(offset + read as u64, &mut output[read..])?;
                    if count == 0 {
                        break;
                    }
                    read += count;
                }
                output.truncate(read);
                Ok(output)
            }
            InputFileKind::Closed => {
                unreachable!("format readers retain an open input file")
            }
        }
    }

    pub(crate) fn close(&mut self) -> StorageResult<()> {
        if let InputFileKind::Object(file) = &mut self.inner {
            file.close()?;
        }
        self.inner = InputFileKind::Closed;
        Ok(())
    }
}

impl Read for InputFile {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.read_into(output).map_err(io::Error::other)
    }
}
