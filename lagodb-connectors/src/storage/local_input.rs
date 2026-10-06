//! Stable local directory membership, shared by schema inference and scans.

use std::fs;
use std::path::PathBuf;

use lagodb_storage::StorageError;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::local::LocalFdReservation;
use super::{InputFile, ObjectLocationKind};

pub(crate) struct LocalInput {
    paths: Box<[PathBuf]>,
    total_bytes: u64,
}

impl LocalInput {
    pub(super) fn resolve(
        path: &str,
        kind: ObjectLocationKind,
        matches: impl Fn(&str) -> bool,
    ) -> Result<Self, ConnectorError> {
        if kind == ObjectLocationKind::Exact {
            let metadata = fs::metadata(path).map_err(|error| {
                StorageError::io(format!("stat local file {path}"), error)
            })?;
            return Ok(Self {
                paths: vec![PathBuf::from(path)].into_boxed_slice(),
                total_bytes: metadata.len(),
            });
        }
        let mut directories = vec![PathBuf::from(path)];
        let mut paths = Vec::new();
        let mut total_bytes = 0_u64;
        while let Some(directory) = directories.pop() {
            let _reservation = LocalFdReservation::acquire()?;
            let entries = fs::read_dir(&directory).map_err(|error| {
                StorageError::io(
                    format!("list local directory {}", directory.display()),
                    error,
                )
            })?;
            for entry in entries {
                pg_sys::check_for_interrupts!();
                let entry = entry.map_err(StorageError::from)?;
                let file_type = entry.file_type().map_err(StorageError::from)?;
                if file_type.is_dir() {
                    directories.push(entry.path());
                    continue;
                }
                let path = entry.path();
                if !matches(&path.to_string_lossy()) {
                    continue;
                }
                let metadata = fs::metadata(&path).map_err(StorageError::from)?;
                // Do not follow directory symlinks: recursive membership must
                // not cycle. Symlinks to ordinary files retain normal file semantics.
                if metadata.is_file() {
                    total_bytes = total_bytes.saturating_add(metadata.len());
                    paths.push(path);
                }
            }
        }
        paths.sort_unstable();
        Ok(Self {
            paths: paths.into_boxed_slice(),
            total_bytes,
        })
    }

    pub(super) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(super) fn open(self) -> LocalFiles {
        LocalFiles {
            paths: self.paths,
            index: 0,
        }
    }
}

pub(crate) struct LocalFiles {
    paths: Box<[PathBuf]>,
    index: usize,
}

impl LocalFiles {
    pub(super) fn reset(&mut self) {
        self.index = 0;
    }
}

impl Iterator for LocalFiles {
    type Item = Result<InputFile, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.paths.get(self.index)?;
        self.index += 1;
        Some(InputFile::local(path).map_err(ConnectorError::from))
    }
}
