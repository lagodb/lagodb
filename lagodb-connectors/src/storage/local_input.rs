//! Stable local directory membership, shared by schema inference and scans.

use std::fs;
use std::path::PathBuf;

use lagodb_storage::StorageError;
use pgrx::check_for_interrupts;

use crate::error::ConnectorError;

use super::ObjectLocationKind;
use super::input::InputMember;
use super::local::LocalFdReservation;

/// Local-file membership resolution.
pub(super) struct LocalInput;

impl LocalInput {
    pub(super) fn resolve(
        path: &str,
        kind: ObjectLocationKind,
        matches: impl Fn(&str) -> bool,
    ) -> Result<Vec<InputMember<PathBuf>>, ConnectorError> {
        if kind == ObjectLocationKind::Exact {
            let metadata = fs::metadata(path).map_err(|error| {
                StorageError::io(format!("stat local file {path}"), error)
            })?;
            return Ok(vec![InputMember::new(PathBuf::from(path), metadata.len())]);
        }
        let mut directories = vec![PathBuf::from(path)];
        let mut files = Vec::new();
        while let Some(directory) = directories.pop() {
            let _reservation = LocalFdReservation::acquire()?;
            let entries = fs::read_dir(&directory).map_err(|error| {
                StorageError::io(
                    format!("list local directory {}", directory.display()),
                    error,
                )
            })?;
            for entry in entries {
                check_for_interrupts();
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
                    files.push((path, metadata.len()));
                }
            }
        }
        files.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        Ok(files
            .into_iter()
            .map(|(path, size)| InputMember::new(path, size))
            .collect())
    }
}
