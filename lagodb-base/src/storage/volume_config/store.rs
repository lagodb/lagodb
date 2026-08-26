use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::domain::{StorageVolumeError, StorageVolumeSnapshot};
use super::error::ConfigSecurityError;
use super::lifecycle::UnixMillis;
use pgrx::pg_sys;

const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const CONFIG_WRITER_LOCK_CLASS: u16 = 0x4c43;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct StorageVolumeConfigStore {
    directory: PathBuf,
    config_path: PathBuf,
}

impl StorageVolumeConfigStore {
    pub(crate) fn for_current_data_directory() -> Self {
        // SAFETY: callers run after PostgreSQL initializes DataDir.
        let data_directory = unsafe { CStr::from_ptr(pgrx::pg_sys::DataDir) };
        Self::for_data_directory(PathBuf::from(std::ffi::OsStr::from_bytes(
            data_directory.to_bytes(),
        )))
    }

    pub(crate) fn for_data_directory(data_directory: impl AsRef<Path>) -> Self {
        let directory = data_directory.as_ref().join("pg_lakebase");
        Self {
            config_path: directory.join("storage-volumes.json"),
            directory,
        }
    }

    pub(crate) fn initialize_if_missing(&self) -> Result<(), StorageVolumeError> {
        self.ensure_directory()?;
        match self.read() {
            Ok(_) => Ok(()),
            Err(StorageVolumeError::ConfigIo { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                self.write_snapshot(&StorageVolumeSnapshot::default())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn read(&self) -> Result<StorageVolumeSnapshot, StorageVolumeError> {
        let mut file = open_secure_existing(&self.config_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|source| {
            StorageVolumeError::config_io("read", &self.config_path, source)
        })?;
        let snapshot: StorageVolumeSnapshot = serde_json::from_slice(&bytes)
            .map_err(|source| StorageVolumeError::ConfigJson {
                path: self.config_path.clone(),
                source,
            })?;
        snapshot
            .validate()
            .map_err(StorageVolumeError::InvalidSnapshot)?;
        Ok(snapshot)
    }

    /// Serialize mutation writers with a short-lived cluster-wide session lock
    /// and atomically publish a complete snapshot when the value changes.
    pub(crate) fn update<T>(
        &self,
        mutation: impl FnOnce(
            &mut StorageVolumeSnapshot,
        ) -> Result<(T, bool), StorageVolumeError>,
    ) -> Result<(T, bool), StorageVolumeError> {
        let _guard = ConfigWriteGuard::acquire();
        self.ensure_directory()?;
        let mut snapshot = self.read()?;
        let (value, changed) = mutation(&mut snapshot)?;
        if changed {
            snapshot
                .validate()
                .map_err(StorageVolumeError::InvalidSnapshot)?;
            self.write_snapshot(&snapshot)?;
        }
        Ok((value, changed))
    }

    pub(crate) fn sweep_due_volumes(&self) -> Result<(), StorageVolumeError> {
        let _ = self.update(|snapshot| {
            let now = UnixMillis::now()?;
            Ok(((), snapshot.sweep_due(now)))
        })?;
        Ok(())
    }

    fn ensure_directory(&self) -> Result<(), StorageVolumeError> {
        match std::fs::symlink_metadata(&self.directory) {
            Ok(metadata) => self.validate_directory(&metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = std::fs::DirBuilder::new();
                builder.mode(DIRECTORY_MODE);
                match builder.create(&self.directory) {
                    Ok(()) => {}
                    Err(error)
                        if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(source) => {
                        return Err(StorageVolumeError::config_io(
                            "create directory",
                            &self.directory,
                            source,
                        ));
                    }
                }
                let metadata =
                    std::fs::symlink_metadata(&self.directory).map_err(|source| {
                        StorageVolumeError::config_io(
                            "inspect directory",
                            &self.directory,
                            source,
                        )
                    })?;
                self.validate_directory(&metadata)?;
            }
            Err(source) => {
                return Err(StorageVolumeError::config_io(
                    "inspect directory",
                    &self.directory,
                    source,
                ));
            }
        }
        Ok(())
    }

    fn validate_directory(
        &self,
        metadata: &std::fs::Metadata,
    ) -> Result<(), StorageVolumeError> {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(StorageVolumeError::ConfigSecurity {
                path: self.directory.clone(),
                source: ConfigSecurityError::NotDirectory,
            });
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(StorageVolumeError::ConfigSecurity {
                path: self.directory.clone(),
                source: ConfigSecurityError::WrongOwner,
            });
        }
        let actual = metadata.mode() & 0o777;
        if actual != DIRECTORY_MODE {
            return Err(StorageVolumeError::ConfigSecurity {
                path: self.directory.clone(),
                source: ConfigSecurityError::WrongMode {
                    actual,
                    expected: DIRECTORY_MODE,
                },
            });
        }
        Ok(())
    }

    fn write_snapshot(
        &self,
        snapshot: &StorageVolumeSnapshot,
    ) -> Result<(), StorageVolumeError> {
        let mut temp = TempFile::create_unique(&self.directory)?;
        let mut bytes = serde_json::to_vec_pretty(snapshot).map_err(|source| {
            StorageVolumeError::ConfigJson {
                path: self.config_path.clone(),
                source,
            }
        })?;
        bytes.push(b'\n');
        temp.file.write_all(&bytes).map_err(|source| {
            StorageVolumeError::config_io("write temporary file", &temp.path, source)
        })?;
        temp.file.flush().map_err(|source| {
            StorageVolumeError::config_io("flush temporary file", &temp.path, source)
        })?;
        temp.file.sync_all().map_err(|source| {
            StorageVolumeError::config_io("sync temporary file", &temp.path, source)
        })?;
        std::fs::rename(&temp.path, &self.config_path).map_err(|source| {
            StorageVolumeError::config_io("publish", &self.config_path, source)
        })?;
        temp.published = true;
        let directory = File::open(&self.directory).map_err(|source| {
            StorageVolumeError::config_io_published(
                "open directory",
                &self.directory,
                source,
            )
        })?;
        directory.sync_all().map_err(|source| {
            StorageVolumeError::config_io_published(
                "sync directory",
                &self.directory,
                source,
            )
        })?;
        Ok(())
    }
}

struct ConfigWriteGuard;

impl ConfigWriteGuard {
    fn acquire() -> Self {
        let tag = Self::lock_tag();
        // SAFETY: all callers run on a PostgreSQL backend or background-worker
        // main thread. A session lock is released explicitly by this guard.
        let result = unsafe {
            pg_sys::LockAcquire(&tag, pg_sys::ExclusiveLock as _, true, false)
        };
        debug_assert!(result != pg_sys::LockAcquireResult::LOCKACQUIRE_NOT_AVAIL);
        Self
    }

    fn lock_tag() -> pg_sys::LOCKTAG {
        pg_sys::LOCKTAG {
            locktag_field1: pg_sys::InvalidOid.to_u32(),
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: CONFIG_WRITER_LOCK_CLASS,
            locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
            locktag_lockmethodid: pg_sys::USER_LOCKMETHOD as u8,
        }
    }
}

impl Drop for ConfigWriteGuard {
    fn drop(&mut self) {
        let tag = Self::lock_tag();
        // SAFETY: this guard acquired the same session-scoped advisory lock and
        // is dropped before callers report any returned storage error.
        let released =
            unsafe { pg_sys::LockRelease(&tag, pg_sys::ExclusiveLock as _, true) };
        debug_assert!(released);
    }
}

fn open_secure_existing(path: &Path) -> Result<File, StorageVolumeError> {
    let open = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path);
    let file = match open {
        Ok(file) => file,
        Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
            return Err(StorageVolumeError::ConfigSecurity {
                path: path.to_path_buf(),
                source: ConfigSecurityError::NotRegularFile,
            });
        }
        Err(source) => {
            return Err(StorageVolumeError::config_io("open", path, source));
        }
    };
    validate_secure_file(&file, path)?;
    Ok(file)
}

fn validate_secure_file(file: &File, path: &Path) -> Result<(), StorageVolumeError> {
    let metadata = file.metadata().map_err(|source| {
        StorageVolumeError::config_io("inspect file", path, source)
    })?;
    if !metadata.is_file() {
        return Err(StorageVolumeError::ConfigSecurity {
            path: path.to_path_buf(),
            source: ConfigSecurityError::NotRegularFile,
        });
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(StorageVolumeError::ConfigSecurity {
            path: path.to_path_buf(),
            source: ConfigSecurityError::WrongOwner,
        });
    }
    let actual = metadata.mode() & 0o777;
    if actual != FILE_MODE {
        return Err(StorageVolumeError::ConfigSecurity {
            path: path.to_path_buf(),
            source: ConfigSecurityError::WrongMode {
                actual,
                expected: FILE_MODE,
            },
        });
    }
    Ok(())
}

struct TempFile {
    path: PathBuf,
    file: File,
    published: bool,
}

impl TempFile {
    fn create_unique(directory: &Path) -> Result<Self, StorageVolumeError> {
        loop {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!(
                ".storage-volumes.json.tmp.{}.{}",
                std::process::id(),
                sequence
            ));
            let open = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(FILE_MODE)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&path);
            match open {
                Ok(file) => {
                    validate_secure_file(&file, &path)?;
                    return Ok(Self {
                        path,
                        file,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(StorageVolumeError::config_io(
                        "create temporary file",
                        path,
                        source,
                    ));
                }
            }
        }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
