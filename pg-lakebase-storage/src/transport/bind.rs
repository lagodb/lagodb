//! Unix socket bind helper: safely binds a pathname socket, probing for stale files before
//! unlinking.

use std::fs::{remove_file, symlink_metadata};
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream;
use std::path::Path;

use tokio::net::UnixListener;

use crate::error::{StorageError, StorageResult};

/// Bind a pathname unix socket without unlinking a live listener.
///
/// Attempts `bind` first. On `AddrInUse`, probes with `connect`: success means another server
/// owns the path; `ConnectionRefused` usually indicates a stale socket file (safe to remove and
/// retry).
pub fn bind_storage_unix_listener(path: &Path) -> StorageResult<UnixListener> {
    UnixListener::bind(path).or_else(|bind_err| bind_unix_listener_after_addr_in_use(path, bind_err))
}

fn bind_unix_listener_after_addr_in_use(path: &Path, bind_err: io::Error) -> StorageResult<UnixListener> {
    if bind_err.kind() != io::ErrorKind::AddrInUse {
        return Err(bind_err.into());
    }

    let meta = match symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(bind_err.into()),
        Err(err) => return Err(err.into()),
    };
    if !meta.file_type().is_socket() {
        return Err(bind_err.into());
    }

    // Live server accepts connect(); stale socket files typically refuse quickly so we can unlink
    // and retry bind.
    match UnixStream::connect(path) {
        Ok(conn) => {
            drop(conn);
            Err(StorageError::io("storage unix socket path already has a listening server", bind_err))
        },
        Err(conn_err) if conn_err.kind() == io::ErrorKind::ConnectionRefused => {
            remove_file(path)?;
            UnixListener::bind(path).map_err(|e| e.into())
        },
        Err(conn_err) => Err(StorageError::io(
            format!(
                "storage unix socket bind failed ({bind_err}); \
                 connect probe inconclusive, leaving socket path unchanged",
            ),
            conn_err,
        )),
    }
}
