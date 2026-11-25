use std::{mem, ptr, str};

use super::rmgr::ICEBERG_RMGR_ID;
use lagodb_core::diag;
use lagodb_core::wal::{WalRecordBuilder, XLogRecPtr};

// ============================================================================
// WAL operation types
// ============================================================================

/// WAL record operation types for Iceberg
///
/// This enum defines the file system operations that are logged
/// to the WAL for standby WAL replay or archive recovery of local Iceberg files:
/// - WriteFile: Write data to a file (creates file and parent directories if
///   offset is 0)
/// - DeleteDirectory: Remove a directory and its contents after the PostgreSQL
///   transaction has committed
/// - DeleteFiles: Remove canceled transaction-created files or VACUUM cleanup
///   files after commit
/// - TruncateDirectory: Clear a disposable transaction-local storage generation
///
/// Invariants:
/// - These WAL records are only for local file systems. Distributed storage
///   (S3, GCS, Azure) guarantees durability after successful writes and does
///   not use WAL-based redo.
/// - `WRITE_FILE` redo is skipped during local crash recovery because
///   successful explicit writer close syncs the file. Directory entries are
///   not synced; see the WAL module's known design debt.
/// - Standby WAL replay or archive recovery uses these records for best-effort,
///   lossy reconstruction because local Iceberg files may not exist on the
///   target system.
/// - `DELETE_DIRECTORY` and `DELETE_FILES` are post-commit cleanup. PostgreSQL
///   extensions cannot add arbitrary AM paths to core commit/abort records, and
///   PostgreSQL's `smgr` switch is not extension-customizable, so we must never
///   log those retirement records before the transaction outcome is known.
/// - `TRUNCATE_DIRECTORY` only clears a transaction-local generation selected
///   by PostgreSQL. It can precede commit because rollback discards that
///   generation, and its redo is skipped during primary crash recovery.
/// - Orphaned files on distributed storage should be cleaned up via a separate
///   garbage collection mechanism (e.g., Iceberg's remove_orphan_files).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IcebergWalOp {
    /// Delete a directory (and all its contents)
    DeleteDirectory = 0x00,
    /// Write data to a file (creates file and parent directories if offset is 0)
    WriteFile = 0x10,
    /// Delete bounded batches of local files after commit.
    DeleteFiles = 0x20,
    /// Clear a transaction-local generation before rebuilding an empty table.
    /// Primary crash recovery skips this record, just as it skips file writes.
    TruncateDirectory = 0x30,
}

impl IcebergWalOp {
    /// Parse operation type from WAL info byte
    ///
    /// The info byte contains the operation type in the high 4 bits.
    /// Returns None if the operation type is not recognized.
    pub(super) fn from_info(info: u8) -> Option<Self> {
        let op = info & 0xF0;
        match op {
            0x00 => Some(Self::DeleteDirectory),
            0x10 => Some(Self::WriteFile),
            0x20 => Some(Self::DeleteFiles),
            0x30 => Some(Self::TruncateDirectory),
            _ => None,
        }
    }

    /// Get the human-readable name of this operation
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::DeleteDirectory => "DELETE_DIRECTORY",
            Self::WriteFile => "WRITE_FILE",
            Self::DeleteFiles => "DELETE_FILES",
            Self::TruncateDirectory => "TRUNCATE_DIRECTORY",
        }
    }
}

// ============================================================================
// WAL Record Data Structures & Zero-Copy Codecs
// ============================================================================

/// Shared path header for directory retirement and in-place truncation.
///
/// Layout in WAL record:
/// - DirectoryHeader (this struct)
/// - path bytes (path_len bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DirectoryHeader {
    /// Length of the directory path (not including null terminator)
    pub path_len: u32,
}

/// Size of DirectoryHeader in bytes
pub(super) const SIZE_OF_DIRECTORY: usize = mem::size_of::<DirectoryHeader>();

impl DirectoryHeader {
    /// Borrow the directory path from a decoded `DELETE_DIRECTORY` or
    /// `TRUNCATE_DIRECTORY` record payload.
    pub(super) fn decode_path(data: &[u8]) -> Option<&str> {
        if data.len() < SIZE_OF_DIRECTORY {
            return None;
        }

        // SAFETY: the preceding length check proves that `data` contains at
        // least one complete header. `read_unaligned` permits the byte slice's
        // alignment, and the header consists entirely of integer fields for
        // which every bit pattern is valid.
        let header = unsafe { ptr::read_unaligned(data.as_ptr().cast::<Self>()) };

        let path_end = SIZE_OF_DIRECTORY.checked_add(header.path_len as usize)?;
        let path_bytes = data.get(SIZE_OF_DIRECTORY..path_end)?;
        str::from_utf8(path_bytes).ok()
    }

    fn insert(path: &str, op: IcebergWalOp) -> XLogRecPtr {
        let header = Self {
            path_len: path.len() as u32,
        };
        let mut builder = WalRecordBuilder::begin();
        // SAFETY: the integer-only header remains live until insert consumes
        // the record builder, which copies the registered bytes into WAL.
        unsafe {
            builder.register_data_as(&header);
        }
        builder.register_data(path.as_bytes());
        builder.insert(ICEBERG_RMGR_ID.as_u8(), op as u8)
    }
}

/// Header for one bounded batch of file paths.
///
/// The payload repeats `[path_len: u32][path bytes]` `path_count` times.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DeleteFilesHeader {
    pub path_count: u32,
    pub payload_len: u32,
}

pub(super) const SIZE_OF_DELETE_FILES: usize = mem::size_of::<DeleteFilesHeader>();

/// Project batching policy, not PostgreSQL's WAL record limit. These bounds
/// keep one cleanup record and its redo path vector small; larger transactions
/// are split across records rather than rejected.
pub(super) const MAX_DELETE_FILES_PER_RECORD: usize = 256;
pub(super) const MAX_DELETE_FILES_PAYLOAD_BYTES: usize = 64 * 1024;

/// Validated, zero-allocation iterator over a `DELETE_FILES` record's paths.
#[derive(Clone, Copy, Debug)]
pub(super) struct DeleteFilesPaths<'a> {
    payload: &'a [u8],
    remaining: u32,
}

impl<'a> DeleteFilesPaths<'a> {
    #[inline]
    pub(super) const fn len(&self) -> usize {
        self.remaining as usize
    }
}

impl<'a> Iterator for DeleteFilesPaths<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let (len_bytes, rest) = self.payload.split_first_chunk::<4>()?;
        let path_len = u32::from_ne_bytes(*len_bytes) as usize;
        let (path_bytes, tail) = rest.split_at_checked(path_len)?;
        self.payload = tail;
        self.remaining -= 1;
        // `DeleteFilesHeader::decode_paths` already validated every entry.
        str::from_utf8(path_bytes).ok()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for DeleteFilesPaths<'_> {}

impl DeleteFilesHeader {
    /// Validate a `DELETE_FILES` record payload without heap allocation and
    /// return a zero-copy path iterator.
    pub(super) fn decode_paths(data: &[u8]) -> Option<DeleteFilesPaths<'_>> {
        if data.len() < SIZE_OF_DELETE_FILES {
            return None;
        }
        // SAFETY: the preceding length check proves that `data` contains at
        // least one complete header. `read_unaligned` permits the byte slice's
        // alignment, and the header's integer fields accept every bit pattern.
        let header = unsafe { ptr::read_unaligned(data.as_ptr().cast::<Self>()) };
        if header.path_count == 0
            || header.path_count as usize > MAX_DELETE_FILES_PER_RECORD
            || header.payload_len as usize > MAX_DELETE_FILES_PAYLOAD_BYTES
        {
            return None;
        }
        let payload_end =
            SIZE_OF_DELETE_FILES.checked_add(header.payload_len as usize)?;
        if data.len() != payload_end {
            return None;
        }

        let payload = &data[SIZE_OF_DELETE_FILES..payload_end];
        let mut cursor = payload;
        for _ in 0..header.path_count {
            let (len_bytes, rest) = cursor.split_first_chunk::<4>()?;
            let path_len = u32::from_ne_bytes(*len_bytes) as usize;
            let (path_bytes, tail) = rest.split_at_checked(path_len)?;
            if path_bytes.is_empty()
                || path_bytes.contains(&0)
                || str::from_utf8(path_bytes).is_err()
            {
                return None;
            }
            cursor = tail;
        }
        cursor.is_empty().then_some(DeleteFilesPaths {
            payload,
            remaining: header.path_count,
        })
    }
}

/// WAL record header for WriteFile operation
///
/// Layout in WAL record:
/// - WriteFileHeader (this struct)
/// - path bytes (path_len bytes)
/// - file data (remaining bytes)
///
/// When offset is 0, the file will be created (along with any missing parent
/// directories). If the file already exists, it will be truncated.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct WriteFileHeader {
    /// Length of the file path (not including null terminator)
    pub path_len: u32,
    /// Explicit padding keeps the WAL header deterministic on all platforms.
    pub _padding: u32,
    /// Offset in the file to write at
    /// If offset is 0, the file (and parent directories) will be created
    pub offset: i64,
}

/// Size of WriteFileHeader in bytes
pub(super) const SIZE_OF_WRITE_FILE: usize = mem::size_of::<WriteFileHeader>();

/// Zero-copy view of a decoded `WRITE_FILE` WAL record payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DecodedWriteFile<'a> {
    pub path: &'a str,
    pub offset: i64,
    pub file_data: &'a [u8],
}

impl WriteFileHeader {
    pub(super) fn decode(data: &[u8]) -> Option<DecodedWriteFile<'_>> {
        if data.len() < SIZE_OF_WRITE_FILE {
            return None;
        }

        // SAFETY: the preceding length check proves that `data` contains at
        // least one complete header. `read_unaligned` permits the byte slice's
        // alignment, and the header consists entirely of integer fields for
        // which every bit pattern is valid.
        let header = unsafe { ptr::read_unaligned(data.as_ptr().cast::<Self>()) };

        let path_end = SIZE_OF_WRITE_FILE.checked_add(header.path_len as usize)?;
        let path_bytes = data.get(SIZE_OF_WRITE_FILE..path_end)?;
        let path = str::from_utf8(path_bytes).ok()?;

        Some(DecodedWriteFile {
            path,
            offset: header.offset,
            file_data: &data[path_end..],
        })
    }
}

// ============================================================================
// WAL Logging Functions
// ============================================================================

/// Log a directory deletion to WAL
///
/// Call this before deleting a directory and all its contents.
/// This is used for post-commit cleanup of local table directories. It must not
/// be called from pre-commit or abort cleanup paths: unlike PostgreSQL's native
/// relation storage, extension-owned Iceberg paths cannot be embedded in the
/// core transaction commit/abort WAL record, and `smgr` cannot be registered by
/// this AM. Logging the delete before the transaction outcome is known would let
/// standby WAL replay or archive recovery delete data for a transaction that later aborts.
pub(in crate::storage) fn log_delete_directory(path: &str) -> XLogRecPtr {
    DirectoryHeader::insert(path, IcebergWalOp::DeleteDirectory)
}

/// Log an in-place directory truncation for a disposable transaction-local
/// generation selected by PostgreSQL's nontransactional truncate path.
pub(in crate::storage) fn log_truncate_directory(path: &str) -> XLogRecPtr {
    DirectoryHeader::insert(path, IcebergWalOp::TruncateDirectory)
}

/// Count how many bounded `DELETE_FILES` WAL records `log_delete_files` will
/// emit for the representable subset of `paths`.
pub(in crate::storage) fn count_delete_files_wal_batches<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> u64 {
    let mut batches = 0_u64;
    let mut batch_paths = 0_usize;
    let mut payload_bytes = 0_usize;

    for path in paths {
        if !delete_file_fits_wal(path) {
            continue;
        }
        let encoded_len = mem::size_of::<u32>() + path.len();
        if batch_paths > 0
            && (batch_paths >= MAX_DELETE_FILES_PER_RECORD
                || payload_bytes + encoded_len > MAX_DELETE_FILES_PAYLOAD_BYTES)
        {
            batches += 1;
            batch_paths = 0;
            payload_bytes = 0;
        }
        batch_paths += 1;
        payload_bytes += encoded_len;
    }

    if batch_paths > 0 {
        batches += 1;
    }
    batches
}

/// Log bounded batches of local file deletions after commit.
///
/// Returns the last inserted LSN so the caller can flush every preceding batch
/// before deleting the files on the primary. Paths that cannot fit the record
/// representation are skipped with a warning; their primary cleanup may still
/// proceed and standby orphan maintenance can reclaim any replayed copy.
pub(in crate::storage) fn log_delete_files<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Option<XLogRecPtr> {
    let mut payload = Vec::with_capacity(MAX_DELETE_FILES_PAYLOAD_BYTES);
    let mut path_count = 0_u32;
    let mut last_lsn = None;

    for path in paths {
        if path.is_empty() || path.as_bytes().contains(&0) {
            diag::report_warning(
                "skipping DELETE_FILES WAL for empty path or path containing NUL",
            );
            continue;
        }
        let Ok(path_len) = u32::try_from(path.len()) else {
            diag::report_warning(format_args!(
                "skipping DELETE_FILES WAL for path longer than u32: {} bytes",
                path.len()
            ));
            continue;
        };
        let encoded_len = mem::size_of::<u32>() + path.len();
        if encoded_len > MAX_DELETE_FILES_PAYLOAD_BYTES {
            diag::report_warning(format_args!(
                "skipping DELETE_FILES WAL for path exceeding bounded payload: {} bytes",
                path.len()
            ));
            continue;
        }
        if path_count > 0
            && (path_count as usize >= MAX_DELETE_FILES_PER_RECORD
                || payload.len() + encoded_len > MAX_DELETE_FILES_PAYLOAD_BYTES)
        {
            last_lsn = Some(insert_delete_files_record(path_count, &payload));
            payload.clear();
            path_count = 0;
        }

        payload.extend_from_slice(&path_len.to_ne_bytes());
        payload.extend_from_slice(path.as_bytes());
        path_count += 1;
    }

    if path_count > 0 {
        last_lsn = Some(insert_delete_files_record(path_count, &payload));
    }
    last_lsn
}

pub(in crate::storage) fn delete_file_fits_wal(path: &str) -> bool {
    !path.is_empty()
        && !path.as_bytes().contains(&0)
        && u32::try_from(path.len()).is_ok()
        && mem::size_of::<u32>() + path.len() <= MAX_DELETE_FILES_PAYLOAD_BYTES
}

fn insert_delete_files_record(path_count: u32, payload: &[u8]) -> XLogRecPtr {
    let header = DeleteFilesHeader {
        path_count,
        payload_len: u32::try_from(payload.len())
            .expect("bounded DELETE_FILES payload must fit u32"),
    };
    let mut builder = WalRecordBuilder::begin();
    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(payload);
    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::DeleteFiles as u8)
}

/// Log a file write to WAL
///
/// Call this when writing data to a file. If offset is 0, the file will be
/// created (along with any missing parent directories), or truncated if it
/// already exists.
pub(in crate::storage) fn log_write_file(
    path: &str,
    offset: i64,
    data: &[u8],
) -> XLogRecPtr {
    let header = WriteFileHeader {
        path_len: path.len() as u32,
        _padding: 0,
        offset,
    };

    let mut builder = WalRecordBuilder::begin();

    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(path.as_bytes());

    if !data.is_empty() {
        builder.register_data(data);
    }

    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::WriteFile as u8)
}

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;

    fn header_bytes<T>(header: &T) -> &[u8] {
        // SAFETY: every caller passes one of this module's `#[repr(C)]` WAL
        // header structs. Those structs contain only initialized integer fields
        // and have no implicit padding (WriteFileHeader uses explicit padding),
        // so all `size_of::<T>()` bytes are initialized and readable while the
        // returned slice borrows `header`.
        unsafe {
            slice::from_raw_parts(
                (header as *const T).cast::<u8>(),
                mem::size_of::<T>(),
            )
        }
    }

    fn delete_directory_record(path: &[u8], path_len: u32) -> Vec<u8> {
        let header = DirectoryHeader { path_len };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(path);
        data
    }

    fn write_file_record(path: &[u8], offset: i64, payload: &[u8]) -> Vec<u8> {
        let header = WriteFileHeader {
            path_len: path.len() as u32,
            _padding: 0,
            offset,
        };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(path);
        data.extend_from_slice(payload);
        data
    }

    fn delete_files_record(paths: &[&[u8]]) -> Vec<u8> {
        let mut payload = Vec::new();
        for path in paths {
            payload.extend_from_slice(&(path.len() as u32).to_ne_bytes());
            payload.extend_from_slice(path);
        }
        let header = DeleteFilesHeader {
            path_count: paths.len() as u32,
            payload_len: payload.len() as u32,
        };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(&payload);
        data
    }

    #[test]
    fn wal_op_from_info_masks_postgres_flags() {
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::DeleteDirectory as u8 | 0x0f),
            Some(IcebergWalOp::DeleteDirectory)
        );
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::WriteFile as u8 | 0x0f),
            Some(IcebergWalOp::WriteFile)
        );
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::DeleteFiles as u8 | 0x0f),
            Some(IcebergWalOp::DeleteFiles)
        );
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::TruncateDirectory as u8 | 0x0f),
            Some(IcebergWalOp::TruncateDirectory)
        );
        assert_eq!(IcebergWalOp::from_info(0x40), None);
    }

    #[test]
    fn extracts_delete_directory_path() {
        let data = delete_directory_record(b"base/1/2_iceberg", 16);
        assert_eq!(
            DirectoryHeader::decode_path(&data),
            Some("base/1/2_iceberg")
        );
    }

    #[test]
    fn rejects_malformed_delete_directory_records() {
        assert_eq!(DirectoryHeader::decode_path(&[]), None);
        assert_eq!(
            DirectoryHeader::decode_path(&delete_directory_record(b"abc", 4)),
            None
        );
        assert_eq!(
            DirectoryHeader::decode_path(&delete_directory_record(b"", u32::MAX)),
            None
        );
        assert_eq!(
            DirectoryHeader::decode_path(&delete_directory_record(&[0xff], 1)),
            None
        );
    }

    #[test]
    fn extracts_delete_file_paths() {
        let data = delete_files_record(&[
            &b"base/1/data-a.parquet"[..],
            &b"base/1/delete-a.parquet"[..],
        ]);
        let decoded = DeleteFilesHeader::decode_paths(&data).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded.collect::<Vec<_>>(),
            vec!["base/1/data-a.parquet", "base/1/delete-a.parquet"],
        );
    }

    #[test]
    fn rejects_malformed_delete_files_records() {
        assert!(DeleteFilesHeader::decode_paths(&[]).is_none());

        let empty_header = DeleteFilesHeader {
            path_count: 0,
            payload_len: 0,
        };
        assert!(
            DeleteFilesHeader::decode_paths(header_bytes(&empty_header)).is_none()
        );

        let too_many_paths = DeleteFilesHeader {
            path_count: (MAX_DELETE_FILES_PER_RECORD + 1) as u32,
            payload_len: 0,
        };
        assert!(
            DeleteFilesHeader::decode_paths(header_bytes(&too_many_paths)).is_none()
        );

        let oversized_payload = DeleteFilesHeader {
            path_count: 1,
            payload_len: (MAX_DELETE_FILES_PAYLOAD_BYTES + 1) as u32,
        };
        assert!(
            DeleteFilesHeader::decode_paths(header_bytes(&oversized_payload))
                .is_none()
        );

        let mut truncated = delete_files_record(&[&b"abc"[..]]);
        truncated.pop();
        assert!(DeleteFilesHeader::decode_paths(&truncated).is_none());

        let mut trailing = delete_files_record(&[&b"abc"[..]]);
        trailing.push(0);
        assert!(DeleteFilesHeader::decode_paths(&trailing).is_none());

        assert!(
            DeleteFilesHeader::decode_paths(&delete_files_record(&[&b"a\0b"[..]]))
                .is_none()
        );
        assert!(
            DeleteFilesHeader::decode_paths(&delete_files_record(&[&[0xff_u8][..]]))
                .is_none()
        );
    }

    #[test]
    fn extracts_write_file_info() {
        let data = write_file_record(b"base/1/data.parquet", 128, b"payload");
        assert_eq!(
            WriteFileHeader::decode(&data),
            Some(DecodedWriteFile {
                path: "base/1/data.parquet",
                offset: 128,
                file_data: b"payload",
            })
        );
    }

    #[test]
    fn rejects_malformed_write_file_records() {
        assert_eq!(WriteFileHeader::decode(&[]), None);

        let mut short_path = write_file_record(b"abc", 0, b"");
        let header = WriteFileHeader {
            path_len: 4,
            _padding: 0,
            offset: 0,
        };
        short_path[..SIZE_OF_WRITE_FILE].copy_from_slice(header_bytes(&header));
        assert_eq!(WriteFileHeader::decode(&short_path), None);

        let long_path_header = WriteFileHeader {
            path_len: u32::MAX,
            _padding: 0,
            offset: 0,
        };
        let mut long_path = Vec::new();
        long_path.extend_from_slice(header_bytes(&long_path_header));
        assert_eq!(WriteFileHeader::decode(&long_path), None);

        assert_eq!(
            WriteFileHeader::decode(&write_file_record(&[0xff], 0, b"")),
            None
        );
    }
}
