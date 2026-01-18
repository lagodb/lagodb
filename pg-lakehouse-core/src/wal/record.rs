//! WAL Record types and utilities
//!
//! This module provides safe wrappers around PostgreSQL's WAL record structures.

use crate::diag;
use pgrx::pg_sys;
use std::marker::PhantomData;

/// WAL Log Sequence Number (LSN) - represents a position in the WAL stream
pub type XLogRecPtr = pg_sys::XLogRecPtr;

/// Safe wrapper around a WAL record during redo/replay
///
/// This struct provides read-only access to a WAL record's contents
/// during crash recovery or replication replay.
pub struct WalRecord<'a> {
    record: *mut pg_sys::XLogReaderState,
    _marker: PhantomData<&'a pg_sys::XLogReaderState>,
}

impl<'a> WalRecord<'a> {
    /// Create a new WalRecord wrapper
    ///
    /// # Safety
    /// The caller must ensure the pointer is valid for the lifetime 'a
    pub unsafe fn from_raw(record: *mut pg_sys::XLogReaderState) -> Self {
        Self {
            record,
            _marker: PhantomData,
        }
    }

    /// Get the raw pointer to the XLogReaderState
    ///
    /// This is useful when you need to call PostgreSQL C functions directly.
    pub fn as_ptr(&self) -> *mut pg_sys::XLogReaderState {
        self.record
    }

    /// Get the LSN (Log Sequence Number) of this record
    pub fn lsn(&self) -> XLogRecPtr {
        unsafe { (*self.record).ReadRecPtr }
    }

    /// Get the end LSN of this record
    pub fn end_lsn(&self) -> XLogRecPtr {
        unsafe { (*self.record).EndRecPtr }
    }

    /// Get the info flags from the record header
    ///
    /// The lower 4 bits typically contain the operation type,
    /// and the upper 4 bits contain flags like XLR_INFO_MASK.
    pub fn info(&self) -> u8 {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                0
            } else {
                (*decoded).header.xl_info
            }
        }
    }

    /// Get the operation type (high 4 bits of info)
    ///
    /// This filters out the internal PostgreSQL bits (XLR_INFO_MASK)
    /// to get the RMGR-specific operation code.
    pub fn op_code(&self) -> u8 {
        self.info() & !pg_sys::XLR_INFO_MASK as u8
    }

    /// Get the transaction ID that generated this record
    pub fn xid(&self) -> pg_sys::TransactionId {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                pg_sys::InvalidTransactionId
            } else {
                (*decoded).header.xl_xid
            }
        }
    }

    /// Get the total length of the record
    pub fn total_len(&self) -> u32 {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                0
            } else {
                (*decoded).header.xl_tot_len
            }
        }
    }

    /// Get the resource manager ID
    pub fn rmgr_id(&self) -> u8 {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                0
            } else {
                (*decoded).header.xl_rmid
            }
        }
    }

    /// Get the main data portion of the WAL record
    ///
    /// Returns None if there is no main data or the record is invalid.
    pub fn main_data(&self) -> Option<&[u8]> {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                return None;
            }
            let data_ptr = (*decoded).main_data;
            let len = (*decoded).main_data_len as usize;
            if data_ptr.is_null() || len == 0 {
                return None;
            }
            Some(std::slice::from_raw_parts(data_ptr as *const u8, len))
        }
    }

    /// Get the main data as a typed reference
    ///
    /// # Safety
    /// The caller must ensure that T matches the actual data layout
    /// stored in this WAL record.
    pub unsafe fn main_data_as<T>(&self) -> Option<&T> {
        let data = self.main_data()?;
        if data.len() < std::mem::size_of::<T>() {
            return None;
        }
        unsafe { Some(&*(data.as_ptr() as *const T)) }
    }

    /// Get the number of block references in this record
    pub fn max_block_id(&self) -> i8 {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                -1
            } else {
                (*decoded).max_block_id as i8
            }
        }
    }

    /// Check if a block reference exists at the given index
    pub fn has_block_ref(&self, block_id: u8) -> bool {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                return false;
            }

            if block_id as i8 > (*decoded).max_block_id as i8 {
                return false;
            }

            // Using pointer arithmetic because .blocks is a flexible array member
            let block_ptr = (*decoded).blocks.as_ptr().add(block_id as usize);
            (*block_ptr).in_use
        }
    }

    /// Check if a block reference has a full-page image associated with it
    pub fn has_block_image(&self, block_id: u8) -> bool {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                return false;
            }
            let block_ptr = (*decoded).blocks.as_ptr().add(block_id as usize);
            (*block_ptr).has_image
        }
    }

    /// Check if a block reference has incremental block data associated with it
    pub fn has_block_data(&self, block_id: u8) -> bool {
        unsafe {
            let decoded = (*self.record).record;
            if decoded.is_null() {
                return false;
            }
            let block_ptr = (*decoded).blocks.as_ptr().add(block_id as usize);
            (*block_ptr).has_data
        }
    }

    /// Get the block data for a given block reference
    ///
    /// Returns the raw block data bytes, or None if not available.
    pub fn block_data(&self, block_id: u8) -> Option<&[u8]> {
        unsafe {
            let mut len: pg_sys::Size = 0;
            let data = pg_sys::XLogRecGetBlockData(self.record, block_id, &mut len);
            if data.is_null() || len == 0 {
                None
            } else {
                Some(std::slice::from_raw_parts(data as *const u8, len))
            }
        }
    }

    /// Get the RelFileLocator for a block reference
    ///
    /// This identifies which relation/table the block belongs to.
    pub fn block_rel_file_locator(
        &self,
        block_id: u8,
    ) -> Option<pg_sys::RelFileLocator> {
        unsafe {
            let mut locator = std::mem::zeroed::<pg_sys::RelFileLocator>();
            if pg_sys::XLogRecGetBlockTagExtended(
                self.record,
                block_id,
                &mut locator,         // 3: rlocator
                std::ptr::null_mut(), // 4: forknum
                std::ptr::null_mut(), // 5: blknum
                std::ptr::null_mut(), // 6: prefetch_buffer
            ) {
                Some(locator)
            } else {
                None
            }
        }
    }

    /// Get the block number for a block reference
    pub fn block_number(&self, block_id: u8) -> Option<pg_sys::BlockNumber> {
        unsafe {
            let mut blkno: pg_sys::BlockNumber = 0;
            if pg_sys::XLogRecGetBlockTagExtended(
                self.record,
                block_id,
                std::ptr::null_mut(), // 3: rlocator
                std::ptr::null_mut(), // 4: forknum
                &mut blkno,           // 5: blknum (FIXED POSITION)
                std::ptr::null_mut(), // 6: prefetch_buffer
            ) {
                Some(blkno)
            } else {
                None
            }
        }
    }
}

/// Builder for creating WAL records
///
/// This provides a safe interface for constructing WAL records
/// before inserting them into the WAL stream.
pub struct WalRecordBuilder {
    started: bool,
}

impl WalRecordBuilder {
    /// Begin a new WAL record insertion
    ///
    /// # Panics
    /// Panics if called while another record is being built
    pub fn begin() -> Self {
        unsafe {
            pg_sys::XLogBeginInsert();
        }
        Self { started: true }
    }

    /// Register main data for the WAL record
    ///
    /// This is the primary payload of the record.
    pub fn register_data(&mut self, data: &[u8]) -> &mut Self {
        unsafe {
            // XLogRegisterData expects mutable pointer even though it doesn't modify
            pg_sys::XLogRegisterData(data.as_ptr() as *mut i8, data.len() as u32);
        }
        self
    }

    /// Register a typed value as main data
    ///
    /// # Safety
    /// The type T must be safe to serialize as raw bytes (repr(C), no padding issues).
    pub unsafe fn register_data_as<T>(&mut self, value: &T) -> &mut Self {
        unsafe {
            let data = std::slice::from_raw_parts(
                value as *const T as *const u8,
                std::mem::size_of::<T>(),
            );
            self.register_data(data)
        }
    }

    /// Register a buffer/block reference
    ///
    /// # Arguments
    /// * `block_id` - Block reference ID (0-based)
    /// * `buffer` - The PostgreSQL buffer
    /// * `flags` - Registration flags (REGBUF_STANDARD, REGBUF_FORCE_IMAGE, etc.)
    pub fn register_buffer(
        &mut self,
        block_id: u8,
        buffer: pg_sys::Buffer,
        flags: u8,
    ) -> &mut Self {
        unsafe {
            pg_sys::XLogRegisterBuffer(block_id, buffer, flags);
        }
        self
    }

    /// Register block-specific data
    ///
    /// This data is associated with a specific block reference.
    pub fn register_buf_data(&mut self, block_id: u8, data: &[u8]) -> &mut Self {
        unsafe {
            pg_sys::XLogRegisterBufData(
                block_id,
                data.as_ptr() as *mut i8,
                data.len() as u32,
            );
        }
        self
    }

    /// Insert the WAL record and return its LSN
    ///
    /// # Arguments
    /// * `rmgr_id` - The resource manager ID
    /// * `info` - The info byte (operation type + flags)
    ///
    /// This consumes the builder and commits the record to WAL.
    pub fn insert(mut self, rmgr_id: u8, info: u8) -> XLogRecPtr {
        self.started = false;
        unsafe { pg_sys::XLogInsert(rmgr_id, info) }
    }

    /// Set this record as needing to be flushed before commit
    ///
    /// Typically used for operations that must be durable before proceeding.
    pub fn set_record_flags(&mut self, flags: u8) -> &mut Self {
        unsafe {
            pg_sys::XLogSetRecordFlags(flags);
        }
        self
    }
}

impl Drop for WalRecordBuilder {
    fn drop(&mut self) {
        if self.started {
            // If the builder is dropped without inserting, we need to cancel
            // This is a safety measure to prevent WAL corruption
            // Note: PostgreSQL doesn't have an explicit cancel, but the next
            // XLogBeginInsert will reset state
            diag::report_warning("WalRecordBuilder dropped without calling insert()");
        }
    }
}

/// WAL record flags for XLogSetRecordFlags
pub mod record_flags {
    use pgrx::pg_sys;

    /// Force a full-page image of the buffer to be written
    pub const REGBUF_FORCE_IMAGE: u8 = pg_sys::REGBUF_FORCE_IMAGE as u8;

    /// Register a standard buffer (heap or index)
    pub const REGBUF_STANDARD: u8 = pg_sys::REGBUF_STANDARD as u8;

    /// Don't log this record during recovery
    pub const REGBUF_NO_IMAGE: u8 = pg_sys::REGBUF_NO_IMAGE as u8;

    /// Mark this as a hint bit-only change
    pub const REGBUF_NO_CHANGE: u8 = 0x20;
}
