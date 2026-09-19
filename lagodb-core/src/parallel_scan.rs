//! Native PostgreSQL parallel-scan coordination.
//!
//! The framework owns only the DSM header, immutable provider payload, and an
//! atomic group counter. Providers retain responsibility for payload semantics
//! and for opening a cursor for each claimed group.

use core::ffi::c_void;
use core::mem::{align_of, size_of};
use core::ptr::NonNull;
use core::slice;

use pgrx::pg_sys;
use thiserror::Error;

use crate::diag::SqlStateError;

const MAGIC: u32 = 0x4c_47_50_53;
const VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum ParallelScanError {
    #[error("parallel scan work count must be greater than zero")]
    EmptyWork,
    #[error("parallel scan payload must not be empty")]
    EmptyPayload,
    #[error("parallel scan DSM size overflows PostgreSQL Size")]
    SizeOverflow,
    #[error("parallel scan DSM coordinate is null")]
    NullCoordinate,
    #[error("parallel scan DSM header is invalid")]
    InvalidHeader,
    #[error("parallel scan payload was not prepared before DSM estimation")]
    NotPrepared,
}

impl SqlStateError for ParallelScanError {
    fn sql_error_code(&self) -> pgrx::prelude::PgSqlErrorCode {
        pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    }
}

/// Immutable provider bytes and the number of independently claimable groups.
pub struct PreparedParallelScan {
    bytes: Box<[u8]>,
    work_count: u32,
}

impl PreparedParallelScan {
    pub fn new(bytes: Box<[u8]>, work_count: u32) -> Result<Self, ParallelScanError> {
        if bytes.is_empty() {
            return Err(ParallelScanError::EmptyPayload);
        }
        if work_count == 0 {
            return Err(ParallelScanError::EmptyWork);
        }
        Ok(Self { bytes, work_count })
    }
}

#[repr(C)]
struct SharedHeader {
    magic: u32,
    version: u16,
    reserved: u16,
    work_count: u32,
    payload_len: usize,
    next_work: pg_sys::pg_atomic_uint32,
}

/// Backend-local owner for one provider's native parallel scan attachment.
#[derive(Default)]
pub struct ParallelScanCoordinator {
    prepared: Option<PreparedParallelScan>,
    shared: Option<NonNull<SharedHeader>>,
}

impl ParallelScanCoordinator {
    pub fn prepare(&mut self, prepared: PreparedParallelScan) {
        self.prepared = Some(prepared);
        self.shared = None;
    }

    pub fn estimate(&self) -> Result<pg_sys::Size, ParallelScanError> {
        let prepared = self
            .prepared
            .as_ref()
            .ok_or(ParallelScanError::NotPrepared)?;
        payload_offset()
            .checked_add(prepared.bytes.len())
            .ok_or(ParallelScanError::SizeOverflow)
    }

    /// Populate the leader's provider-specific coordinate and attach locally.
    ///
    /// # Safety
    ///
    /// `coordinate` must address at least [`Self::estimate`] bytes of DSM.
    pub unsafe fn initialize(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ParallelScanError> {
        let shared = NonNull::new(coordinate.cast::<SharedHeader>())
            .ok_or(ParallelScanError::NullCoordinate)?;
        let prepared = self.prepared.take().ok_or(ParallelScanError::NotPrepared)?;
        let header = shared.as_ptr();
        unsafe {
            header.write(SharedHeader {
                magic: MAGIC,
                version: VERSION,
                reserved: 0,
                work_count: prepared.work_count,
                payload_len: prepared.bytes.len(),
                next_work: pg_sys::pg_atomic_uint32::default(),
            });
            pg_sys::pg_atomic_init_u32(
                core::ptr::addr_of_mut!((*header).next_work),
                0,
            );
            core::ptr::copy_nonoverlapping(
                prepared.bytes.as_ptr(),
                coordinate.cast::<u8>().add(payload_offset()),
                prepared.bytes.len(),
            );
        }
        self.shared = Some(shared);
        Ok(())
    }

    /// Attach this backend to a coordinate populated by the leader.
    ///
    /// # Safety
    ///
    /// `coordinate` must remain live DSM storage until this coordinator is
    /// detached or dropped.
    pub unsafe fn attach(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ParallelScanError> {
        let shared = NonNull::new(coordinate.cast::<SharedHeader>())
            .ok_or(ParallelScanError::NullCoordinate)?;
        let header = unsafe { shared.as_ref() };
        if header.magic != MAGIC
            || header.version != VERSION
            || header.reserved != 0
            || header.work_count == 0
            || header.payload_len == 0
        {
            return Err(ParallelScanError::InvalidHeader);
        }
        payload_offset()
            .checked_add(header.payload_len)
            .ok_or(ParallelScanError::InvalidHeader)?;
        self.prepared = None;
        self.shared = Some(shared);
        Ok(())
    }

    pub fn payload(&self) -> Result<&[u8], ParallelScanError> {
        let shared = self.shared.ok_or(ParallelScanError::InvalidHeader)?;
        let header = unsafe { shared.as_ref() };
        let data = unsafe { shared.as_ptr().cast::<u8>().add(payload_offset()) };
        Ok(unsafe { slice::from_raw_parts(data, header.payload_len) })
    }

    /// Claim one group. The atomic operation runs once per group, not per row.
    pub fn claim(&self) -> Result<Option<u32>, ParallelScanError> {
        let shared = self.shared.ok_or(ParallelScanError::InvalidHeader)?;
        let header = shared.as_ptr();
        let work_id = unsafe {
            pg_sys::pg_atomic_fetch_add_u32(
                core::ptr::addr_of_mut!((*header).next_work),
                1,
            )
        };
        Ok((work_id < unsafe { (*header).work_count }).then_some(work_id))
    }

    /// Reset only shared claim state; the immutable payload remains in place.
    pub fn reinitialize(&self) -> Result<(), ParallelScanError> {
        let shared = self.shared.ok_or(ParallelScanError::InvalidHeader)?;
        unsafe {
            pg_sys::pg_atomic_write_u32(
                core::ptr::addr_of_mut!((*shared.as_ptr()).next_work),
                0,
            );
        }
        Ok(())
    }

    pub fn detach(&mut self) {
        self.shared = None;
        self.prepared = None;
    }
}

const fn payload_offset() -> usize {
    let alignment = align_of::<SharedHeader>();
    (size_of::<SharedHeader>() + alignment - 1) & !(alignment - 1)
}
