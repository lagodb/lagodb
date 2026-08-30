use super::tuple::ItemPointer;
use pgrx::pg_sys;

/// Safe wrapper for PostgreSQL TM_IndexDeleteOp.
#[derive(Debug)]
pub struct TMIndexDeleteOpHandle<'a> {
    inner: &'a mut pg_sys::TM_IndexDeleteOp,
}

impl<'a> TMIndexDeleteOpHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null, uniquely borrowed, and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::TM_IndexDeleteOp) -> Self {
        let ptr = std::ptr::NonNull::new(ptr)
            .expect("PostgreSQL passed a null TM_IndexDeleteOp pointer");
        unsafe {
            Self {
                inner: ptr.as_ptr().as_mut().unwrap(),
            }
        }
    }

    #[inline]
    pub fn as_raw(&mut self) -> *mut pg_sys::TM_IndexDeleteOp {
        self.inner as *mut pg_sys::TM_IndexDeleteOp
    }
}

/// Safe wrapper for PostgreSQL TM_FailureData.
#[derive(Debug, Clone, Copy, Default)]
#[allow(non_camel_case_types)]
pub struct TM_FailureData {
    pub ctid: ItemPointer,
    pub xmax: pg_sys::TransactionId,
    pub cmax: pg_sys::CommandId,
    pub traversed: bool,
}

impl TM_FailureData {
    /// # Safety
    ///
    /// If `ptr` is non-null, it must point to writable PostgreSQL
    /// `TM_FailureData` storage for the duration of this call.
    #[inline]
    pub unsafe fn write_to_ptr(&self, ptr: *mut pg_sys::TM_FailureData) {
        unsafe {
            if !ptr.is_null() {
                (*ptr).ctid = self.ctid.to_pg_sys();
                (*ptr).xmax = self.xmax;
                (*ptr).cmax = self.cmax;
                (*ptr).traversed = self.traversed;
            }
        }
    }
}
