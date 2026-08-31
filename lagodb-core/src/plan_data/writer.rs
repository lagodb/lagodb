//! Writer for `copyObject`-safe PostgreSQL plan-data lists.

use core::ffi::{CStr, c_void};
use core::ptr;
use std::ffi::CString;

use pgrx::pg_sys;

use super::PlanDataError;

pub struct PlanDataWriter {
    list: *mut pg_sys::List,
    error: Option<PlanDataError>,
    position: usize,
}

impl PlanDataWriter {
    #[inline]
    const fn new() -> Self {
        Self {
            list: ptr::null_mut(),
            error: None,
            position: 0,
        }
    }

    /// Encode one complete list payload.
    ///
    /// The field encoder and writer finalization form one operation, so plan
    /// sinks cannot publish a partially encoded provider payload or forget to
    /// propagate a deferred writer error.
    pub fn encode_list<E>(
        encode: impl FnOnce(&mut Self) -> Result<(), E>,
    ) -> Result<*mut pg_sys::List, E>
    where
        E: From<PlanDataError>,
    {
        let mut writer = Self::new();
        encode(&mut writer)?;
        let list = writer.into_list()?;
        Ok(list)
    }

    /// # Safety
    ///
    /// `node` must be PostgreSQL NIL or a live, `copyObject`-safe node in the
    /// current planner memory context.
    unsafe fn push_node(&mut self, node: *mut pg_sys::Node) {
        self.list = unsafe { pg_sys::lappend(self.list, node.cast::<c_void>()) };
        self.position += 1;
    }

    pub fn append_i32(&mut self, value: i32) -> &mut Self {
        if self.error.is_none() {
            unsafe { self.push_node(pg_sys::makeInteger(value).cast()) };
        }
        self
    }

    pub fn append_oid(&mut self, value: pg_sys::Oid) -> &mut Self {
        self.append_i32(u32::from(value) as i32)
    }

    pub fn append_count(&mut self, value: usize) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match i32::try_from(value) {
            Ok(value) => {
                self.append_i32(value);
            }
            Err(_) => self.error = Some(PlanDataError::CountTooLarge { value }),
        }
        self
    }

    pub fn append_bool(&mut self, value: bool) -> &mut Self {
        if self.error.is_none() {
            unsafe { self.push_node(pg_sys::makeBoolean(value).cast()) };
        }
        self
    }

    pub fn append_i64(&mut self, value: i64) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let decimal = CString::new(value.to_string())
            .expect("the decimal representation of an i64 contains no NUL");
        unsafe {
            let value = pg_sys::pstrdup(decimal.as_ptr());
            self.push_node(pg_sys::makeFloat(value).cast());
        }
        self
    }

    pub fn append_str(&mut self, value: &str) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match CString::new(value) {
            Ok(value) => self.append_cstr(&value),
            Err(_) => {
                self.error = Some(PlanDataError::InteriorNul {
                    position: self.position,
                });
                self
            }
        }
    }

    pub fn append_cstr(&mut self, value: &CStr) -> &mut Self {
        if self.error.is_none() {
            unsafe {
                let value = pg_sys::pstrdup(value.as_ptr());
                self.push_node(pg_sys::makeString(value).cast());
            }
        }
        self
    }

    /// Append a nested `T_List`; PostgreSQL NIL is represented by NULL.
    ///
    /// # Safety
    ///
    /// `list` must be NIL or a live PostgreSQL-owned list in the current
    /// planner memory context.
    pub(crate) unsafe fn append_list(&mut self, list: *mut pg_sys::List) {
        if self.error.is_none() {
            unsafe { self.push_node(list.cast()) };
        }
    }

    /// Append an already encoded nested plan-data frame.
    ///
    /// This is the composition boundary for independently owned codecs, such
    /// as the engine envelope and an opaque provider source plan.
    ///
    /// # Safety
    ///
    /// `list` must be NIL or a live, `copyObject`-safe PostgreSQL `T_List` in
    /// the current planner memory context.
    pub unsafe fn append_encoded_list(
        &mut self,
        list: *mut pg_sys::List,
    ) -> &mut Self {
        if self.error.is_none() {
            unsafe { self.append_list(list) };
        }
        self
    }

    pub fn append_nested(
        &mut self,
        build: impl FnOnce(&mut PlanDataWriter),
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let mut child = Self::new();
        build(&mut child);
        match child.into_list() {
            Ok(list) => unsafe { self.append_list(list) },
            Err(error) => self.error = Some(error),
        }
        self
    }

    fn into_list(self) -> Result<*mut pg_sys::List, PlanDataError> {
        self.error.map_or(Ok(self.list), Err)
    }
}
