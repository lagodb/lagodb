//! Neutral copyObject-safe positional list codec for FDW plan data.

use core::ffi::{CStr, c_void};
use core::marker::PhantomData;
use core::ptr;
use std::ffi::CString;

use pgrx::pg_sys;

#[derive(Debug, thiserror::Error)]
pub enum PrivateCodecError {
    #[error("FDW private-data top-level list is NULL")]
    NullList,
    #[error("FDW private-data list {field} has a negative length: {length}")]
    NegativeListLength { field: usize, length: i32 },
    #[error("FDW private-data cell {field} is NULL")]
    NullCell { field: usize },
    #[error("FDW private-data read past end: position {position}, length {len}")]
    ReadPastEnd { position: usize, len: usize },
    #[error("FDW private-data has trailing cells: read {read}, length {len}")]
    UnexpectedTrailingCells { read: usize, len: usize },
    #[error(
        "FDW private-data cell {field} has node tag {found:?}, expected {expected:?}"
    )]
    WrongNodeTag {
        field: usize,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
    #[error("FDW private-data string cell {field} has a NULL value")]
    NullString { field: usize },
    #[error("FDW private-data string cell {field} is not valid UTF-8")]
    InvalidUtf8 { field: usize },
    #[error("FDW private-data count {value} exceeds PostgreSQL Integer range")]
    CountTooLarge { value: usize },
    #[error("FDW private-data count at cell {field} is negative: {value}")]
    NegativeCount { field: usize, value: i32 },
    #[error(
        "FDW private-data string at position {position} contains an interior NUL"
    )]
    InteriorNul { position: usize },
    #[error("FDW private-data float cell {field} is malformed")]
    MalformedFloat { field: usize },
    #[error("nested FDW private-data encoding failed: {message}")]
    NestedEncode { message: String },
}

/// Writer for a PostgreSQL list whose cells contain `copyObject`-safe nodes.
pub struct ForeignPrivateWriter {
    list: *mut pg_sys::List,
    error: Option<PrivateCodecError>,
    position: usize,
}

impl ForeignPrivateWriter {
    #[inline]
    pub const fn new() -> Self {
        Self {
            list: ptr::null_mut(),
            error: None,
            position: 0,
        }
    }

    /// # Safety
    ///
    /// `self.list` must be a PostgreSQL list in the current planner memory
    /// context, and `node` must be a live copyObject-safe node or PostgreSQL NIL.
    unsafe fn push_node(&mut self, node: *mut pg_sys::Node) {
        self.list = unsafe { pg_sys::lappend(self.list, node.cast::<c_void>()) };
        self.position += 1;
    }

    pub fn append_i32(&mut self, value: i32) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            self.push_node(pg_sys::makeInteger(value).cast());
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
            Err(_) => {
                self.error = Some(PrivateCodecError::CountTooLarge { value });
            }
        }
        self
    }

    pub fn append_bool(&mut self, value: bool) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            self.push_node(pg_sys::makeBoolean(value).cast());
        }
        self
    }

    pub fn append_i64(&mut self, value: i64) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let decimal = value.to_string();
        let Ok(decimal) = CString::new(decimal) else {
            self.error = Some(PrivateCodecError::InteriorNul {
                position: self.position,
            });
            return self;
        };
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
        let Ok(value) = CString::new(value) else {
            self.error = Some(PrivateCodecError::InteriorNul {
                position: self.position,
            });
            return self;
        };
        self.append_cstr(&value)
    }

    pub fn append_cstr(&mut self, value: &CStr) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            let value = pg_sys::pstrdup(value.as_ptr());
            self.push_node(pg_sys::makeString(value).cast());
        }
        self
    }

    /// Append a nested `T_List` cell.  A NULL list is the PostgreSQL NIL form.
    ///
    /// # Safety
    ///
    /// `list` must be a PostgreSQL-owned nested list in the current planner
    /// memory context, or NULL to represent NIL.
    pub(crate) unsafe fn append_list(&mut self, list: *mut pg_sys::List) {
        if self.error.is_some() {
            return;
        }
        unsafe {
            self.push_node(list.cast());
        }
    }

    pub fn append_nested(
        &mut self,
        build: impl FnOnce(&mut ForeignPrivateWriter),
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let mut child = ForeignPrivateWriter::new();
        build(&mut child);
        match child.finish() {
            Ok(list) => unsafe { self.append_list(list) },
            Err(error) => {
                self.error = Some(PrivateCodecError::NestedEncode {
                    message: error.to_string(),
                });
            }
        }
        self
    }

    pub fn finish(self) -> Result<*mut pg_sys::List, PrivateCodecError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.list),
        }
    }
}

impl Default for ForeignPrivateWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for a PostgreSQL list produced by [`ForeignPrivateWriter`].
pub struct ForeignPrivateReader<'a> {
    list: *mut pg_sys::List,
    position: usize,
    length: usize,
    _marker: PhantomData<&'a pg_sys::List>,
}

impl<'a> ForeignPrivateReader<'a> {
    /// # Safety
    ///
    /// `list` is NULL or a live PostgreSQL `T_List` allocated in a plan memory
    /// context that remains live for `'a`.
    pub unsafe fn from_list(list: *mut pg_sys::List) -> Self {
        let length = if list.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(list) as usize }
        };
        Self {
            list,
            position: 0,
            length,
            _marker: PhantomData,
        }
    }

    /// # Safety
    ///
    /// If non-null, `list` must point to a live PostgreSQL node whose memory is
    /// valid for the duration of this reader construction.
    pub(crate) unsafe fn checked_from_list(
        list: *mut pg_sys::List,
        field: usize,
    ) -> Result<Self, PrivateCodecError> {
        if list.is_null() {
            return Err(PrivateCodecError::NullList);
        }
        let found = unsafe { (*list).type_ };
        if found != pg_sys::NodeTag::T_List {
            return Err(PrivateCodecError::WrongNodeTag {
                field,
                expected: pg_sys::NodeTag::T_List,
                found,
            });
        }
        let length = unsafe { (*list).length };
        if length < 0 {
            return Err(PrivateCodecError::NegativeListLength { field, length });
        }
        Ok(Self {
            list,
            position: 0,
            length: length as usize,
            _marker: PhantomData,
        })
    }

    /// # Safety
    ///
    /// The reader must have been constructed from a live PostgreSQL list that
    /// remains valid while cells are read.
    unsafe fn read_cell(&mut self) -> Result<*mut pg_sys::Node, PrivateCodecError> {
        if self.position >= self.length {
            return Err(PrivateCodecError::ReadPastEnd {
                position: self.position,
                len: self.length,
            });
        }
        let cell = unsafe { pg_sys::list_nth(self.list, self.position as i32) };
        let field = self.position;
        self.position += 1;
        if cell.is_null() {
            Err(PrivateCodecError::NullCell { field })
        } else {
            Ok(cell.cast())
        }
    }

    /// # Safety
    ///
    /// `node` must be a non-null live PostgreSQL node from this reader.
    unsafe fn expect(
        &self,
        node: *mut pg_sys::Node,
        expected: pg_sys::NodeTag,
        field: usize,
    ) -> Result<(), PrivateCodecError> {
        let found = unsafe { (*node).type_ };
        if found != expected {
            return Err(PrivateCodecError::WrongNodeTag {
                field,
                expected,
                found,
            });
        }
        Ok(())
    }

    pub fn read_i32(&mut self) -> Result<i32, PrivateCodecError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { self.expect(node, pg_sys::NodeTag::T_Integer, field)? };
        Ok(unsafe { (*node.cast::<pg_sys::Integer>()).ival })
    }

    pub fn read_oid(&mut self) -> Result<pg_sys::Oid, PrivateCodecError> {
        Ok((self.read_i32()? as u32).into())
    }

    pub fn read_count(&mut self) -> Result<usize, PrivateCodecError> {
        let field = self.position;
        let value = self.read_i32()?;
        usize::try_from(value)
            .map_err(|_| PrivateCodecError::NegativeCount { field, value })
    }

    pub fn read_bool(&mut self) -> Result<bool, PrivateCodecError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { self.expect(node, pg_sys::NodeTag::T_Boolean, field)? };
        Ok(unsafe { (*node.cast::<pg_sys::Boolean>()).boolval })
    }

    pub fn read_i64(&mut self) -> Result<i64, PrivateCodecError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { self.expect(node, pg_sys::NodeTag::T_Float, field)? };
        let value = unsafe { (*node.cast::<pg_sys::Float>()).fval };
        if value.is_null() {
            return Err(PrivateCodecError::MalformedFloat { field });
        }
        let value = unsafe { CStr::from_ptr(value) };
        value
            .to_str()
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or(PrivateCodecError::MalformedFloat { field })
    }

    pub fn read_cstr(&mut self) -> Result<&'a CStr, PrivateCodecError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { self.expect(node, pg_sys::NodeTag::T_String, field)? };
        let value = unsafe { (*node.cast::<pg_sys::String>()).sval };
        if value.is_null() {
            return Err(PrivateCodecError::NullString { field });
        }
        Ok(unsafe { CStr::from_ptr(value) })
    }

    pub fn read_str(&mut self) -> Result<String, PrivateCodecError> {
        let field = self.position;
        let value = self.read_cstr()?;
        value
            .to_str()
            .map(str::to_owned)
            .map_err(|_| PrivateCodecError::InvalidUtf8 { field })
    }

    /// Read a nested `T_List`; a NULL cell maps to an empty reader.
    pub fn read_nested(
        &mut self,
    ) -> Result<ForeignPrivateReader<'a>, PrivateCodecError> {
        if self.position >= self.length {
            return Err(PrivateCodecError::ReadPastEnd {
                position: self.position,
                len: self.length,
            });
        }
        let field = self.position;
        let cell = unsafe { pg_sys::list_nth(self.list, self.position as i32) };
        self.position += 1;
        if cell.is_null() {
            return Ok(unsafe { Self::from_list(ptr::null_mut()) });
        }
        let node = cell.cast::<pg_sys::Node>();
        unsafe { self.expect(node, pg_sys::NodeTag::T_List, field)? };
        Ok(unsafe { Self::checked_from_list(node.cast::<pg_sys::List>(), field)? })
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.length.saturating_sub(self.position)
    }

    pub fn finish(self) -> Result<(), PrivateCodecError> {
        if self.position == self.length {
            Ok(())
        } else {
            Err(PrivateCodecError::UnexpectedTrailingCells {
                read: self.position,
                len: self.length,
            })
        }
    }
}
