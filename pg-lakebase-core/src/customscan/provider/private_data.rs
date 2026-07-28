//! Typed `copyObject`-safe plan-data codec for the `provider_metadata` cell in
//! Provider-owned `custom_private` payload SPI. Providers use `append_*` /
//! `read_*` only; PG list/node FFI stays in Core. Sticky writer errors surface
//! at [`PrivateDataWriter::finish`]; Core raises `ereport` at the trampoline.

use std::ffi::{CStr, CString};
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;

/// Internal errors produced by the provider payload codec.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PrivateDataError {
    #[error("custom_private cell {field} is NULL but a Node* was expected")]
    NullCell { field: i32 },
    #[error(
        "custom_private cell {field} has wrong NodeTag: found {found:?}, expected {expected:?}"
    )]
    WrongNodeTag {
        field: i32,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
    #[error("custom_private cell {field} encodes negative count {value}")]
    NegativeCount { field: i32, value: i32 },
    #[error("custom_private cannot encode count {value}: exceeds i32::MAX")]
    CountTooLargeToEncode { value: usize },
    #[error(
        "custom_private read past end of payload: position {position}, len {len}"
    )]
    ReadPastEnd { position: usize, len: usize },
    #[error(
        "custom_private payload has unexpected trailing cells: read {read}, len {len}"
    )]
    UnexpectedTrailingCells { read: usize, len: usize },
    #[error(
        "custom_private cannot encode string at position {position}: contains interior NUL byte"
    )]
    StringContainsInteriorNul { position: usize },
    #[error("custom_private cell at position {position} holds a malformed i64 value")]
    MalformedI64Cell { position: usize },
    #[error(
        "custom_private cell at position {position} holds a malformed string value"
    )]
    MalformedStringCell { position: usize },
}

/// Write side: accumulates a `copyObject`-safe `List*` with sticky encode errors.
pub struct PrivateDataWriter {
    list: *mut pg_sys::List,
    error: Option<PrivateDataError>,
    position: usize,
}

impl PrivateDataWriter {
    /// Fresh writer over an empty payload.
    pub fn new() -> Self {
        PrivateDataWriter {
            list: ptr::null_mut(),
            error: None,
            position: 0,
        }
    }

    /// # Safety
    ///
    /// `node` is NULL or a valid `copyObject`-safe node in the current context.
    unsafe fn push_node(&mut self, node: *mut pg_sys::Node) {
        self.list = unsafe { pg_sys::lappend(self.list, node.cast()) };
        self.position += 1;
    }

    /// Append `Oid` as `T_Integer` (`to_u32() as i32` round-trip).
    pub fn append_oid(&mut self, value: pg_sys::Oid) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            let node = pg_sys::makeInteger(value.to_u32() as i32);
            self.push_node(node.cast());
        }
        self
    }

    pub fn append_i32(&mut self, value: i32) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            let node = pg_sys::makeInteger(value);
            self.push_node(node.cast());
        }
        self
    }

    /// Append count via [`usize_to_int`] (no truncation past `i32::MAX`).
    pub fn append_count(&mut self, value: usize) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match usize_to_int(value) {
            Ok(ival) => unsafe {
                let node = pg_sys::makeInteger(ival);
                self.push_node(node.cast());
            },
            Err(err) => self.error = Some(err),
        }
        self
    }

    /// Append `i64` as `T_Float` (decimal string in `fval`, PG convention).
    pub fn append_i64(&mut self, value: i64) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let decimal = value.to_string();
        let c_decimal = CString::new(decimal)
            .expect("decimal form of an i64 is NUL-free; conversion cannot fail");
        unsafe {
            let fval = pg_sys::pstrdup(c_decimal.as_ptr());
            let node = pg_sys::makeFloat(fval);
            self.push_node(node.cast());
        }
        self
    }

    pub fn append_bool(&mut self, value: bool) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        unsafe {
            let node = pg_sys::makeBoolean(value);
            self.push_node(node.cast());
        }
        self
    }

    /// Append UTF-8 as `T_String`; interior NUL records sticky error.
    pub fn append_str(&mut self, value: &str) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        match CString::new(value) {
            Ok(c_value) => unsafe {
                let sval = pg_sys::pstrdup(c_value.as_ptr());
                let node = pg_sys::makeString(sval);
                self.push_node(node.cast());
            },
            Err(_) => {
                self.error = Some(PrivateDataError::StringContainsInteriorNul {
                    position: self.position,
                });
            }
        }
        self
    }

    /// Nested sub-payload as one `T_List` cell; empty child encodes as NIL.
    pub fn append_nested(
        &mut self,
        build: impl FnOnce(&mut PrivateDataWriter),
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let mut child = PrivateDataWriter::new();
        build(&mut child);
        if let Some(err) = child.error.take() {
            self.error = Some(err);
            return self;
        }
        unsafe {
            self.push_node(child.list.cast());
        }
        self
    }

    /// Finish encoding; empty payload is NIL (NULL).
    pub fn finish(self) -> Result<*mut pg_sys::List, CustomScanError> {
        match self.error {
            Some(err) => Err(CustomScanError::private_codec(err)),
            None => Ok(self.list),
        }
    }
}

impl Default for PrivateDataWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Read side: forward cursor with node-tag validation.
pub struct PrivateDataReader<'a> {
    list: *mut pg_sys::List,
    pos: usize,
    len: usize,
    _marker: std::marker::PhantomData<&'a pg_sys::List>,
}

impl<'a> PrivateDataReader<'a> {
    /// # Safety
    ///
    /// `list` is NULL or a valid `*mut pg_sys::List` for `'a`.
    pub unsafe fn from_list(list: *mut pg_sys::List) -> Self {
        let len = if list.is_null() {
            0
        } else {
            unsafe { (*list).length as usize }
        };
        PrivateDataReader {
            list,
            pos: 0,
            len,
            _marker: std::marker::PhantomData,
        }
    }

    unsafe fn read_cell(&mut self) -> Result<*mut pg_sys::Node, CustomScanError> {
        if self.pos >= self.len {
            return Err(CustomScanError::private_codec(
                PrivateDataError::ReadPastEnd {
                    position: self.pos,
                    len: self.len,
                },
            ));
        }
        let cell = unsafe { pg_sys::list_nth(self.list, self.pos as i32) };
        self.pos += 1;
        if cell.is_null() {
            Err(CustomScanError::private_codec(PrivateDataError::NullCell {
                field: (self.pos - 1) as i32,
            }))
        } else {
            Ok(cell.cast())
        }
    }

    unsafe fn expect_node_tag(
        &self,
        node: *mut pg_sys::Node,
        expected: pg_sys::NodeTag,
        field: i32,
    ) -> Result<(), CustomScanError> {
        let found = unsafe { (*node).type_ };
        if found != expected {
            return Err(CustomScanError::private_codec(
                PrivateDataError::WrongNodeTag {
                    field,
                    expected,
                    found,
                },
            ));
        }
        Ok(())
    }

    pub fn read_oid(&mut self) -> Result<pg_sys::Oid, CustomScanError> {
        let node = unsafe { self.read_cell()? };
        let field = (self.pos - 1) as i32;
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_Integer, field)?;
            let integer = node.cast::<pg_sys::Integer>();
            Ok(pg_sys::Oid::from((*integer).ival as u32))
        }
    }

    pub fn read_i32(&mut self) -> Result<i32, CustomScanError> {
        let node = unsafe { self.read_cell()? };
        let field = (self.pos - 1) as i32;
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_Integer, field)?;
            let integer = node.cast::<pg_sys::Integer>();
            Ok((*integer).ival)
        }
    }

    pub fn read_count(&mut self) -> Result<usize, CustomScanError> {
        let ival = self.read_i32()?;
        if ival < 0 {
            Err(CustomScanError::private_codec(
                PrivateDataError::NegativeCount {
                    field: (self.pos - 1) as i32,
                    value: ival,
                },
            ))
        } else {
            Ok(ival as usize)
        }
    }

    pub fn read_i64(&mut self) -> Result<i64, CustomScanError> {
        let node = unsafe { self.read_cell()? };
        let field = (self.pos - 1) as i32;
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_Float, field)?;
            let float = node.cast::<pg_sys::Float>();
            let fval = (*float).fval;
            if fval.is_null() {
                return Err(CustomScanError::private_codec(
                    PrivateDataError::MalformedI64Cell {
                        position: self.pos - 1,
                    },
                ));
            }
            let c_str = CStr::from_ptr(fval);
            match c_str.to_str() {
                Ok(s) => s.parse::<i64>().map_err(|_| {
                    CustomScanError::private_codec(
                        PrivateDataError::MalformedI64Cell {
                            position: self.pos - 1,
                        },
                    )
                }),
                Err(_) => Err(CustomScanError::private_codec(
                    PrivateDataError::MalformedI64Cell {
                        position: self.pos - 1,
                    },
                )),
            }
        }
    }

    pub fn read_bool(&mut self) -> Result<bool, CustomScanError> {
        let node = unsafe { self.read_cell()? };
        let field = (self.pos - 1) as i32;
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_Boolean, field)?;
            let boolean = node.cast::<pg_sys::Boolean>();
            Ok((*boolean).boolval)
        }
    }

    pub fn read_str(&mut self) -> Result<String, CustomScanError> {
        let node = unsafe { self.read_cell()? };
        let field = (self.pos - 1) as i32;
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_String, field)?;
            let string = node.cast::<pg_sys::String>();
            let sval = (*string).sval;
            if sval.is_null() {
                return Err(CustomScanError::private_codec(
                    PrivateDataError::NullCell { field },
                ));
            }
            let c_str = CStr::from_ptr(sval);
            c_str.to_str().map(|s| s.to_string()).map_err(|_| {
                CustomScanError::private_codec(
                    PrivateDataError::MalformedStringCell {
                        position: self.pos - 1,
                    },
                )
            })
        }
    }

    /// Nested `T_List`; NIL cell maps to empty sub-reader (not via `read_cell`).
    pub fn read_nested(&mut self) -> Result<PrivateDataReader<'a>, CustomScanError> {
        if self.pos >= self.len {
            return Err(CustomScanError::private_codec(
                PrivateDataError::ReadPastEnd {
                    position: self.pos,
                    len: self.len,
                },
            ));
        }
        let cell = unsafe { pg_sys::list_nth(self.list, self.pos as i32) };
        let field = self.pos as i32;
        self.pos += 1;
        if cell.is_null() {
            return Ok(unsafe { PrivateDataReader::from_list(ptr::null_mut()) });
        }
        let node = cell.cast::<pg_sys::Node>();
        unsafe {
            self.expect_node_tag(node, pg_sys::NodeTag::T_List, field)?;
            let list = node.cast::<pg_sys::List>();
            Ok(PrivateDataReader::from_list(list))
        }
    }

    /// Cells not yet consumed.
    pub fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    pub fn finish(self) -> Result<(), CustomScanError> {
        if self.pos == self.len {
            Ok(())
        } else {
            Err(CustomScanError::private_codec(
                PrivateDataError::UnexpectedTrailingCells {
                    read: self.pos,
                    len: self.len,
                },
            ))
        }
    }
}

/// Provider-extensible encode/decode for the opaque tail of `custom_private`.
///
/// Encoded lists MUST be `copyObject`-safe (PG `Node*` values only).
pub trait CustomScanPrivate: Sized {
    /// Encode provider fields via `writer.append_*`.
    fn encode(&self, writer: &mut PrivateDataWriter) -> Result<(), CustomScanError>;

    /// Decode fields previously written by `encode`.
    fn decode(reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError>;
}

/// Provider plan data for providers that need no private payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoPrivateData;

impl CustomScanPrivate for NoPrivateData {
    fn encode(&self, _writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn decode(_reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        Ok(Self)
    }
}

pub(crate) fn usize_to_int(value: usize) -> Result<i32, PrivateDataError> {
    i32::try_from(value)
        .map_err(|_| PrivateDataError::CountTooLargeToEncode { value })
}
