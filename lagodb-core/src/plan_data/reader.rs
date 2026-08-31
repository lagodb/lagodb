//! Validating reader for plan-owned PostgreSQL lists.

use core::ffi::CStr;
use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use super::PlanDataError;

pub struct PlanDataReader<'a> {
    list: *mut pg_sys::List,
    position: usize,
    length: usize,
    _marker: PhantomData<&'a pg_sys::List>,
}

impl<'a> PlanDataReader<'a> {
    /// Construct a reader for a list whose tag was established by its owner.
    ///
    /// # Safety
    ///
    /// `list` must be NIL or a live PostgreSQL `T_List` for all of `'a`.
    unsafe fn from_list(list: *mut pg_sys::List) -> Self {
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

    /// Validate an untrusted plan node before constructing the reader.
    ///
    /// # Safety
    ///
    /// A non-NULL `list` must point to a live PostgreSQL node for all of `'a`.
    unsafe fn checked_from_list(
        list: *mut pg_sys::List,
        field: usize,
    ) -> Result<Self, PlanDataError> {
        if list.is_null() {
            return Err(PlanDataError::NullList);
        }
        let found = unsafe { (*list).type_ };
        if found != pg_sys::NodeTag::T_List {
            return Err(PlanDataError::WrongNodeTag {
                field,
                expected: pg_sys::NodeTag::T_List,
                found,
            });
        }
        let length = unsafe { (*list).length };
        if length < 0 {
            return Err(PlanDataError::NegativeListLength { field, length });
        }
        Ok(Self {
            list,
            position: 0,
            length: length as usize,
            _marker: PhantomData,
        })
    }

    /// Decode one complete list payload and reject unconsumed fields.
    ///
    /// This is the frame boundary for a list whose node tag was established by
    /// its containing envelope.  Callers provide only the field decoder; they
    /// cannot accidentally omit the trailing-field check.
    ///
    /// # Safety
    ///
    /// `list` must be NIL or a live PostgreSQL `T_List` for the duration of the
    /// decoder call.
    pub(crate) unsafe fn decode_list<T, E>(
        list: *mut pg_sys::List,
        decode: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E>
    where
        T: 'static,
        E: From<PlanDataError>,
    {
        unsafe { Self::from_list(list) }.decode_complete(decode)
    }

    /// Decode a non-NIL top-level list after validating its PostgreSQL node
    /// tag, length, and complete field consumption.
    ///
    /// # Safety
    ///
    /// A non-NULL `list` must point to a live PostgreSQL node for the duration
    /// of the decoder call.
    pub unsafe fn decode_checked_list<T, E>(
        list: *mut pg_sys::List,
        field: usize,
        decode: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E>
    where
        T: 'static,
        E: From<PlanDataError>,
    {
        unsafe { Self::checked_from_list(list, field) }?.decode_complete(decode)
    }

    fn decode_complete<T, E>(
        mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E>
    where
        T: 'static,
        E: From<PlanDataError>,
    {
        let value = decode(&mut self)?;
        self.ensure_consumed()?;
        Ok(value)
    }

    unsafe fn read_cell(&mut self) -> Result<*mut pg_sys::Node, PlanDataError> {
        if self.position >= self.length {
            return Err(PlanDataError::ReadPastEnd {
                position: self.position,
                len: self.length,
            });
        }
        let field = self.position;
        let cell = unsafe { pg_sys::list_nth(self.list, field as i32) };
        self.position += 1;
        if cell.is_null() {
            Err(PlanDataError::NullCell { field })
        } else {
            Ok(cell.cast())
        }
    }

    unsafe fn expect(
        node: *mut pg_sys::Node,
        expected: pg_sys::NodeTag,
        field: usize,
    ) -> Result<(), PlanDataError> {
        let found = unsafe { (*node).type_ };
        if found == expected {
            Ok(())
        } else {
            Err(PlanDataError::WrongNodeTag {
                field,
                expected,
                found,
            })
        }
    }

    pub fn read_i32(&mut self) -> Result<i32, PlanDataError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { Self::expect(node, pg_sys::NodeTag::T_Integer, field)? };
        Ok(unsafe { (*node.cast::<pg_sys::Integer>()).ival })
    }

    pub fn read_oid(&mut self) -> Result<pg_sys::Oid, PlanDataError> {
        Ok((self.read_i32()? as u32).into())
    }

    pub fn read_count(&mut self) -> Result<usize, PlanDataError> {
        let field = self.position;
        let value = self.read_i32()?;
        usize::try_from(value)
            .map_err(|_| PlanDataError::NegativeCount { field, value })
    }

    pub fn read_bool(&mut self) -> Result<bool, PlanDataError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { Self::expect(node, pg_sys::NodeTag::T_Boolean, field)? };
        Ok(unsafe { (*node.cast::<pg_sys::Boolean>()).boolval })
    }

    pub fn read_i64(&mut self) -> Result<i64, PlanDataError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { Self::expect(node, pg_sys::NodeTag::T_Float, field)? };
        let value = unsafe { (*node.cast::<pg_sys::Float>()).fval };
        if value.is_null() {
            return Err(PlanDataError::MalformedI64 { field });
        }
        unsafe { CStr::from_ptr(value) }
            .to_str()
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or(PlanDataError::MalformedI64 { field })
    }

    pub fn read_cstr(&mut self) -> Result<&'a CStr, PlanDataError> {
        let field = self.position;
        let node = unsafe { self.read_cell()? };
        unsafe { Self::expect(node, pg_sys::NodeTag::T_String, field)? };
        let value = unsafe { (*node.cast::<pg_sys::String>()).sval };
        if value.is_null() {
            return Err(PlanDataError::NullString { field });
        }
        Ok(unsafe { CStr::from_ptr(value) })
    }

    pub fn read_str(&mut self) -> Result<String, PlanDataError> {
        let field = self.position;
        self.read_cstr()?
            .to_str()
            .map(str::to_owned)
            .map_err(|_| PlanDataError::InvalidUtf8 { field })
    }

    /// Decode one nested `T_List`; a NULL cell is an empty payload.
    ///
    /// The nested frame is consumed atomically, including its trailing-field
    /// check, before this method returns.
    pub fn read_nested<T, E>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E>
    where
        T: 'static,
        E: From<PlanDataError>,
    {
        let list = self.read_optional_list(pg_sys::NodeTag::T_List)?;
        unsafe { Self::decode_list(list, decode) }
    }

    /// Read a list-valued cell with an explicit PostgreSQL list tag. NIL is
    /// returned as NULL and is valid for every list kind.
    pub(crate) fn read_optional_list(
        &mut self,
        expected: pg_sys::NodeTag,
    ) -> Result<*mut pg_sys::List, PlanDataError> {
        if self.position >= self.length {
            return Err(PlanDataError::ReadPastEnd {
                position: self.position,
                len: self.length,
            });
        }
        let field = self.position;
        let cell = unsafe { pg_sys::list_nth(self.list, field as i32) };
        self.position += 1;
        if cell.is_null() {
            return Ok(ptr::null_mut());
        }
        let node = cell.cast::<pg_sys::Node>();
        unsafe { Self::expect(node, expected, field)? };
        Ok(node.cast())
    }

    /// Read an independently encoded nested plan-data frame.
    ///
    /// The returned pointer remains owned by PostgreSQL and is valid for the
    /// same lifetime as this reader's containing plan. The nested codec must
    /// perform its own complete-field validation.
    pub fn read_encoded_list(&mut self) -> Result<*mut pg_sys::List, PlanDataError> {
        let list = self.read_optional_list(pg_sys::NodeTag::T_List)?;
        if list.is_null() {
            Err(PlanDataError::NullList)
        } else {
            Ok(list)
        }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.length.saturating_sub(self.position)
    }

    fn ensure_consumed(self) -> Result<(), PlanDataError> {
        if self.position == self.length {
            Ok(())
        } else {
            Err(PlanDataError::UnexpectedTrailingCells {
                read: self.position,
                len: self.length,
            })
        }
    }
}
