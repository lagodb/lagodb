//! Shared PostgreSQL COPY text/CSV option values.

use std::ffi::{CStr, CString, c_void};

use lagodb_core::fdw::ColumnRequirements;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{FormatKind, FormatOption, StreamFormat};

#[derive(Clone, Copy)]
pub(crate) enum DelimitedFormat {
    Text,
    Csv,
}

impl DelimitedFormat {
    #[inline]
    pub(crate) const fn kind(self) -> FormatKind {
        match self {
            Self::Text => FormatKind::Text,
            Self::Csv => FormatKind::Csv,
        }
    }

    #[inline]
    pub(crate) const fn stream(self) -> StreamFormat {
        match self {
            Self::Text => StreamFormat::Text,
            Self::Csv => StreamFormat::Csv,
        }
    }
}

#[derive(Debug)]
pub(super) struct DelimitedOptions {
    pub(super) delimiter: u8,
    pub(super) null_marker: Box<str>,
    pub(super) encoding: Option<Box<str>>,
}

#[derive(Default)]
pub(super) struct DelimitedOptionsBuilder<'a> {
    delimiter: Option<&'a str>,
    null_marker: Option<&'a str>,
    encoding: Option<&'a str>,
}

impl<'a> DelimitedOptionsBuilder<'a> {
    pub(super) fn consume(
        &mut self,
        option: FormatOption<'a>,
    ) -> Result<bool, ConnectorError> {
        let target = match option.name() {
            "delimiter" => &mut self.delimiter,
            "null" => &mut self.null_marker,
            "encoding" => &mut self.encoding,
            _ => return Ok(false),
        };
        if target.replace(option.value()).is_some() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "must not be specified more than once",
            ));
        }
        Ok(true)
    }

    pub(super) fn resolve(
        self,
        default_delimiter: &'static str,
        default_null: &'static str,
    ) -> Result<DelimitedOptions, ConnectorError> {
        let delimiter = self.delimiter.unwrap_or(default_delimiter);
        Self::validate_single_byte("delimiter", delimiter)?;
        if matches!(delimiter.as_bytes()[0], b'\r' | b'\n') {
            return Err(ConnectorError::invalid_option(
                "delimiter",
                "cannot be newline or carriage return",
            ));
        }

        let null_marker = self.null_marker.unwrap_or(default_null);
        if null_marker
            .chars()
            .any(|character| matches!(character, '\r' | '\n'))
        {
            return Err(ConnectorError::invalid_option(
                "null",
                "cannot contain newline or carriage return",
            ));
        }
        if null_marker.as_bytes().contains(&delimiter.as_bytes()[0]) {
            return Err(ConnectorError::invalid_option(
                "null",
                "cannot contain the delimiter character",
            ));
        }

        if let Some(encoding) = self.encoding {
            Self::validate_encoding(encoding)?;
        }
        Ok(DelimitedOptions {
            delimiter: delimiter.as_bytes()[0],
            null_marker: null_marker.into(),
            encoding: self.encoding.map(Box::<str>::from),
        })
    }

    pub(super) fn validate_single_byte(
        name: &'static str,
        value: &str,
    ) -> Result<(), ConnectorError> {
        if value.len() != 1 {
            return Err(ConnectorError::invalid_option(
                name,
                "must be a single one-byte character",
            ));
        }
        Ok(())
    }

    fn validate_encoding(value: &str) -> Result<(), ConnectorError> {
        let value = CString::new(value).map_err(|_| {
            ConnectorError::invalid_option(
                "encoding",
                "must be a valid encoding name",
            )
        })?;
        // SAFETY: CString guarantees a NUL-terminated encoding name and the
        // pointer remains live for this call.
        let encoding = unsafe { pg_sys::pg_char_to_encoding_private(value.as_ptr()) };
        if encoding < 0 {
            return Err(ConnectorError::invalid_option(
                "encoding",
                "must be a valid encoding name",
            ));
        }
        Ok(())
    }
}

impl DelimitedOptions {
    pub(super) fn append_postgres_output_options(
        &self,
        options: *mut pg_sys::List,
        format: super::FormatKind,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        self.append_base_options(options, format)
    }

    pub(super) fn append_postgres_options(
        &self,
        options: *mut pg_sys::List,
        format: super::FormatKind,
        relation: &RelationHandle<'_>,
        requirements: &ColumnRequirements,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let mut options = self.append_base_options(options, format)?;
        if !requirements.needs_all_columns() {
            let columns = relation.live_columns();
            let mut columns_by_attno = vec![None; relation.natts()];
            for column in columns.iter() {
                columns_by_attno[(column.attno() - 1) as usize] = Some(column);
            }
            let selected = requirements
                .user_columns()
                .filter_map(|attno| columns_by_attno[(attno - 1) as usize])
                .map(|column| column.name());
            options = Self::append_identifier_list_option(
                options,
                "convert_selectively",
                selected,
            )?;
        }
        Ok(options)
    }

    fn append_base_options(
        &self,
        mut options: *mut pg_sys::List,
        format: super::FormatKind,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        options = Self::append_string_option(options, "format", format.as_str())?;
        let delimiter = [self.delimiter];
        let delimiter = std::str::from_utf8(&delimiter).map_err(|_| {
            ConnectorError::invalid_option(
                "delimiter",
                "must be valid in the PostgreSQL server encoding",
            )
        })?;
        options = Self::append_string_option(options, "delimiter", delimiter)?;
        options = Self::append_string_option(options, "null", &self.null_marker)?;
        if let Some(encoding) = self.encoding.as_deref() {
            options = Self::append_string_option(options, "encoding", encoding)?;
        }

        Ok(options)
    }

    pub(super) fn append_string_option(
        options: *mut pg_sys::List,
        name: &str,
        value: &str,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let name_c = CString::new(name).map_err(|_| {
            ConnectorError::invalid_option(name, "must be a valid COPY option name")
        })?;
        let value_c = CString::new(value).map_err(|_| {
            ConnectorError::invalid_option(name, "must not contain NUL")
        })?;
        // SAFETY: PostgreSQL copies both strings into its current memory
        // context and owns the resulting option node.
        let option = unsafe {
            pg_sys::makeDefElem(
                pg_sys::pstrdup(name_c.as_ptr()),
                pg_sys::makeString(pg_sys::pstrdup(value_c.as_ptr())).cast(),
                -1,
            )
        };
        // SAFETY: `option` is a freshly allocated PostgreSQL node.
        Ok(unsafe { pg_sys::lappend(options, option.cast::<c_void>()) })
    }

    pub(super) fn append_identifier_list_option<'a>(
        options: *mut pg_sys::List,
        name: &str,
        values: impl IntoIterator<Item = &'a CStr>,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let mut value_list = std::ptr::null_mut();
        for value in values {
            // SAFETY: CStr guarantees a NUL-terminated value without interior
            // NUL. COPY expects the original server-encoding identifier bytes
            // and copies them into its current memory context.
            let string =
                unsafe { pg_sys::makeString(pg_sys::pstrdup(value.as_ptr())) };
            // SAFETY: both the list and node are PostgreSQL-owned allocations.
            value_list =
                unsafe { pg_sys::lappend(value_list, string.cast::<c_void>()) };
        }
        let name_c = CString::new(name).map_err(|_| {
            ConnectorError::invalid_option(name, "must be a valid COPY option name")
        })?;
        // SAFETY: `value_list` contains valid String nodes and the DefElem is
        // allocated in the current PostgreSQL memory context.
        let option = unsafe {
            pg_sys::makeDefElem(
                pg_sys::pstrdup(name_c.as_ptr()),
                value_list.cast(),
                -1,
            )
        };
        // SAFETY: `option` is a freshly allocated PostgreSQL node.
        Ok(unsafe { pg_sys::lappend(options, option.cast::<c_void>()) })
    }
}
