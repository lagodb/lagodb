//! Shared PostgreSQL COPY text/CSV option values.

use std::ffi::CString;

use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::FormatOption;

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
