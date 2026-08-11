//! PostgreSQL COPY CSV-format object and its validated options.

use pg_lakebase_core::storage::foreign::ForeignOptionView;

use crate::error::ConnectorError;

use super::delimited::{DelimitedOptions, DelimitedOptionsBuilder};
use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatWriter,
    StreamCompression,
};

/// CSV-format processor.
pub(crate) struct CsvFormat {
    // Validated once here; the CSV scan/write implementation will consume
    // this configuration when that existing capability skeleton is filled in.
    _options: CsvOptions,
    pub(super) _compression: StreamCompression,
}

#[derive(Debug)]
struct CsvOptions {
    _delimiter: u8,
    _null_marker: Box<str>,
    _quote: u8,
    _escape: u8,
    _header: CsvHeader,
    _encoding: Option<Box<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CsvHeader {
    False,
    True,
    Match,
}

impl CsvHeader {
    fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("off") {
            Some(Self::False)
        } else if value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("on")
        {
            Some(Self::True)
        } else if value == "0" {
            Some(Self::False)
        } else if value.eq_ignore_ascii_case("match") {
            Some(Self::Match)
        } else {
            None
        }
    }
}

impl CsvFormat {
    pub(crate) fn resolve(
        compression: StreamCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        let mut delimited = DelimitedOptionsBuilder::default();
        let mut header = None;
        let mut quote = None;
        let mut escape = None;
        for option in options.iter().copied() {
            if delimited.consume(option)? {
                continue;
            }
            let target = match option.name() {
                "header" => &mut header,
                "quote" => &mut quote,
                "escape" => &mut escape,
                _ => {
                    return Err(ConnectorError::invalid_option(
                        option.name(),
                        "is not valid for csv",
                    ));
                }
            };
            if target.replace(option.value()).is_some() {
                return Err(ConnectorError::invalid_option(
                    option.name(),
                    "must not be specified more than once",
                ));
            }
        }

        let DelimitedOptions {
            delimiter,
            null_marker,
            encoding,
        } = delimited.resolve(",", "")?;
        let quote_value = quote.unwrap_or("\"");
        DelimitedOptionsBuilder::validate_single_byte("quote", quote_value)?;
        let quote = quote_value.as_bytes()[0];
        let escape_value = escape.unwrap_or(quote_value);
        DelimitedOptionsBuilder::validate_single_byte("escape", escape_value)?;
        let escape = escape_value.as_bytes()[0];
        if delimiter == quote {
            return Err(ConnectorError::invalid_option(
                "quote",
                "must differ from delimiter",
            ));
        }
        if null_marker.as_bytes().contains(&quote) {
            return Err(ConnectorError::invalid_option(
                "null",
                "cannot contain the CSV quote character",
            ));
        }
        let header = header
            .map(|value| {
                CsvHeader::parse(value).ok_or_else(|| {
                    ConnectorError::invalid_option(
                        "header",
                        "must be false, true, on, off, 0, 1, or match",
                    )
                })
            })
            .transpose()?
            .unwrap_or(CsvHeader::False);

        Ok(Self {
            _options: CsvOptions {
                _delimiter: delimiter,
                _null_marker: null_marker,
                _quote: quote,
                _escape: escape,
                _header: header,
                _encoding: encoding,
            },
            _compression: compression,
        })
    }

    pub(crate) fn validate_column_options(
        options: &[Option<String>],
    ) -> Result<(), ConnectorError> {
        ColumnOptions::parse(options)?.validate()
    }

    pub(crate) fn validate_column_view(
        options: ForeignOptionView<'_>,
    ) -> Result<(), ConnectorError> {
        ColumnOptions::parse_view(options)?.validate()
    }
}

impl FormatObject for CsvFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Csv
    }
}

impl FormatReader for CsvFormat {}

impl FormatWriter for CsvFormat {}

struct ColumnOptions {
    force_null: bool,
    force_not_null: bool,
    seen_force_null: bool,
    seen_force_not_null: bool,
}

impl ColumnOptions {
    fn empty() -> Self {
        Self {
            force_null: false,
            force_not_null: false,
            seen_force_null: false,
            seen_force_not_null: false,
        }
    }

    fn parse(options: &[Option<String>]) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                ConnectorError::invalid_option(
                    "foreign column option",
                    "expected name=value",
                )
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn parse_view(options: ForeignOptionView<'_>) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                ConnectorError::invalid_option(
                    "foreign column option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                ConnectorError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn set(&mut self, name: &str, value: &str) -> Result<(), ConnectorError> {
        let (seen, target) = match name {
            "force_null" => (&mut self.seen_force_null, &mut self.force_null),
            "force_not_null" => {
                (&mut self.seen_force_not_null, &mut self.force_not_null)
            }
            _ => {
                return Err(ConnectorError::invalid_option(
                    name,
                    "is not a supported foreign column option",
                ));
            }
        };
        if *seen {
            return Err(ConnectorError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        *seen = true;
        *target = Self::parse_boolean(name, value)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.force_null && self.force_not_null {
            return Err(ConnectorError::invalid_option(
                "force_null/force_not_null",
                "cannot both be true",
            ));
        }
        Ok(())
    }

    fn parse_boolean(name: &str, value: &str) -> Result<bool, ConnectorError> {
        if value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("on")
        {
            Ok(true)
        } else if value == "0"
            || value.eq_ignore_ascii_case("false")
            || value.eq_ignore_ascii_case("off")
        {
            Ok(false)
        } else {
            Err(ConnectorError::invalid_option(
                name,
                "must be true, false, on, off, 1, or 0",
            ))
        }
    }
}
