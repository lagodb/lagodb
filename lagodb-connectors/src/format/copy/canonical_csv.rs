//! Private PostgreSQL CSV contract used only as a native-format bridge.
//!
//! # Accepted native-format performance trade-off
//!
//! PostgreSQL 17's COPY extension callbacks exchange bytes, not `Datum` arrays
//! or tuple slots. The Parquet adapters therefore deliberately round-trip each
//! non-null value through the target PostgreSQL type's text I/O and this fixed
//! CSV representation. COPY FROM converts Arrow to `Datum`, text, CSV, and back
//! to the final `Datum`; COPY TO performs the reverse bridge before producing
//! Arrow. This adds per-datum type I/O, escaping/parsing, and buffer copies.
//!
//! A typed core boundary could convert each Arrow value once into its final
//! `Datum` and feed PostgreSQL-owned values/nulls or slots directly. That would
//! remove the text I/O and CSV work. PG17's `CopyFrom` instead hard-codes
//! `NextCopyFrom`, and its heap/FDW batching implementation is private. Keeping
//! defaults, WHERE, FREEZE, ON_ERROR, triggers, constraints, partition routing,
//! progress reporting, and batching would require an extension-owned,
//! PG17-derived COPY executor. Duplicating that version-sensitive executor has
//! a high correctness and maintenance risk, so this bridge is an intentional
//! trade-off. Revisit it only with benchmark evidence and the regression budget
//! for the complete COPY semantic surface.

use std::ffi::CStr;

use pg_lakebase_core::copy::{CopyContext, CopyOptionView};
use pgrx::pg_sys;

use crate::error::ConnectorError;

pub(super) struct CanonicalCsv;

#[derive(Clone, Copy)]
struct FieldRange {
    start: usize,
    end: usize,
    null: bool,
}

pub(super) struct CanonicalCsvRow {
    bytes: Vec<u8>,
    fields: Vec<FieldRange>,
}

impl CanonicalCsv {
    pub(super) const NULL: &'static [u8] = br"\N";

    const CONNECTOR_OPTION_NAMES: [&[u8]; 3] =
        [b"storage_server", b"format", b"compression"];

    const USER_OVERRIDE_OPTION_NAMES: [&[u8]; 10] = [
        b"delimiter",
        b"null",
        b"default",
        b"header",
        b"quote",
        b"escape",
        b"encoding",
        b"force_quote",
        b"force_not_null",
        b"force_null",
    ];

    pub(super) fn reject_user_overrides(
        options: CopyOptionView<'_>,
    ) -> Result<(), ConnectorError> {
        for option in options.iter() {
            if Self::USER_OVERRIDE_OPTION_NAMES
                .iter()
                .any(|candidate| *candidate == option.name().to_bytes())
            {
                let name = option.name().to_str().map_err(|_| {
                    ConnectorError::invalid_copy_option(
                        "COPY option",
                        "must be valid UTF-8",
                    )
                })?;
                return Err(ConnectorError::invalid_copy_option(
                    name,
                    "is only valid for text or csv",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn postgres_options(context: &CopyContext<'_>) -> *mut pg_sys::List {
        let mut ignored_names = Vec::with_capacity(
            Self::CONNECTOR_OPTION_NAMES.len()
                + Self::USER_OVERRIDE_OPTION_NAMES.len(),
        );
        ignored_names.extend_from_slice(&Self::CONNECTOR_OPTION_NAMES);
        ignored_names.extend_from_slice(&Self::USER_OVERRIDE_OPTION_NAMES);
        let mut options = context
            .statement()
            .option_view()
            .without_names(&ignored_names);
        for (name, value) in [
            (c"format", c"csv"),
            (c"delimiter", c","),
            (c"null", c"\\N"),
            (c"quote", c"\""),
            (c"escape", c"\""),
            (c"header", c"false"),
            (c"encoding", c"UTF8"),
        ] {
            options = unsafe { Self::append_option(options, name, value) };
        }
        options
    }

    unsafe fn append_option(
        options: *mut pg_sys::List,
        name: &std::ffi::CStr,
        value: &std::ffi::CStr,
    ) -> *mut pg_sys::List {
        let option = unsafe {
            pg_sys::makeDefElem(
                pg_sys::pstrdup(name.as_ptr()),
                pg_sys::makeString(pg_sys::pstrdup(value.as_ptr())).cast(),
                -1,
            )
        };
        unsafe { pg_sys::lappend(options, option.cast()) }
    }

    pub(super) fn write_field(output: &mut Vec<u8>, value: &[u8]) {
        let quote = value.is_empty()
            || value == Self::NULL
            || value == br"\."
            || value
                .iter()
                .any(|byte| matches!(*byte, b',' | b'"' | b'\n' | b'\r'));
        if !quote {
            output.extend_from_slice(value);
            return;
        }
        output.push(b'"');
        for &byte in value {
            if byte == b'"' {
                output.push(b'"');
            }
            output.push(byte);
        }
        output.push(b'"');
    }

    pub(super) fn validate_row_width(
        actual: usize,
        expected: usize,
    ) -> Result<(), crate::error::ConnectorError> {
        if actual != expected {
            return Err(crate::error::ConnectorError::invalid_copy_option(
                "COPY row",
                "canonical CSV row width differs from the COPY column layout",
            ));
        }
        Ok(())
    }
}

impl CanonicalCsvRow {
    pub(super) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            fields: Vec::new(),
        }
    }

    pub(super) fn parse(
        &mut self,
        input: &[u8],
        expected_fields: usize,
    ) -> Result<(), crate::error::ConnectorError> {
        self.bytes.clear();
        self.fields.clear();
        if input.is_empty() && expected_fields == 0 {
            return Ok(());
        }
        let mut input_index = 0;
        loop {
            let start = self.bytes.len();
            let quoted = input.get(input_index) == Some(&b'"');
            if quoted {
                input_index += 1;
                loop {
                    let Some(&byte) = input.get(input_index) else {
                        return Err(
                            crate::error::ConnectorError::invalid_copy_option(
                                "COPY row",
                                "canonical CSV contains an unterminated quoted field",
                            ),
                        );
                    };
                    input_index += 1;
                    if byte != b'"' {
                        if byte == 0 {
                            return Err(
                                crate::error::ConnectorError::invalid_copy_option(
                                    "COPY row",
                                    "canonical CSV contains a NUL byte",
                                ),
                            );
                        }
                        self.bytes.push(byte);
                        continue;
                    }
                    if input.get(input_index) == Some(&b'"') {
                        self.bytes.push(b'"');
                        input_index += 1;
                        continue;
                    }
                    break;
                }
                if input_index < input.len() && input[input_index] != b',' {
                    return Err(crate::error::ConnectorError::invalid_copy_option(
                        "COPY row",
                        "canonical CSV has bytes after a closing quote",
                    ));
                }
            } else {
                while let Some(&byte) = input.get(input_index) {
                    if byte == b',' {
                        break;
                    }
                    if byte == b'"' || byte == b'\n' || byte == b'\r' {
                        return Err(
                            crate::error::ConnectorError::invalid_copy_option(
                                "COPY row",
                                "canonical CSV contains an invalid unquoted byte",
                            ),
                        );
                    }
                    if byte == 0 {
                        return Err(
                            crate::error::ConnectorError::invalid_copy_option(
                                "COPY row",
                                "canonical CSV contains a NUL byte",
                            ),
                        );
                    }
                    self.bytes.push(byte);
                    input_index += 1;
                }
            }
            let end = self.bytes.len();
            let null = !quoted && &self.bytes[start..end] == CanonicalCsv::NULL;
            self.bytes.push(0);
            self.fields.push(FieldRange { start, end, null });
            if input_index == input.len() {
                break;
            }
            input_index += 1;
            if input_index == input.len() {
                let start = self.bytes.len();
                self.bytes.push(0);
                self.fields.push(FieldRange {
                    start,
                    end: start,
                    null: false,
                });
                break;
            }
        }
        CanonicalCsv::validate_row_width(self.fields.len(), expected_fields)
    }

    pub(super) fn fields(&self) -> impl ExactSizeIterator<Item = Option<&CStr>> {
        self.fields.iter().map(|field| {
            (!field.null).then(|| {
                // SAFETY: parse rejects embedded NUL bytes and appends exactly
                // one terminator immediately after every recorded field.
                unsafe {
                    CStr::from_bytes_with_nul_unchecked(
                        &self.bytes[field.start..=field.end],
                    )
                }
            })
        })
    }
}
