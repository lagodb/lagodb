//! Reusable JSON scalar-string decoding storage.

use std::ffi::CStr;

use crate::error::ConnectorError;

/// Decodes a serde_json-validated string directly into retained C-string
/// storage for PostgreSQL type input functions.
pub(super) struct JsonScalarDecoder {
    bytes: Vec<u8>,
}

impl JsonScalarDecoder {
    pub(super) const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(super) fn decode<'a>(
        &'a mut self,
        raw: &[u8],
        column: &str,
        logical_line: u64,
    ) -> Result<&'a CStr, ConnectorError> {
        // JsonRecordDecoder already validated the JSON escape syntax while
        // capturing this RawValue. RawValue deliberately defers UTF-16
        // surrogate validation, so the Unicode branch below still checks that
        // one semantic condition while decoding into retained storage.
        let input = &raw[1..raw.len() - 1];
        self.bytes.clear();
        self.bytes.reserve(input.len() + 1);

        let invalid_surrogate = || {
            ConnectorError::invalid_json_value(
                logical_line,
                column,
                "string contains an invalid Unicode surrogate pair",
            )
        };

        let decode_hex_quad = |digits: &[u8]| {
            digits.iter().fold(0_u16, |value, byte| {
                let digit = match byte {
                    b'0'..=b'9' => u16::from(*byte - b'0'),
                    b'a'..=b'f' => u16::from(*byte - b'a' + 10),
                    b'A'..=b'F' => u16::from(*byte - b'A' + 10),
                    _ => {
                        unreachable!("serde_json accepted an invalid Unicode escape")
                    }
                };
                (value << 4) | digit
            })
        };

        let mut copied = 0;
        let mut cursor = 0;
        while let Some(offset) =
            input[cursor..].iter().position(|byte| *byte == b'\\')
        {
            let escape_start = cursor + offset;
            self.bytes.extend_from_slice(&input[copied..escape_start]);
            cursor = escape_start + 1;
            match input[cursor] {
                b'"' => self.bytes.push(b'"'),
                b'\\' => self.bytes.push(b'\\'),
                b'/' => self.bytes.push(b'/'),
                b'b' => self.bytes.push(0x08),
                b'f' => self.bytes.push(0x0c),
                b'n' => self.bytes.push(b'\n'),
                b'r' => self.bytes.push(b'\r'),
                b't' => self.bytes.push(b'\t'),
                b'u' => {
                    let first = decode_hex_quad(&input[cursor + 1..cursor + 5]);
                    cursor += 4;
                    let codepoint = if (0xd800..=0xdbff).contains(&first) {
                        let Some(pair) = input.get(cursor + 1..cursor + 7) else {
                            return Err(invalid_surrogate());
                        };
                        if pair[0] != b'\\' || pair[1] != b'u' {
                            return Err(invalid_surrogate());
                        }
                        let second = decode_hex_quad(&pair[2..6]);
                        if !(0xdc00..=0xdfff).contains(&second) {
                            return Err(invalid_surrogate());
                        }
                        cursor += 6;
                        0x10000
                            + (u32::from(first - 0xd800) << 10)
                            + u32::from(second - 0xdc00)
                    } else if (0xdc00..=0xdfff).contains(&first) {
                        return Err(invalid_surrogate());
                    } else {
                        u32::from(first)
                    };
                    if codepoint == 0 {
                        return Err(ConnectorError::invalid_json_value(
                            logical_line,
                            column,
                            "PostgreSQL values cannot contain a NUL byte",
                        ));
                    }
                    let character = char::from_u32(codepoint)
                        .expect("serde_json accepted an invalid Unicode scalar");
                    let mut encoded = [0; 4];
                    self.bytes.extend_from_slice(
                        character.encode_utf8(&mut encoded).as_bytes(),
                    );
                }
                _ => unreachable!("serde_json accepted an invalid string escape"),
            }
            cursor += 1;
            copied = cursor;
        }
        self.bytes.extend_from_slice(&input[copied..]);
        self.bytes.push(0);
        // SAFETY: every decoded byte came from a serde_json-validated string,
        // decoded NUL was rejected, and one terminator was appended above.
        Ok(unsafe { CStr::from_bytes_with_nul_unchecked(&self.bytes) })
    }
}
