// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Types for keeping sensitive data redacted and zeroized in memory.

use std::fmt;

use zeroize::Zeroizing;

/// A string-like value containing sensitive information such as a password or token.
///
/// The value is redacted from debug output and zeroized when dropped. It does not
/// implement [`Display`]; callers must explicitly call [`Self::expose`] when a
/// protocol or request builder needs the clear-text value.
#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveString(Zeroizing<String>);

impl SensitiveString {
    /// Returns the raw string value.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Returns whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SensitiveString([REDACTED])")
    }
}

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for SensitiveString {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

/// Wrapper for sensitive byte data such as encryption keys and DEKs.
///
/// The value is zeroized on drop, redacted by both [`Debug`] and [`Display`],
/// and exposes only an immutable byte slice. `Box<[u8]>` is used because key
/// material does not grow after it is created.
#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveBytes(Zeroizing<Box<[u8]>>);

impl SensitiveBytes {
    /// Wraps bytes as sensitive material.
    pub fn new(bytes: impl Into<Box<[u8]>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    /// Returns the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the number of bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the byte slice is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

impl fmt::Display for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{SensitiveBytes, SensitiveString};

    #[test]
    fn sensitive_string_redacts_debug_value() {
        let sensitive_value = "my-pw-12345";
        let logged = format!("{:?}", SensitiveString::from(sensitive_value));

        assert!(!logged.contains(sensitive_value));
    }

    #[test]
    fn sensitive_bytes_redacts_debug_value() {
        let sensitive_value = b"my-secret-bytes";
        let logged = format!("{:?}", SensitiveBytes::new(&sensitive_value[..]));

        assert!(
            !logged
                .as_bytes()
                .windows(sensitive_value.len())
                .any(|window| window == sensitive_value)
        );
    }

    #[test]
    fn sensitive_bytes_redacts_display_value() {
        let sensitive_value = b"my-secret-bytes";
        let logged = format!("{}", SensitiveBytes::new(&sensitive_value[..]));

        assert!(
            !logged
                .as_bytes()
                .windows(sensitive_value.len())
                .any(|window| window == sensitive_value)
        );
    }
}
