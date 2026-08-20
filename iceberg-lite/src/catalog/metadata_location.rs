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

use std::fmt::Display;
use std::str::FromStr;

use uuid::Uuid;

use crate::compression::CompressionCodec;
use crate::spec::{TableMetadata, parse_metadata_file_compression};
use crate::{Error, ErrorKind, Result};

/// Default folder name for metadata files under the table location.
pub(crate) const METADATA_FOLDER_NAME: &str = "metadata";

/// Helper for parsing a location of the format: `<metadata-dir>/<version>-<uuid>.metadata.json`
/// or with compression: `<metadata-dir>/<version>-<uuid>.gz.metadata.json`.
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    location: String,
    version: i32,
    id: Uuid,
    compression_codec: CompressionCodec,
}

impl MetadataLocation {
    fn compression_from_properties(
        properties: &std::collections::HashMap<String, String>,
    ) -> CompressionCodec {
        parse_metadata_file_compression(properties).unwrap_or(CompressionCodec::None)
    }

    /// Creates a new metadata location starting at version 0 from table metadata.
    pub fn try_new_with_metadata(metadata: &TableMetadata) -> Result<Self> {
        Ok(Self {
            location: metadata.metadata_location()?,
            version: 0,
            id: Uuid::new_v4(),
            compression_codec: Self::compression_from_properties(
                metadata.properties(),
            ),
        })
    }

    /// Creates a new metadata location for an updated metadata file.
    pub fn with_next_version(&self) -> Self {
        Self {
            location: self.location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
            compression_codec: self.compression_codec,
        }
    }

    /// Updates the metadata location from the metadata being committed.
    pub fn try_with_new_metadata(
        &self,
        new_metadata: &TableMetadata,
    ) -> Result<Self> {
        Ok(Self {
            location: new_metadata.metadata_location()?,
            version: self.version,
            id: self.id,
            compression_codec: Self::compression_from_properties(
                new_metadata.properties(),
            ),
        })
    }

    /// Returns the compression codec used for this metadata location.
    pub fn compression_codec(&self) -> CompressionCodec {
        self.compression_codec
    }

    /// Parses a file name of the format `<version>-<uuid>.metadata.json`
    /// or with compression: `<version>-<uuid>.gz.metadata.json`.
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid, CompressionCodec)> {
        let stripped = file_name.strip_suffix(".metadata.json").ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file ending: {file_name}"),
        ))?;

        let gzip_suffix = CompressionCodec::gzip_default().suffix()?;
        let (stripped, compression_codec) =
            if let Some(stripped) = stripped.strip_suffix(gzip_suffix) {
                (stripped, CompressionCodec::gzip_default())
            } else {
                (stripped, CompressionCodec::None)
            };

        let (version, id) = stripped.split_once('-').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata file name format: {file_name}"),
        ))?;

        Ok((
            version.parse::<i32>()?,
            Uuid::parse_str(id)?,
            compression_codec,
        ))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = self.compression_codec.suffix().unwrap_or("");
        write!(
            f,
            "{}/{:0>5}-{}{}.metadata.json",
            self.location, self.version, self.id, suffix
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (location, file_name) = s.rsplit_once('/').ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("Invalid metadata location: {s}"),
        ))?;

        let (version, id, compression_codec) = Self::parse_file_name(file_name)?;

        Ok(MetadataLocation {
            location: location.to_string(),
            version,
            id,
            compression_codec,
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use uuid::Uuid;

    use crate::MetadataLocation;
    use crate::compression::CompressionCodec;

    fn expected_location(
        location: &str,
        version: i32,
        id: &str,
        compression_codec: CompressionCodec,
    ) -> MetadataLocation {
        MetadataLocation {
            location: location.to_string(),
            version,
            id: Uuid::from_str(id).unwrap(),
            compression_codec,
        }
    }

    #[test]
    fn test_metadata_location_from_string() {
        let test_cases = vec![
            // No prefix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "/metadata",
                    1234567,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // Some prefix
            (
                "/abc/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "/abc/metadata",
                    1234567,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // Longer prefix
            (
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "/abc/def/metadata",
                    1234567,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // Prefix with special characters
            (
                "https://127.0.0.1/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "https://127.0.0.1/metadata",
                    1234567,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // Another id
            (
                "/abc/metadata/1234567-81056704-ce5b-41c4-bb83-eb6408081af6.metadata.json",
                Ok(expected_location(
                    "/abc/metadata",
                    1234567,
                    "81056704-ce5b-41c4-bb83-eb6408081af6",
                    CompressionCodec::None,
                )),
            ),
            // Version 0
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "/abc/metadata",
                    0,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // Gzip compression suffix
            (
                "/abc/metadata/00000-2cd22b57-5127-4198-92ba-e4e67c79821b.gz.metadata.json",
                Ok(expected_location(
                    "/abc/metadata",
                    0,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::gzip_default(),
                )),
            ),
            // Negative version
            (
                "/metadata/-123-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // Invalid uuid
            (
                "/metadata/1234567-no-valid-id.metadata.json",
                Err("".to_string()),
            ),
            // Non-numeric version
            (
                "/metadata/noversion-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Err("".to_string()),
            ),
            // Custom metadata directories are valid.
            (
                "/wrongsubdir/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
                Ok(expected_location(
                    "/wrongsubdir",
                    1234567,
                    "2cd22b57-5127-4198-92ba-e4e67c79821b",
                    CompressionCodec::None,
                )),
            ),
            // No .metadata.json suffix
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata",
                Err("".to_string()),
            ),
            (
                "/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.wrong.file",
                Err("".to_string()),
            ),
        ];

        for (input, expected) in test_cases {
            match MetadataLocation::from_str(input) {
                Ok(metadata_location) => {
                    assert!(expected.is_ok());
                    assert_eq!(metadata_location, expected.unwrap());
                }
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

    #[test]
    fn test_metadata_location_with_next_version() {
        let test_cases = vec![
            MetadataLocation::from_str(
                "/abc/def/metadata/1234567-2cd22b57-5127-4198-92ba-e4e67c79821b.metadata.json",
            )
            .unwrap(),
        ];

        for input in test_cases {
            let next = MetadataLocation::from_str(&input.to_string())
                .unwrap()
                .with_next_version();
            assert_eq!(next.location, input.location);
            assert_eq!(next.version, input.version + 1);
            assert_ne!(next.id, input.id);
        }
    }
}
