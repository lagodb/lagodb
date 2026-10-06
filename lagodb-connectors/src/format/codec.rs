//! Direction-specific compression domains.
//!
//! Stream compression wraps line-oriented bytes. Parquet and Avro compression
//! is recorded in container metadata and is therefore selected only by a
//! writer; readers discover it from the object itself.

use std::fmt::{self, Display, Formatter};

use crate::storage::InputFile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamCompression {
    None,
    Gzip,
    Zstd,
}

impl StreamCompression {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "gzip" => Some(Self::Gzip),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    pub(crate) fn from_suffix(key: &str) -> Option<Self> {
        match key.rsplit_once('.')?.1 {
            "gz" | "gzip" => Some(Self::Gzip),
            "zst" | "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Preserve explicit overrides while inferring reads from each selected file.
/// Output compression is bound once from the configured destination path.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StreamCompressionOptions {
    explicit: Option<StreamCompression>,
    output: StreamCompression,
}

impl StreamCompressionOptions {
    pub(crate) fn new(
        explicit: Option<StreamCompression>,
        suffix: Option<StreamCompression>,
    ) -> Self {
        Self {
            explicit,
            output: explicit.or(suffix).unwrap_or(StreamCompression::None),
        }
    }

    pub(crate) fn for_file(self, file: &InputFile) -> StreamCompression {
        self.explicit.unwrap_or_else(|| {
            StreamCompression::from_suffix(file.name())
                .unwrap_or(StreamCompression::None)
        })
    }

    pub(crate) const fn for_output(self) -> StreamCompression {
        self.output
    }
}

impl Display for StreamCompression {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ParquetWriteCompression {
    Uncompressed,
    #[default]
    Snappy,
    Gzip,
    Zstd,
}

impl ParquetWriteCompression {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::Uncompressed),
            "snappy" => Some(Self::Snappy),
            "gzip" => Some(Self::Gzip),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl Display for ParquetWriteCompression {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Uncompressed => "none",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AvroWriteCompression {
    #[default]
    Null,
    Deflate,
    Snappy,
    Zstd,
}

impl AvroWriteCompression {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::Null),
            "deflate" => Some(Self::Deflate),
            "snappy" => Some(Self::Snappy),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl Display for AvroWriteCompression {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Null => "none",
            Self::Deflate => "deflate",
            Self::Snappy => "snappy",
            Self::Zstd => "zstd",
        })
    }
}
