//! Streaming compression around line-oriented object formats.

use std::io::{self, BufReader, Read, Write};

use flate2::{
    Compression as GzipCompression, read::MultiGzDecoder, write::GzEncoder,
};
use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};

use super::StreamCompression;

/// A reader that exposes decoded object bytes to a format parser.
pub(crate) enum StreamDecoder<R: Read> {
    Plain(R),
    Gzip(MultiGzDecoder<R>),
    Zstd(ZstdDecoder<'static, BufReader<R>>),
}

impl<R: Read> StreamDecoder<R> {
    pub(crate) fn new(reader: R, compression: StreamCompression) -> io::Result<Self> {
        match compression {
            StreamCompression::None => Ok(Self::Plain(reader)),
            StreamCompression::Gzip => Ok(Self::Gzip(MultiGzDecoder::new(reader))),
            StreamCompression::Zstd => Ok(Self::Zstd(ZstdDecoder::new(reader)?)),
        }
    }

    /// Read enough decoded bytes to satisfy PostgreSQL's COPY callback contract.
    ///
    /// A return value below `min_read` is reserved for decoded EOF. Readers are
    /// allowed to produce short non-EOF reads, so keep reading until the
    /// contract is satisfied rather than deriving EOF from the compressed
    /// object's byte length.
    pub(crate) fn read_at_least(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let target = min_read.max(1);
        if target > output.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "minimum read exceeds output buffer length",
            ));
        }

        let mut read = 0;
        while read < target {
            let count = Read::read(self, &mut output[read..])?;
            if count == 0 {
                break;
            }
            read += count;
        }
        Ok(read)
    }
}

impl<R: Read> Read for StreamDecoder<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(reader) => reader.read(output),
            Self::Gzip(reader) => reader.read(output),
            Self::Zstd(reader) => reader.read(output),
        }
    }
}

/// A writer that encodes format bytes before they reach object storage.
pub(crate) enum StreamEncoder<W: Write> {
    Plain(W),
    Gzip(GzEncoder<W>),
    Zstd(ZstdEncoder<'static, W>),
}

impl<W: Write> StreamEncoder<W> {
    pub(crate) fn new(writer: W, compression: StreamCompression) -> io::Result<Self> {
        match compression {
            StreamCompression::None => Ok(Self::Plain(writer)),
            StreamCompression::Gzip => Ok(Self::Gzip(GzEncoder::new(
                writer,
                GzipCompression::default(),
            ))),
            StreamCompression::Zstd => Ok(Self::Zstd(ZstdEncoder::new(writer, 0)?)),
        }
    }

    /// Finish the encoded stream and return its writer.
    ///
    /// This must be called on every successful write. Gzip and Zstandard both
    /// emit required stream metadata during finalization; `Drop` cannot report
    /// a failure and is therefore only an error-path cleanup fallback.
    pub(crate) fn finish(self) -> io::Result<W> {
        match self {
            Self::Plain(mut writer) => {
                writer.flush()?;
                Ok(writer)
            }
            Self::Gzip(writer) => writer.finish(),
            Self::Zstd(writer) => writer.finish(),
        }
    }

    pub(crate) fn writer(&self) -> &W {
        match self {
            Self::Plain(writer) => writer,
            Self::Gzip(writer) => writer.get_ref(),
            Self::Zstd(writer) => writer.get_ref(),
        }
    }
}

impl<W: Write> Write for StreamEncoder<W> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(writer) => writer.write(input),
            Self::Gzip(writer) => writer.write(input),
            Self::Zstd(writer) => writer.write(input),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Gzip(writer) => writer.flush(),
            Self::Zstd(writer) => writer.flush(),
        }
    }
}
