//! Avro object-container header and block framing.
//!
//! This mirrors `write/ocf.rs`: apache-avro supplies the schema model and the
//! block codecs, while LagoDB owns the container framing so one decompressed
//! block buffer is reused by every row it holds.

use std::io::{self, BufRead, Read};
use std::str::FromStr;

use apache_avro::error::Details;
use apache_avro::util::{DEFAULT_MAX_ALLOCATION_BYTES, max_allocation_bytes};
use apache_avro::{Codec, Schema};

use crate::error::ConnectorError;

use super::decoder::{avro_error, decode_varint, zigzag};

const AVRO_OBJECT_HEADER: [u8; 4] = *b"Obj\x01";
const SYNC_MARKER_BYTES: usize = 16;

/// One Avro object container positioned after its header.
pub(super) struct AvroContainer<R> {
    input: R,
    codec: Codec,
    marker: [u8; SYNC_MARKER_BYTES],
    block: Vec<u8>,
    position: usize,
    remaining_rows: usize,
}

impl<R: BufRead> AvroContainer<R> {
    /// Reads the container header and returns the writer schema it declares.
    pub(super) fn open(mut input: R) -> Result<(Self, Schema), ConnectorError> {
        let mut magic = [0_u8; 4];
        input
            .read_exact(&mut magic)
            .map_err(|source| avro_error(Details::ReadHeader(source)))?;
        if magic != AVRO_OBJECT_HEADER {
            return Err(avro_error(Details::HeaderMagic));
        }

        // The header metadata is an Avro map<bytes>. Like apache-avro, a key
        // repeated in a later map block replaces the earlier value.
        let mut schema = None;
        let mut codec = None;
        loop {
            let count = read_stream_long(&mut input)?;
            if count == 0 {
                break;
            }
            let count = if count < 0 {
                // A negative count is followed by the block byte size.
                read_stream_long(&mut input)?;
                count
                    .checked_neg()
                    .ok_or_else(|| avro_error(Details::IntegerOverflow))?
            } else {
                count
            };
            for _ in 0..count {
                let key = read_stream_bytes(&mut input, Details::ReadString)?;
                let key = String::from_utf8(key)
                    .map_err(|source| avro_error(Details::ConvertToUtf8(source)))?;
                let value = read_stream_bytes(&mut input, Details::ReadBytes)?;
                match key.as_str() {
                    "avro.schema" => schema = Some(value),
                    "avro.codec" => codec = Some(value),
                    _ => {}
                }
            }
        }

        let schema: serde_json::Value = schema
            .and_then(|schema| serde_json::from_slice(&schema).ok())
            .ok_or_else(|| avro_error(Details::GetAvroSchemaFromMap))?;
        let schema = Schema::parse(&schema)?;
        let codec = match codec {
            None => Codec::Null,
            Some(name) => {
                let name = std::str::from_utf8(&name).map_err(|source| {
                    avro_error(Details::ConvertToUtf8Error(source))
                })?;
                Codec::from_str(name).map_err(|_| {
                    avro_error(Details::CodecNotSupported(name.to_owned()))
                })?
            }
        };
        let mut marker = [0; SYNC_MARKER_BYTES];
        input
            .read_exact(&mut marker)
            .map_err(|source| avro_error(Details::ReadMarker(source)))?;
        Ok((
            Self {
                input,
                codec,
                marker,
                block: Vec::new(),
                position: 0,
                remaining_rows: 0,
            },
            schema,
        ))
    }

    /// Loads blocks until one has an unread row. Returns `false` at the end of
    /// the container.
    pub(super) fn begin_row(&mut self) -> Result<bool, ConnectorError> {
        while self.remaining_rows == 0 {
            if !self.read_block()? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Decompressed payload of the current block.
    pub(super) fn block(&self) -> &[u8] {
        &self.block
    }

    /// Offset of the next unread row in [`Self::block`].
    pub(super) fn position(&self) -> usize {
        self.position
    }

    /// Records that the current row ends at `end`. The last row of a block
    /// must end exactly at the block boundary.
    pub(super) fn finish_row(&mut self, end: usize) -> Result<(), ConnectorError> {
        self.position = end;
        self.remaining_rows -= 1;
        if self.remaining_rows == 0 && end != self.block.len() {
            return Err(avro_error(Details::ReadBlock));
        }
        Ok(())
    }

    fn read_block(&mut self) -> Result<bool, ConnectorError> {
        let at_end = self
            .input
            .fill_buf()
            .map_err(|source| avro_error(Details::ReadVariableIntegerBytes(source)))?
            .is_empty();
        if at_end {
            return Ok(false);
        }
        let rows = read_stream_long(&mut self.input)?;
        let rows = usize::try_from(rows)
            .map_err(|source| avro_error(Details::ConvertI64ToUsize(source, rows)))?;
        let size = allocation_len(read_stream_long(&mut self.input)?)?;

        // Read through `take` so a corrupt size cannot preallocate memory the
        // object does not contain.
        self.block.clear();
        let read = (&mut self.input)
            .take(size as u64)
            .read_to_end(&mut self.block)
            .map_err(|source| avro_error(Details::ReadIntoBuf(source)))?;
        if read != size {
            return Err(avro_error(Details::ReadIntoBuf(
                io::ErrorKind::UnexpectedEof.into(),
            )));
        }
        let mut marker = [0; SYNC_MARKER_BYTES];
        self.input
            .read_exact(&mut marker)
            .map_err(|source| avro_error(Details::ReadBlockMarker(source)))?;
        if marker != self.marker {
            return Err(avro_error(Details::GetBlockMarker));
        }
        self.codec.decompress(&mut self.block)?;
        if rows == 0 && !self.block.is_empty() {
            return Err(avro_error(Details::ReadBlock));
        }
        self.position = 0;
        self.remaining_rows = rows;
        Ok(true)
    }
}

/// Checks a block or header-value length against apache-avro's allocation
/// limit, which its reader applied to the same lengths.
fn allocation_len(len: i64) -> Result<usize, ConnectorError> {
    let desired = usize::try_from(len)
        .map_err(|source| avro_error(Details::ConvertI64ToUsize(source, len)))?;
    let maximum = max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES);
    if desired > maximum {
        return Err(avro_error(Details::MemoryAllocation {
            desired: Some(desired),
            maximum,
        }));
    }
    Ok(desired)
}

/// Reads one zig-zag varint from the container stream.
fn read_stream_long(input: &mut impl Read) -> Result<i64, ConnectorError> {
    decode_varint(|| {
        let mut byte = [0];
        input.read_exact(&mut byte).map_err(|source| {
            avro_error(Details::ReadVariableIntegerBytes(source))
        })?;
        Ok(byte[0])
    })
    .map(zigzag)
}

/// Reads one length-prefixed header value without preallocating its length.
fn read_stream_bytes(
    input: &mut impl Read,
    read_error: fn(io::Error) -> Details,
) -> Result<Vec<u8>, ConnectorError> {
    let len = allocation_len(read_stream_long(input)?)?;
    let mut bytes = Vec::new();
    let read = input
        .take(len as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| avro_error(read_error(source)))?;
    if read != len {
        return Err(avro_error(read_error(io::ErrorKind::UnexpectedEof.into())));
    }
    Ok(bytes)
}
