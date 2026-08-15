//! Reusable Avro object-container block encoding.

use std::io::Write;

use apache_avro::error::Details;
use apache_avro::{Codec, Schema};

use crate::error::ConnectorError;
use crate::format::AvroWriteCompression;
use crate::storage::StagedObjectWriter;

const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";
const BLOCK_TARGET_BYTES: usize = 16_000;
const SYNC_MARKER_BYTES: usize = 16;

/// Mutable Avro binary payload reused across all rows in one OCF block.
pub(super) struct AvroBinaryBuffer {
    bytes: Vec<u8>,
}

impl AvroBinaryBuffer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub(super) fn write_union_index(&mut self, present: bool) {
        self.write_long(if present { 1 } else { 0 });
    }

    #[inline]
    pub(super) fn write_boolean(&mut self, value: bool) {
        self.bytes.push(u8::from(value));
    }

    #[inline]
    pub(super) fn write_int(&mut self, value: i32) {
        self.write_long(i64::from(value));
    }

    #[inline]
    pub(super) fn write_long(&mut self, value: i64) {
        let mut encoded = ((value as u64) << 1) ^ ((value >> 63) as u64);
        while encoded & !0x7f != 0 {
            self.bytes.push((encoded as u8 & 0x7f) | 0x80);
            encoded >>= 7;
        }
        self.bytes.push(encoded as u8);
    }

    #[inline]
    pub(super) fn write_float(&mut self, value: f32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub(super) fn write_double(&mut self, value: f64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub(super) fn write_bytes(&mut self, value: &[u8]) {
        let len = i64::try_from(value.len())
            .expect("PostgreSQL varlena length fits in an Avro long");
        self.write_long(len);
        self.bytes.extend_from_slice(value);
    }

    #[inline]
    fn write_raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    #[inline]
    fn clear(&mut self) {
        self.bytes.clear();
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    fn truncate(&mut self, len: usize) {
        self.bytes.truncate(len);
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

/// One Avro OCF file with a reusable uncompressed row block.
pub(super) struct AvroOcfWriter {
    output: StagedObjectWriter,
    codec: Codec,
    marker: [u8; SYNC_MARKER_BYTES],
    block: AvroBinaryBuffer,
    framing: AvroBinaryBuffer,
    rows: usize,
}

impl AvroOcfWriter {
    pub(super) fn new(
        schema: &Schema,
        compression: AvroWriteCompression,
        output: StagedObjectWriter,
    ) -> Result<Self, ConnectorError> {
        let codec = match compression {
            AvroWriteCompression::Null => Codec::Null,
            AvroWriteCompression::Deflate => Codec::Deflate(Default::default()),
            AvroWriteCompression::Snappy => Codec::Snappy,
            AvroWriteCompression::Zstd => Codec::Zstandard(Default::default()),
        };
        let mut writer = Self {
            output,
            codec,
            marker: uuid::Uuid::now_v7().into_bytes(),
            block: AvroBinaryBuffer::with_capacity(BLOCK_TARGET_BYTES),
            framing: AvroBinaryBuffer::with_capacity(20),
            rows: 0,
        };
        writer.write_header(schema, compression)?;
        Ok(writer)
    }

    /// Append one complete row, rolling back a recoverable conversion error.
    pub(super) fn append_row(
        &mut self,
        encode: impl FnOnce(&mut AvroBinaryBuffer) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        let start = self.block.len();
        if let Err(error) = encode(&mut self.block) {
            self.block.truncate(start);
            return Err(error);
        }
        self.rows += 1;
        if self.block.len() >= BLOCK_TARGET_BYTES {
            self.flush_block()?;
        }
        Ok(())
    }

    pub(super) fn estimated_file_bytes(&self) -> u64 {
        let buffered = u64::try_from(self.block.len()).expect(
            "PostgreSQL is supported only on platforms where usize fits in u64",
        );
        self.output.bytes_written().saturating_add(buffered)
    }

    pub(super) fn finish(mut self) -> Result<StagedObjectWriter, ConnectorError> {
        self.flush_block()?;
        self.output.flush().map_err(|source| {
            ConnectorError::Avro(Details::FlushWriter(source).into())
        })?;
        Ok(self.output)
    }

    fn write_header(
        &mut self,
        schema: &Schema,
        compression: AvroWriteCompression,
    ) -> Result<(), ConnectorError> {
        let schema = serde_json::to_string(schema).map_err(|source| {
            ConnectorError::Avro(Details::ConvertJsonToString(source).into())
        })?;
        let codec_name = match compression {
            AvroWriteCompression::Null => &b"null"[..],
            AvroWriteCompression::Deflate => &b"deflate"[..],
            AvroWriteCompression::Snappy => &b"snappy"[..],
            AvroWriteCompression::Zstd => &b"zstandard"[..],
        };
        let metadata_count: i64 = if matches!(compression, AvroWriteCompression::Zstd)
        {
            3
        } else {
            2
        };
        let mut header = AvroBinaryBuffer::with_capacity(schema.len() + 96);
        header.write_raw(AVRO_OBJECT_HEADER);
        header.write_long(metadata_count);
        header.write_bytes(b"avro.schema");
        header.write_bytes(schema.as_bytes());
        header.write_bytes(b"avro.codec");
        header.write_bytes(codec_name);
        if matches!(compression, AvroWriteCompression::Zstd) {
            header.write_bytes(b"avro.codec.compression_level");
            header.write_bytes(&[0]);
        }
        header.write_long(0);
        header.write_raw(&self.marker);
        self.write_all(header.as_slice())
    }

    fn flush_block(&mut self) -> Result<(), ConnectorError> {
        if self.rows == 0 {
            return Ok(());
        }
        self.codec.compress(&mut self.block.bytes)?;
        self.framing.clear();
        self.framing.write_long(
            i64::try_from(self.rows).expect("one Avro block row count fits in i64"),
        );
        self.framing.write_long(
            i64::try_from(self.block.len()).expect("one Avro block size fits in i64"),
        );
        self.output
            .write_all(self.framing.as_slice())
            .map_err(|source| {
                ConnectorError::Avro(Details::WriteBytes(source).into())
            })?;
        self.output
            .write_all(self.block.as_slice())
            .map_err(|source| {
                ConnectorError::Avro(Details::WriteBytes(source).into())
            })?;
        self.output.write_all(&self.marker).map_err(|source| {
            ConnectorError::Avro(Details::WriteMarker(source).into())
        })?;
        self.block.clear();
        self.rows = 0;
        Ok(())
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), ConnectorError> {
        self.output.write_all(bytes).map_err(|source| {
            ConnectorError::Avro(Details::WriteBytes(source).into())
        })
    }
}
