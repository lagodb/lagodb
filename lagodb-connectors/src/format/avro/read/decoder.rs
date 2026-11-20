//! Bounds-checked Avro binary primitives over one decompressed block.
//!
//! Every read returns an error instead of indexing past the block, so a
//! corrupt object can only fail the scan. Running past a block reports
//! `Details::ReadBlock`: the block was already read in full, so this is
//! corrupt data rather than an I/O failure.

use apache_avro::Uuid;
use apache_avro::error::Details;
use apache_avro::schema::{DecimalSchema, InnerDecimalSchema, Schema, UuidSchema};

use crate::error::ConnectorError;

#[cold]
pub(super) fn avro_error(details: Details) -> ConnectorError {
    ConnectorError::Avro(details.into())
}

#[cold]
fn truncated() -> ConnectorError {
    avro_error(Details::ReadBlock)
}

#[inline]
pub(super) const fn zigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Decodes one varint from bytes supplied by `next_byte`; both the block
/// cursor and the container stream use it.
///
/// A `u64` spans nine full 7-bit groups and one more bit, so a tenth byte
/// above 1 overflows instead of silently losing its high bits.
#[inline]
pub(super) fn decode_varint(
    mut next_byte: impl FnMut() -> Result<u8, ConnectorError>,
) -> Result<u64, ConnectorError> {
    let mut value = 0_u64;
    for index in 0..9_u32 {
        let byte = next_byte()?;
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    match next_byte()? {
        byte @ 0..=1 => Ok(value | (u64::from(byte) << 63)),
        _ => Err(avro_error(Details::IntegerOverflow)),
    }
}

/// Read position inside one decompressed block.
pub(super) struct AvroCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> AvroCursor<'a> {
    pub(super) const fn new(bytes: &'a [u8], position: usize) -> Self {
        Self { bytes, position }
    }

    pub(super) const fn position(&self) -> usize {
        self.position
    }

    pub(super) const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    #[inline]
    pub(super) fn take(&mut self, len: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self.position.checked_add(len).ok_or_else(truncated)?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(truncated)?;
        self.position = end;
        Ok(bytes)
    }

    #[inline]
    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        let bytes = self.take(N)?;
        Ok(bytes.try_into().expect("take returned exactly N bytes"))
    }

    #[inline]
    pub(super) fn advance(&mut self, len: usize) -> Result<(), ConnectorError> {
        self.take(len).map(drop)
    }

    #[inline]
    fn read_byte(&mut self) -> Result<u8, ConnectorError> {
        let byte = *self.bytes.get(self.position).ok_or_else(truncated)?;
        self.position += 1;
        Ok(byte)
    }

    #[inline]
    fn read_varint(&mut self) -> Result<u64, ConnectorError> {
        decode_varint(|| self.read_byte())
    }

    #[inline]
    pub(super) fn read_long(&mut self) -> Result<i64, ConnectorError> {
        self.read_varint().map(zigzag)
    }

    #[inline]
    pub(super) fn skip_long(&mut self) -> Result<(), ConnectorError> {
        self.read_varint().map(drop)
    }

    #[inline]
    fn read_int(&mut self) -> Result<i32, ConnectorError> {
        let value = self.read_long()?;
        i32::try_from(value)
            .map_err(|source| avro_error(Details::ZagI32(source, value)))
    }

    #[inline]
    pub(super) fn read_len(&mut self) -> Result<usize, ConnectorError> {
        let len = self.read_long()?;
        usize::try_from(len)
            .map_err(|source| avro_error(Details::ConvertI64ToUsize(source, len)))
    }

    #[inline]
    pub(super) fn read_sized(&mut self) -> Result<&'a [u8], ConnectorError> {
        let len = self.read_len()?;
        self.take(len)
    }

    /// Reads a union branch index and checks it against the variant count.
    #[inline]
    pub(super) fn read_union_index(
        &mut self,
        variants: usize,
    ) -> Result<usize, ConnectorError> {
        let index = self.read_long()?;
        let branch = usize::try_from(index).map_err(|source| {
            avro_error(Details::ConvertI64ToUsize(source, index))
        })?;
        if branch >= variants {
            return Err(avro_error(Details::GetUnionVariant {
                index,
                num_variants: variants,
            }));
        }
        Ok(branch)
    }
}

/// One decoded scalar, borrowing variable-length values from the block or,
/// for enum symbols, from the read plan.
pub(super) enum AvroDatum<'a> {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(&'a [u8]),
    String(&'a str),
    Uuid([u8; 16]),
    Date(i32),
    TimeMillis(i32),
    TimeMicros(i64),
    TimestampMillis(i64),
    TimestampMicros(i64),
    LocalTimestampMillis(i64),
    LocalTimestampMicros(i64),
    /// Big-endian two's-complement unscaled value.
    Decimal(&'a [u8]),
}

/// Wire decoding for each writer type that `AvroValueKind` can bind.
pub(super) enum LeafDecode {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Fixed(usize),
    String,
    Enum(Box<[String]>),
    UuidString,
    UuidBytes,
    UuidFixed,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    LocalTimestampMillis,
    LocalTimestampMicros,
    DecimalBytes,
    DecimalFixed(usize),
}

impl LeafDecode {
    /// Returns `None` for writer types that are never bound to a column.
    pub(super) fn from_schema(schema: &Schema) -> Option<Self> {
        Some(match schema {
            Schema::Boolean => Self::Boolean,
            Schema::Int => Self::Int,
            Schema::Long => Self::Long,
            Schema::Float => Self::Float,
            Schema::Double => Self::Double,
            Schema::Bytes => Self::Bytes,
            Schema::Fixed(fixed) => Self::Fixed(fixed.size),
            Schema::String => Self::String,
            Schema::Enum(enumeration) => {
                Self::Enum(enumeration.symbols.clone().into_boxed_slice())
            }
            Schema::Uuid(UuidSchema::String) => Self::UuidString,
            Schema::Uuid(UuidSchema::Bytes) => Self::UuidBytes,
            // apache-avro only parses a uuid over `fixed` of size 16.
            Schema::Uuid(UuidSchema::Fixed(_)) => Self::UuidFixed,
            Schema::Date => Self::Date,
            Schema::TimeMillis => Self::TimeMillis,
            Schema::TimeMicros => Self::TimeMicros,
            Schema::TimestampMillis => Self::TimestampMillis,
            Schema::TimestampMicros => Self::TimestampMicros,
            Schema::LocalTimestampMillis => Self::LocalTimestampMillis,
            Schema::LocalTimestampMicros => Self::LocalTimestampMicros,
            Schema::Decimal(DecimalSchema { inner, .. }) => match inner {
                InnerDecimalSchema::Bytes => Self::DecimalBytes,
                InnerDecimalSchema::Fixed(fixed) => Self::DecimalFixed(fixed.size),
            },
            _ => return None,
        })
    }

    /// Decodes and validates one value, as apache-avro does for a bound
    /// field: booleans must be 0 or 1, strings UTF-8, enum indexes in range,
    /// a string uuid its text form, and a bytes uuid exactly 16 bytes.
    pub(super) fn decode<'a>(
        &'a self,
        cursor: &mut AvroCursor<'a>,
    ) -> Result<AvroDatum<'a>, ConnectorError> {
        Ok(match self {
            Self::Boolean => match cursor.read_byte()? {
                0 => AvroDatum::Boolean(false),
                1 => AvroDatum::Boolean(true),
                byte => return Err(avro_error(Details::BoolValue(byte))),
            },
            Self::Int => AvroDatum::Int(cursor.read_int()?),
            Self::Long => AvroDatum::Long(cursor.read_long()?),
            Self::Float => AvroDatum::Float(f32::from_le_bytes(cursor.take_array()?)),
            Self::Double => {
                AvroDatum::Double(f64::from_le_bytes(cursor.take_array()?))
            }
            Self::Bytes => AvroDatum::Bytes(cursor.read_sized()?),
            Self::Fixed(size) => AvroDatum::Bytes(cursor.take(*size)?),
            Self::String => {
                let bytes = cursor.read_sized()?;
                AvroDatum::String(std::str::from_utf8(bytes).map_err(|source| {
                    avro_error(Details::ConvertToUtf8Error(source))
                })?)
            }
            Self::Enum(symbols) => {
                let raw = cursor.read_int()?;
                let index = usize::try_from(raw).map_err(|source| {
                    avro_error(Details::ConvertI32ToUsize(source, raw))
                })?;
                let symbol = symbols.get(index).ok_or_else(|| {
                    avro_error(Details::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    })
                })?;
                AvroDatum::String(symbol)
            }
            Self::UuidString => {
                let text =
                    std::str::from_utf8(cursor.read_sized()?).map_err(|source| {
                        avro_error(Details::ConvertToUtf8Error(source))
                    })?;
                let uuid = Uuid::parse_str(text).map_err(|source| {
                    avro_error(Details::ConvertStrToUuid(source))
                })?;
                AvroDatum::Uuid(*uuid.as_bytes())
            }
            Self::UuidBytes => {
                let uuid =
                    Uuid::from_slice(cursor.read_sized()?).map_err(|source| {
                        avro_error(Details::ConvertSliceToUuid(source))
                    })?;
                AvroDatum::Uuid(*uuid.as_bytes())
            }
            Self::UuidFixed => AvroDatum::Uuid(cursor.take_array()?),
            Self::Date => AvroDatum::Date(cursor.read_int()?),
            Self::TimeMillis => AvroDatum::TimeMillis(cursor.read_int()?),
            Self::TimeMicros => AvroDatum::TimeMicros(cursor.read_long()?),
            Self::TimestampMillis => AvroDatum::TimestampMillis(cursor.read_long()?),
            Self::TimestampMicros => AvroDatum::TimestampMicros(cursor.read_long()?),
            Self::LocalTimestampMillis => {
                AvroDatum::LocalTimestampMillis(cursor.read_long()?)
            }
            Self::LocalTimestampMicros => {
                AvroDatum::LocalTimestampMicros(cursor.read_long()?)
            }
            Self::DecimalBytes => AvroDatum::Decimal(cursor.read_sized()?),
            Self::DecimalFixed(size) => AvroDatum::Decimal(cursor.take(*size)?),
        })
    }
}
