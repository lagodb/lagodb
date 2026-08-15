//! Reusable PostgreSQL Datum rows and Avro OCF file encoding.

use std::ffi::CStr;
use std::slice;
use std::str;

use apache_avro::types::Value;
use apache_avro::{Codec, Decimal, Schema, Writer};
use pg_lakebase_core::tuple::{
    DetoastedVarlena, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF,
};
use pgrx::datum::USECS_PER_DAY;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    AvroWriteCompression, FileWriteProgress, ObjectFileEncoder,
    ObjectFileEncoderFactory,
};
use crate::storage::{ObjectFileSuffix, StagedObjectWriter};

use super::plan::{AvroValueKind, AvroWritePlan};

const AVRO_SUFFIX: ObjectFileSuffix = ObjectFileSuffix::new("avro");

/// A fixed-width, statement-reused row of callback-scoped PostgreSQL Datums.
pub(super) struct AvroDatumRow {
    values: Box<[Option<pg_sys::Datum>]>,
}

impl AvroDatumRow {
    pub(super) fn new(width: usize) -> Self {
        Self {
            values: vec![None; width].into_boxed_slice(),
        }
    }

    /// Replace one value whose index was bound against this row's width.
    ///
    /// # Safety
    ///
    /// `index` must be smaller than the row width. A present Datum must remain
    /// valid until the row has been synchronously written.
    pub(super) unsafe fn set_at_bound(
        &mut self,
        index: usize,
        value: Option<pg_sys::Datum>,
    ) {
        // SAFETY: required by this method's contract.
        unsafe { *self.values.get_unchecked_mut(index) = value };
    }

    fn iter(&self) -> slice::Iter<'_, Option<pg_sys::Datum>> {
        self.values.iter()
    }
}

pub(super) struct AvroEncoderFactory {
    schema: Schema,
    fields: Box<[AvroValueKind]>,
    codec: Codec,
}

impl AvroEncoderFactory {
    pub(super) fn new(
        plan: AvroWritePlan,
        compression: AvroWriteCompression,
    ) -> Self {
        let (schema, fields) = plan.into_parts();
        let codec = match compression {
            AvroWriteCompression::Null => Codec::Null,
            AvroWriteCompression::Deflate => Codec::Deflate(Default::default()),
            AvroWriteCompression::Snappy => Codec::Snappy,
            AvroWriteCompression::Zstd => Codec::Zstandard(Default::default()),
        };
        Self {
            schema,
            fields,
            codec,
        }
    }
}

impl ObjectFileEncoderFactory for AvroEncoderFactory {
    type Input = AvroDatumRow;
    type Encoder = AvroFileEncoder;

    fn file_suffix(&self) -> ObjectFileSuffix {
        AVRO_SUFFIX
    }

    fn open(
        &mut self,
        writer: StagedObjectWriter,
    ) -> Result<Self::Encoder, ConnectorError> {
        Ok(AvroFileEncoder::new(
            &self.schema,
            &self.fields,
            self.codec.clone(),
            writer,
        ))
    }
}

pub(super) struct AvroFileEncoder {
    // `writer` is declared first so it releases its schema borrow before
    // `schema` drops.
    writer: Writer<'static, StagedObjectWriter>,
    schema: Box<Schema>,
    fields: Box<[AvroValueKind]>,
    record: Value,
}

impl AvroFileEncoder {
    fn new(
        source_schema: &Schema,
        source_fields: &[AvroValueKind],
        codec: Codec,
        output: StagedObjectWriter,
    ) -> Self {
        let schema = Box::new(source_schema.clone());
        // SAFETY: the boxed allocation remains stable when this struct moves.
        // `writer` is declared before `schema` and is consumed by `finish`, so
        // the reference cannot outlive the allocation.
        let schema_ref: &'static Schema = unsafe { &*(schema.as_ref() as *const Schema) };
        let writer = Writer::with_codec(schema_ref, output, codec);
        let Schema::Record(record) = schema_ref else {
            unreachable!("AvroWritePlan always constructs a record schema");
        };
        let fields = record
            .fields
            .iter()
            .map(|field| {
                (
                    field.name.clone(),
                    Value::Union(0, Box::new(Value::Null)),
                )
            })
            .collect();
        Self {
            writer,
            schema,
            fields: source_fields.to_vec().into_boxed_slice(),
            record: Value::Record(fields),
        }
    }
}

impl ObjectFileEncoder for AvroFileEncoder {
    type Input = AvroDatumRow;

    fn write(
        &mut self,
        row: &Self::Input,
    ) -> Result<FileWriteProgress, ConnectorError> {
        let Value::Record(outputs) = &mut self.record else {
            unreachable!("AvroFileEncoder retains a record value");
        };
        for ((datum, kind), (_, output)) in row
            .iter()
            .zip(self.fields.iter().copied())
            .zip(outputs.iter_mut())
        {
            let Value::Union(index, value) = output else {
                unreachable!("AvroFileEncoder initializes every field as a union");
            };
            match datum {
                None => {
                    *index = 0;
                    **value = Value::Null;
                }
                Some(datum) => {
                    *index = 1;
                    // SAFETY: the plan bound each field kind to this row
                    // position and the caller keeps every present Datum live
                    // through this synchronous write.
                    unsafe { kind.encode_bound_datum_into(*datum, value) }?;
                }
            }
        }
        self.writer.append_value_ref(&self.record)?;
        Ok(FileWriteProgress::new(self.writer.get_ref().bytes_written()))
    }

    fn finish(self) -> Result<StagedObjectWriter, ConnectorError> {
        let Self { writer, schema, .. } = self;
        let output = writer.into_inner()?;
        drop(schema);
        Ok(output)
    }
}

impl AvroValueKind {
    /// Encode a present Datum using the source type fixed by the write plan.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL PostgreSQL value of this bound kind on
    /// the current backend thread and remain live for this call.
    pub(super) unsafe fn encode_bound_datum_into(
        self,
        datum: pg_sys::Datum,
        output: &mut Value,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::Boolean => *output = Value::Boolean(datum.value() != 0),
            Self::Int => *output = Value::Int(datum.value() as i32),
            Self::Long => *output = Value::Long(datum.value() as i64),
            Self::Float => {
                *output = Value::Float(f32::from_bits(datum.value() as u32));
            }
            Self::Double => {
                *output = Value::Double(f64::from_bits(datum.value() as u64));
            }
            Self::Bytes => {
                // SAFETY: required by this method's bound bytea contract.
                let value = unsafe { DetoastedVarlena::from_datum(datum) };
                Self::write_bytes(output, value.bytes());
            }
            Self::String => {
                // SAFETY: required by this method's bound text contract.
                let value = unsafe { DetoastedVarlena::from_datum(datum) };
                // SAFETY: plan construction validated PG_UTF8 once before any
                // text-family Datum can reach this encoder.
                let value = unsafe { str::from_utf8_unchecked(value.bytes()) };
                Self::write_string(output, value);
            }
            Self::Name => {
                // SAFETY: a bound NAME datum points to a live NUL-terminated
                // NameData value.
                let value = unsafe {
                    CStr::from_ptr(
                        (*datum.cast_mut_ptr::<pg_sys::NameData>())
                            .data
                            .as_ptr(),
                    )
                };
                // SAFETY: plan construction validated PG_UTF8 once before any
                // name Datum can reach this encoder.
                let value = unsafe { str::from_utf8_unchecked(value.to_bytes()) };
                Self::write_string(output, value);
            }
            Self::Uuid => {
                // SAFETY: PostgreSQL UUID is exactly 16 bytes and `[u8; 16]`
                // has byte alignment, so this bound UUID pointer is readable.
                let bytes = unsafe { *datum.cast_mut_ptr::<[u8; 16]>() };
                *output = Value::Uuid(uuid::Uuid::from_bytes(bytes));
            }
            Self::Date => {
                let pg_days = datum.value() as i32;
                if pg_days == i32::MIN || pg_days == i32::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_days = pg_days
                    .checked_add(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::Date(unix_days);
            }
            Self::TimeMicros => {
                let micros = datum.value() as i64;
                if !(0..USECS_PER_DAY).contains(&micros) {
                    return Err(self.value_out_of_range());
                }
                *output = Value::TimeMicros(micros);
            }
            Self::TimestampMicros => {
                let pg_micros = datum.value() as i64;
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::TimestampMicros(unix_micros);
            }
            Self::LocalTimestampMicros => {
                let pg_micros = datum.value() as i64;
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::LocalTimestampMicros(unix_micros);
            }
            Self::Decimal(codec) => {
                // SAFETY: this variant is constructed only for a bound,
                // present NUMERIC source.
                let value = unsafe { codec.encode_bound_datum(datum) }?;
                *output = Value::Decimal(Decimal::from(value.to_be_bytes()));
            }
        }
        Ok(())
    }

    fn write_bytes(output: &mut Value, value: &[u8]) {
        match output {
            Value::Bytes(buffer) => {
                buffer.clear();
                buffer.extend_from_slice(value);
            }
            output => *output = Value::Bytes(value.to_vec()),
        }
    }

    fn write_string(output: &mut Value, value: &str) {
        match output {
            Value::String(buffer) => {
                buffer.clear();
                buffer.push_str(value);
            }
            output => *output = Value::String(value.to_owned()),
        }
    }
}
