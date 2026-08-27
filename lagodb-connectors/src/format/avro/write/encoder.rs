//! Bound PostgreSQL Datum rows and direct Avro binary encoding.

use std::ffi::CStr;
use std::rc::Rc;
use std::slice;
use std::str;

use apache_avro::Schema;
use lagodb_core::tuple::{DetoastedVarlena, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
use pgrx::datum::USECS_PER_DAY;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    AvroWriteCompression, FileWriteProgress, ObjectFileEncoder,
    ObjectFileEncoderFactory,
};
use crate::storage::{ObjectFileSuffix, StagedObjectWriter};

use super::ocf::{AvroBinaryBuffer, AvroOcfWriter};
use super::plan::{AvroValueKind, AvroWritePlan};

const AVRO_SUFFIX: ObjectFileSuffix = ObjectFileSuffix::new("avro");

/// A fixed-width, statement-reused row of callback-scoped PostgreSQL Datums.
pub(in crate::format::avro) struct AvroDatumRow {
    values: Box<[Option<pg_sys::Datum>]>,
}

impl AvroDatumRow {
    pub(in crate::format::avro) fn new(width: usize) -> Self {
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
    pub(in crate::format::avro) unsafe fn set_at_bound(
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
    fields: Rc<[AvroValueKind]>,
    compression: AvroWriteCompression,
}

impl AvroEncoderFactory {
    pub(super) fn new(
        plan: AvroWritePlan,
        compression: AvroWriteCompression,
    ) -> Self {
        let (schema, fields) = plan.into_parts();
        Self {
            schema,
            fields: Rc::from(fields),
            compression,
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
        AvroFileEncoder::new(
            &self.schema,
            Rc::clone(&self.fields),
            self.compression,
            writer,
        )
    }
}

pub(super) struct AvroFileEncoder {
    writer: AvroOcfWriter,
    fields: Rc<[AvroValueKind]>,
}

impl AvroFileEncoder {
    fn new(
        schema: &Schema,
        fields: Rc<[AvroValueKind]>,
        compression: AvroWriteCompression,
        output: StagedObjectWriter,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            writer: AvroOcfWriter::new(schema, compression, output)?,
            fields,
        })
    }
}

impl ObjectFileEncoder for AvroFileEncoder {
    type Input = AvroDatumRow;

    fn write(
        &mut self,
        row: &Self::Input,
    ) -> Result<FileWriteProgress, ConnectorError> {
        let fields = &self.fields;
        self.writer.append_row(|output| {
            for (datum, kind) in row.iter().zip(fields.iter().copied()) {
                output.write_union_index(datum.is_some());
                if let Some(datum) = datum {
                    // SAFETY: the plan bound this kind to the row position and
                    // the callback keeps the present Datum live for this call.
                    unsafe { kind.encode_bound_datum_into(*datum, output) }?;
                }
            }
            Ok(())
        })?;
        Ok(FileWriteProgress::new(self.writer.estimated_file_bytes()))
    }

    fn finish(self) -> Result<StagedObjectWriter, ConnectorError> {
        self.writer.finish()
    }
}

impl AvroValueKind {
    /// Encode a present Datum directly into one Avro OCF data block.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL PostgreSQL value of this bound kind on
    /// the current backend thread and remain live for this call.
    pub(super) unsafe fn encode_bound_datum_into(
        self,
        datum: pg_sys::Datum,
        output: &mut AvroBinaryBuffer,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::Boolean => output.write_boolean(datum.value() != 0),
            Self::Int => output.write_int(datum.value() as i32),
            Self::Long => output.write_long(datum.value() as i64),
            Self::Float => {
                output.write_float(f32::from_bits(datum.value() as u32));
            }
            Self::Double => {
                output.write_double(f64::from_bits(datum.value() as u64));
            }
            Self::Bytes => {
                // SAFETY: required by this method's bound bytea contract.
                let value = unsafe { DetoastedVarlena::from_datum(datum) };
                output.write_bytes(value.bytes());
            }
            Self::String => {
                // SAFETY: required by this method's bound text contract.
                let value = unsafe { DetoastedVarlena::from_datum(datum) };
                // SAFETY: plan construction validated PG_UTF8 once before any
                // text-family Datum can reach this encoder.
                let value = unsafe { str::from_utf8_unchecked(value.bytes()) };
                output.write_bytes(value.as_bytes());
            }
            Self::Name => {
                // SAFETY: a bound NAME datum points to a live NUL-terminated
                // NameData value.
                let value = unsafe {
                    CStr::from_ptr(
                        (*datum.cast_mut_ptr::<pg_sys::NameData>()).data.as_ptr(),
                    )
                };
                output.write_bytes(value.to_bytes());
            }
            Self::Uuid => {
                // SAFETY: PostgreSQL UUID is exactly 16 bytes and `[u8; 16]`
                // has byte alignment, so this bound UUID pointer is readable.
                let bytes = unsafe { *datum.cast_mut_ptr::<[u8; 16]>() };
                let uuid = uuid::Uuid::from_bytes(bytes);
                let mut encoded = uuid::Uuid::encode_buffer();
                let value = uuid.hyphenated().encode_lower(&mut encoded);
                output.write_bytes(value.as_bytes());
            }
            Self::Date => {
                let pg_days = datum.value() as i32;
                if pg_days == i32::MIN || pg_days == i32::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_days = pg_days
                    .checked_add(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                output.write_int(unix_days);
            }
            Self::TimeMicros => {
                let micros = datum.value() as i64;
                if !(0..USECS_PER_DAY).contains(&micros) {
                    return Err(self.value_out_of_range());
                }
                output.write_long(micros);
            }
            Self::TimestampMicros => {
                let pg_micros = datum.value() as i64;
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                output.write_long(unix_micros);
            }
            Self::LocalTimestampMicros => {
                let pg_micros = datum.value() as i64;
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                output.write_long(unix_micros);
            }
            Self::Decimal(codec) => {
                // SAFETY: this variant is constructed only for a bound,
                // present NUMERIC source.
                let value = unsafe { codec.encode_bound_datum(datum) }?;
                output.write_bytes(&value.to_be_bytes());
            }
        }
        Ok(())
    }
}
