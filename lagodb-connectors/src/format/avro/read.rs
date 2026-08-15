//! Streaming Avro OCF reader and Foreign Scan adapter.

use std::io::{self, Read};

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignPathBuilder, ForeignPathContext, ForeignPathKeys,
    ForeignPathSpec, ForeignPlanContext, ForeignPlanSpec, ForeignRelSize,
    ForeignRelSizeContext, ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
};
use pg_lakebase_core::tuple::{
    ByteaView, Cell, ColumnDatumCodec, ColumnDatumTarget, StringView,
    numeric_precision_scale, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF,
};
use pg_lakebase_storage::StorageFile;
use pgrx::datum::{USECS_PER_DAY, Uuid as PgUuid};
use pgrx::prelude::{Date, Time, Timestamp, TimestampWithTimeZone};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;
use crate::format::{FormatKind, FormatScanPlanner, FormatScanPrivate, FormatScanState};
use crate::storage::ObjectFiles;

use super::AvroValueKind;

const DEFAULT_ESTIMATED_ROWS: f64 = 1_000.0;
const DEFAULT_ESTIMATED_WIDTH: i32 = 32;

pub(super) struct AvroScanPlanner {
    rows: f64,
}

impl AvroScanPlanner {
    pub(super) const fn new() -> Self {
        Self {
            rows: DEFAULT_ESTIMATED_ROWS,
        }
    }
}

impl FormatScanPlanner for AvroScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Ok(ForeignRelSize::new(self.rows, DEFAULT_ESTIMATED_WIDTH))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        let mut path = ForeignPathSpec::new(
            self.rows,
            0.0,
            self.rows,
            FormatScanPrivate::new(FormatKind::Avro),
        );
        path.retrieved_rows = self.rows;
        paths.push(path);
        Ok(())
    }

    fn supports_pathkeys(
        &self,
        _context: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn build_plan(
        &mut self,
        context: &ForeignPlanContext<'_, Lakebase>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        Ok(ForeignPlanSpec::new(context.path_private().to_owned()))
    }
}

/// Owns the storage handle behind one Avro reader.
pub(super) struct AvroObjectReader {
    file: StorageFile,
}

impl AvroObjectReader {
    pub(super) const fn new(file: StorageFile) -> Self {
        Self { file }
    }
}

impl Read for AvroObjectReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.file.read_into(output).map_err(io::Error::other)
    }
}

#[derive(Clone, Copy)]
pub(super) struct AvroReadColumn {
    source: usize,
    kind: AvroValueKind,
    codec: ColumnDatumCodec,
}

impl AvroReadColumn {
    pub(super) fn bind(
        source: usize,
        schema: &Schema,
        target_oid: pg_sys::Oid,
        target_typmod: i32,
    ) -> Result<Self, ConnectorError> {
        let kind = AvroValueKind::from_schema(schema)?;
        if !kind.supports_target(target_oid) {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                format!(
                    "Avro field type cannot be represented by PostgreSQL type OID {target_oid}",
                ),
            ));
        }
        if let AvroValueKind::Decimal(codec) = kind
            && let Some(target) = numeric_precision_scale(target_typmod)
            && (target.precision != codec.precision() || target.scale != codec.scale() as i32)
        {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal precision and scale must match the PostgreSQL numeric column",
            ));
        }
        Ok(Self {
            source,
            kind,
            codec: ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(target_oid))?,
        })
    }

    pub(super) const fn source(self) -> usize {
        self.source
    }

    /// Decodes a field that was already bound against this object's writer schema.
    pub(super) fn decode(self, value: &Value) -> Result<Option<Cell>, ConnectorError> {
        let value = match value {
            Value::Union(_, value) => value.as_ref(),
            value => value,
        };
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        match (self.kind, value) {
            (AvroValueKind::Boolean, Value::Boolean(value)) => Ok(Some(Cell::Bool(*value))),
            (AvroValueKind::Int, Value::Int(value)) => Ok(Some(Cell::I32(*value))),
            (AvroValueKind::Long, Value::Long(value)) => Ok(Some(Cell::I64(*value))),
            (AvroValueKind::Float, Value::Float(value)) => Ok(Some(Cell::F32(*value))),
            (AvroValueKind::Double, Value::Double(value)) => Ok(Some(Cell::F64(*value))),
            (AvroValueKind::Bytes, Value::Bytes(value)) => {
                // SAFETY: the datum conversion immediately copies this view into
                // PostgreSQL-owned memory before the decoded Avro record drops.
                Ok(Some(Cell::ByteaView(unsafe {
                    ByteaView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::Bytes, Value::Fixed(_, value)) => {
                // SAFETY: identical lifetime argument as the bytes branch.
                Ok(Some(Cell::ByteaView(unsafe {
                    ByteaView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::String, Value::String(value)) => {
                // SAFETY: Avro strings are UTF-8 and the datum conversion copies
                // the view before this record is advanced.
                Ok(Some(Cell::StringView(unsafe {
                    StringView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::String, Value::Enum(_, value)) => {
                // SAFETY: Avro enum symbols are UTF-8 schema strings.
                Ok(Some(Cell::StringView(unsafe {
                    StringView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::Uuid, Value::Uuid(value)) => Ok(Some(Cell::Uuid(
                PgUuid::from_bytes(*value.as_bytes()),
            ))),
            (AvroValueKind::Date, Value::Date(value)) => {
                let days = value
                    .checked_sub(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.out_of_range())?;
                if days == i32::MIN || days == i32::MAX {
                    return Err(self.out_of_range());
                }
                let date = Date::try_from(days).map_err(|_| self.out_of_range())?;
                Ok(Some(Cell::Date(date)))
            }
            (AvroValueKind::TimeMicros, Value::TimeMillis(value)) => {
                let micros = i64::from(*value)
                    .checked_mul(1_000)
                    .ok_or_else(|| self.out_of_range())?;
                self.time(micros)
            }
            (AvroValueKind::TimeMicros, Value::TimeMicros(value)) => {
                self.time(*value)
            }
            (AvroValueKind::TimestampMicros, Value::TimestampMillis(value)) => {
                self.timestamp(i64::from(*value).checked_mul(1_000).ok_or_else(|| self.out_of_range())?)
            }
            (AvroValueKind::TimestampMicros, Value::TimestampMicros(value)) => {
                self.timestamp(*value)
            }
            (AvroValueKind::LocalTimestampMicros, Value::LocalTimestampMillis(value)) => {
                self.local_timestamp(i64::from(*value).checked_mul(1_000).ok_or_else(|| self.out_of_range())?)
            }
            (AvroValueKind::LocalTimestampMicros, Value::LocalTimestampMicros(value)) => {
                self.local_timestamp(*value)
            }
            (AvroValueKind::Decimal(codec), Value::Decimal(value)) => {
                let bytes = Vec::try_from(value).map_err(ConnectorError::from)?;
                Ok(Some(Cell::Numeric(codec.decode_signed_be_bytes(&bytes)?)))
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "a decoded Avro datum does not match its writer-schema field",
            )),
        }
    }

    fn timestamp(self, unix_micros: i64) -> Result<Option<Cell>, ConnectorError> {
        let pg_micros = unix_micros
            .checked_sub(PG_EPOCH_USECS_DIFF)
            .ok_or_else(|| self.out_of_range())?;
        if pg_micros == i64::MIN || pg_micros == i64::MAX {
            return Err(self.out_of_range());
        }
        let timestamp = TimestampWithTimeZone::try_from(pg_micros)
            .map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Timestamptz(timestamp)))
    }

    fn time(self, micros: i64) -> Result<Option<Cell>, ConnectorError> {
        if !(0..USECS_PER_DAY).contains(&micros) {
            return Err(self.out_of_range());
        }
        let time = Time::try_from(micros).map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Time(time)))
    }

    fn local_timestamp(self, unix_micros: i64) -> Result<Option<Cell>, ConnectorError> {
        let pg_micros = unix_micros
            .checked_sub(PG_EPOCH_USECS_DIFF)
            .ok_or_else(|| self.out_of_range())?;
        if pg_micros == i64::MIN || pg_micros == i64::MAX {
            return Err(self.out_of_range());
        }
        let timestamp = Timestamp::try_from(pg_micros).map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Timestamp(timestamp)))
    }

    fn out_of_range(self) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Avro,
            "an Avro temporal value is outside the PostgreSQL range",
        )
    }

    pub(super) unsafe fn datum(self, value: &Value) -> Result<Option<pg_sys::Datum>, ConnectorError> {
        let Some(cell) = self.decode(value)? else {
            return Ok(None);
        };
        // SAFETY: the FDW scan callback owns the current PostgreSQL memory
        // context, and the bound codec targets this output column's OID.
        unsafe { self.codec.cell_to_datum(cell).map(Some).map_err(ConnectorError::from) }
    }
}

struct ScanColumn {
    reader: AvroReadColumn,
    output: ScanOutputColumn,
}

pub(super) struct AvroScanState {
    files: ObjectFiles,
    reader: Option<Reader<'static, AvroObjectReader>>,
    schema: Option<Schema>,
    columns: Box<[ScanColumn]>,
}

impl AvroScanState {
    pub(super) fn begin(
        context: BeginForeignScanContext<'_, Lakebase>,
        mut files: ObjectFiles,
    ) -> Result<Self, ConnectorError> {
        let live = context.relation.live_columns();
        for column in live.iter() {
            column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Avro,
                    "PostgreSQL column names must be valid UTF-8 for Avro",
                )
            })?;
        }
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                reader: None,
                schema: None,
                columns: Box::new([]),
            });
        };
        let reader = Reader::new(AvroObjectReader::new(first?))?;
        let schema = reader.writer_schema().clone();
        let Schema::Record(record) = &schema else {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro container writer schema must be a record",
            ));
        };
        let mut columns_by_attno = vec![None; context.relation.natts()];
        for column in live.iter() {
            columns_by_attno[(column.attno() - 1) as usize] = Some(column);
        }
        let columns = context
            .output_layout
            .columns()
            .iter()
            .copied()
            .map(|output| {
                let relation_index = (output.attno() - 1) as usize;
                let column = columns_by_attno[relation_index].ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "a planned output column is not a live relation attribute",
                    )
                })?;
                let name = column
                    .name()
                    .to_str()
                    .expect("all live Avro column names were validated as UTF-8");
                let source = record
                    .fields
                    .iter()
                    .position(|field| field.name == name)
                    .ok_or_else(|| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Avro,
                            format!("column {name:?} is missing from the Avro schema"),
                        )
                    })?;
                Ok(ScanColumn {
                    reader: AvroReadColumn::bind(
                        source,
                        &record.fields[source].schema,
                        column.type_oid(),
                        column.type_mod(),
                    )?,
                    output,
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?
            .into_boxed_slice();
        Ok(Self {
            files,
            reader: Some(reader),
            schema: Some(schema),
            columns,
        })
    }

    fn open_next_reader(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let reader = Reader::new(AvroObjectReader::new(file?))?;
        let schema = self
            .schema
            .as_ref()
            .expect("a non-empty Avro input has a bound writer schema");
        if reader.writer_schema() != schema {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "objects under one prefix do not share the same Avro writer schema",
            ));
        }
        self.reader = Some(reader);
        Ok(true)
    }
}

impl FormatScanState for AvroScanState {
    fn next_slot(&mut self, output: &mut ScanSlotWriter<'_>) -> Result<bool, ConnectorError> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(value) = reader.next()
            {
                let value = value?;
                let Value::Record(fields) = value else {
                    unreachable!("a record writer schema always decodes to a record value");
                };
                // SAFETY: each ScanColumn was created from this scan's output
                // layout and all by-reference Datum values are allocated in the
                // executor tuple context by its bound codec.
                let mut writer = unsafe { output.datum_writer() };
                for column in self.columns.iter() {
                    // SAFETY: `source` was resolved against this exact writer
                    // schema at Begin; apache-avro decodes a record with one
                    // value per writer-schema field in that order.
                    let source = unsafe { fields.get_unchecked(column.reader.source()) };
                    let value = unsafe { column.reader.datum(source) }?;
                    unsafe {
                        writer.write(
                            column.output,
                            value.unwrap_or(pg_sys::Datum::from(0)),
                            value.is_none(),
                        );
                    }
                }
                return Ok(true);
            }
            self.reader = None;
            if !self.open_next_reader()? {
                return Ok(false);
            }
        }
    }

    fn rescan(
        &mut self,
        _context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ConnectorError> {
        self.files.reset();
        self.reader = None;
        self.open_next_reader()?;
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.reader = None;
        Ok(())
    }
}
