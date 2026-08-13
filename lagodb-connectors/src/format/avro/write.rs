//! Avro OCF schema construction and streaming writers.

use apache_avro::types::Value;
use apache_avro::{Codec, Decimal, Schema, Writer};
use pg_lakebase_core::fdw::{ForeignModifyOutcome, ModifyPlanSlot, ModifySlot};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::{
    Cell, ColumnDatumCodec, ColumnDatumTarget, Decimal128NumericCodec, Row,
    SlotDatumIndex, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF,
    numeric_precision_scale,
};
use pgrx::datum::USECS_PER_DAY;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    AvroWriteCompression, EmptyOutputPolicy, FileWriteProgress, FormatKind,
    FormatWriteState, ObjectFileEncoder, ObjectFileEncoderFactory, ObjectSetWriter,
};
use crate::storage::{ObjectFileSuffix, ObjectOutput, StagedObjectWriter};

const AVRO_SUFFIX: ObjectFileSuffix = ObjectFileSuffix::new("avro");

#[derive(Clone, Copy)]
pub(super) enum AvroValueKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Uuid,
    Date,
    TimeMicros,
    TimestampMicros,
    LocalTimestampMicros,
    Decimal(Decimal128NumericCodec),
}

impl AvroValueKind {
    pub(super) fn from_oid(oid: pg_sys::Oid, typmod: i32) -> Result<Self, ConnectorError> {
        match oid {
            pg_sys::BOOLOID => Ok(Self::Boolean),
            pg_sys::INT2OID | pg_sys::INT4OID => Ok(Self::Int),
            pg_sys::INT8OID => Ok(Self::Long),
            pg_sys::FLOAT4OID => Ok(Self::Float),
            pg_sys::FLOAT8OID => Ok(Self::Double),
            pg_sys::BYTEAOID => Ok(Self::Bytes),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID => {
                Ok(Self::String)
            }
            pg_sys::UUIDOID => Ok(Self::Uuid),
            pg_sys::DATEOID => Ok(Self::Date),
            pg_sys::TIMEOID => Ok(Self::TimeMicros),
            pg_sys::TIMESTAMPTZOID => Ok(Self::TimestampMicros),
            pg_sys::TIMESTAMPOID => Ok(Self::LocalTimestampMicros),
            pg_sys::NUMERICOID => {
                let numeric = numeric_precision_scale(typmod).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "unconstrained numeric cannot be represented losslessly as Avro decimal",
                    )
                })?;
                let scale = u32::try_from(numeric.scale).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "numeric scale must be non-negative for Avro decimal",
                    )
                })?;
                Self::decimal(numeric.precision, scale)
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                format!("PostgreSQL type OID {oid} is not supported by Avro"),
            )),
        }
    }

    fn schema_name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Bytes => "bytes",
            Self::String => "string",
            Self::Uuid => "uuid",
            Self::Date => "date",
            Self::TimeMicros => "time-micros",
            Self::TimestampMicros => "timestamp-micros",
            Self::LocalTimestampMicros => "local-timestamp-micros",
            Self::Decimal(_) => "decimal",
        }
    }

    fn schema_json(self) -> serde_json::Value {
        match self {
            Self::Uuid => serde_json::json!({ "type": "string", "logicalType": "uuid" }),
            Self::Date => serde_json::json!({ "type": "int", "logicalType": "date" }),
            Self::TimeMicros => {
                serde_json::json!({ "type": "long", "logicalType": "time-micros" })
            }
            Self::TimestampMicros => {
                serde_json::json!({ "type": "long", "logicalType": "timestamp-micros" })
            }
            Self::LocalTimestampMicros => serde_json::json!({
                "type": "long",
                "logicalType": "local-timestamp-micros",
            }),
            Self::Decimal(codec) => serde_json::json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": codec.precision(),
                "scale": codec.scale(),
            }),
            primitive => serde_json::Value::String(primitive.schema_name().to_owned()),
        }
    }

    pub(super) fn from_schema(schema: &Schema) -> Result<Self, ConnectorError> {
        match schema {
            Schema::Boolean => Ok(Self::Boolean),
            Schema::Int => Ok(Self::Int),
            Schema::Long => Ok(Self::Long),
            Schema::Float => Ok(Self::Float),
            Schema::Double => Ok(Self::Double),
            Schema::Bytes | Schema::Fixed(_) => Ok(Self::Bytes),
            Schema::String | Schema::Enum(_) => Ok(Self::String),
            Schema::Uuid => Ok(Self::Uuid),
            Schema::Date => Ok(Self::Date),
            Schema::TimeMillis | Schema::TimeMicros => Ok(Self::TimeMicros),
            Schema::TimestampMillis | Schema::TimestampMicros => {
                Ok(Self::TimestampMicros)
            }
            Schema::LocalTimestampMillis | Schema::LocalTimestampMicros => {
                Ok(Self::LocalTimestampMicros)
            }
            Schema::Decimal(decimal) => {
                Self::decimal_metadata(decimal.precision, decimal.scale)
            }
            Schema::Union(union) => {
                let mut variants = union
                    .variants()
                    .iter()
                    .filter(|variant| !matches!(variant, Schema::Null));
                let Some(variant) = variants.next() else {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "an Avro union must contain one non-null variant",
                    ));
                };
                if variants.next().is_some() {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "an Avro union with multiple non-null variants is unsupported",
                    ));
                }
                Self::from_schema(variant)
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro field type is not supported",
            )),
        }
    }

    pub(super) const fn supports_target(self, oid: pg_sys::Oid) -> bool {
        match self {
            Self::Boolean => oid == pg_sys::BOOLOID,
            Self::Int | Self::Long => {
                oid == pg_sys::INT2OID || oid == pg_sys::INT4OID || oid == pg_sys::INT8OID
            }
            Self::Float => oid == pg_sys::FLOAT4OID,
            Self::Double => oid == pg_sys::FLOAT8OID,
            Self::Bytes => oid == pg_sys::BYTEAOID,
            Self::String => {
                oid == pg_sys::TEXTOID
                    || oid == pg_sys::VARCHAROID
                    || oid == pg_sys::BPCHAROID
                    || oid == pg_sys::NAMEOID
            }
            Self::Uuid => oid == pg_sys::UUIDOID,
            Self::Date => oid == pg_sys::DATEOID,
            Self::TimeMicros => oid == pg_sys::TIMEOID,
            Self::TimestampMicros => oid == pg_sys::TIMESTAMPTZOID,
            Self::LocalTimestampMicros => oid == pg_sys::TIMESTAMPOID,
            Self::Decimal(_) => oid == pg_sys::NUMERICOID,
        }
    }

    fn encode_into(
        self,
        output: &mut Value,
        cell: &Cell,
    ) -> Result<(), ConnectorError> {
        match (self, cell) {
            (Self::Boolean, Cell::Bool(value)) => *output = Value::Boolean(*value),
            (Self::Int, Cell::I16(value)) => *output = Value::Int(i32::from(*value)),
            (Self::Int, Cell::I32(value)) => *output = Value::Int(*value),
            (Self::Long, Cell::I64(value)) => *output = Value::Long(*value),
            (Self::Float, Cell::F32(value)) => *output = Value::Float(*value),
            (Self::Double, Cell::F64(value)) => *output = Value::Double(*value),
            (Self::Bytes, Cell::Bytea(value)) => match output {
                Value::Bytes(output) => {
                    output.clear();
                    output.extend_from_slice(value);
                }
                output => *output = Value::Bytes(value.to_vec()),
            },
            (Self::String, Cell::String(value)) => match output {
                Value::String(output) => {
                    output.clear();
                    output.push_str(value);
                }
                output => *output = Value::String(value.clone()),
            },
            (Self::Uuid, Cell::Uuid(value)) => *output = Value::Uuid(*value),
            (Self::Date, Cell::Date(value)) => {
                let pg_days = value.to_pg_epoch_days();
                if pg_days == i32::MIN || pg_days == i32::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_days = pg_days
                    .checked_add(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::Date(unix_days);
            }
            (Self::TimeMicros, Cell::Time(value)) => {
                let micros = i64::from(*value);
                if !(0..USECS_PER_DAY).contains(&micros) {
                    return Err(self.value_out_of_range());
                }
                *output = Value::TimeMicros(micros);
            }
            (Self::TimestampMicros, Cell::Timestamptz(value)) => {
                let pg_micros = i64::from(*value);
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::TimestampMicros(unix_micros);
            }
            (Self::LocalTimestampMicros, Cell::Timestamp(value)) => {
                let pg_micros = i64::from(*value);
                if pg_micros == i64::MIN || pg_micros == i64::MAX {
                    return Err(self.value_out_of_range());
                }
                let unix_micros = pg_micros
                    .checked_add(PG_EPOCH_USECS_DIFF)
                    .ok_or_else(|| self.value_out_of_range())?;
                *output = Value::LocalTimestampMicros(unix_micros);
            }
            (Self::Decimal(codec), Cell::Numeric(value)) => {
                *output = Value::Decimal(Decimal::from(codec.encode(value)?.to_be_bytes()));
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "a PostgreSQL datum does not match its bound Avro field",
            ))?,
        }
        Ok(())
    }

    fn value_out_of_range(self) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Avro,
            format!("a {} value is outside the Avro epoch range", self.schema_name()),
        )
    }

    fn decimal(precision: u32, scale: u32) -> Result<Self, ConnectorError> {
        Decimal128NumericCodec::new(precision, scale)
            .map(Self::Decimal)
            .map_err(ConnectorError::from)
    }

    fn decimal_metadata(precision: usize, scale: usize) -> Result<Self, ConnectorError> {
        let precision = u32::try_from(precision).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal precision exceeds the supported range",
            )
        })?;
        let scale = u32::try_from(scale).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal scale exceeds the supported range",
            )
        })?;
        Self::decimal(precision, scale)
    }
}

pub(super) struct AvroWritePlan {
    schema: Schema,
    columns: Box<[Option<AvroValueKind>]>,
}

impl AvroWritePlan {
    pub(super) fn from_relation(relation: &RelationHandle<'_>) -> Result<Self, ConnectorError> {
        let attr_types = relation.attr_types();
        let mut columns = vec![None; relation.natts()];
        let mut fields = Vec::with_capacity(relation.natts());
        for (attno, name) in relation.live_columns() {
            let index = (attno - 1) as usize;
            let kind = AvroValueKind::from_oid(attr_types[index].0, attr_types[index].1)?;
            columns[index] = Some(kind);
            fields.push((name, kind));
        }
        Self::from_fields(fields, columns.into_boxed_slice())
    }

    pub(super) fn from_copy_columns(
        columns: impl Iterator<Item = Result<(&str, pg_sys::Oid, i32), ConnectorError>>,
        count: usize,
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::with_capacity(count);
        let mut kinds = Vec::with_capacity(count);
        for column in columns {
            let (name, oid, typmod) = column?;
            let kind = AvroValueKind::from_oid(oid, typmod)?;
            fields.push((name.to_owned(), kind));
            kinds.push(Some(kind));
        }
        Self::from_fields(fields, kinds.into_boxed_slice())
    }

    fn from_fields(
        fields: Vec<(String, AvroValueKind)>,
        columns: Box<[Option<AvroValueKind>]>,
    ) -> Result<Self, ConnectorError> {
        let fields = fields
            .into_iter()
            .map(|(name, kind)| {
                serde_json::json!({
                    "name": name,
                    "type": ["null", kind.schema_json()],
                    "default": null,
                })
            })
            .collect::<Vec<_>>();
        let schema = serde_json::json!({
            "type": "record",
            "name": "lakebase_record",
            "fields": fields,
        });
        let schema = Schema::parse_str(&schema.to_string())?;
        Ok(Self { schema, columns })
    }

    fn into_factory(self, compression: AvroWriteCompression) -> AvroEncoderFactory {
        AvroEncoderFactory {
            schema: self.schema,
            columns: self.columns,
            codec: AvroEncoderFactory::codec(compression),
        }
    }
}

struct AvroEncoderFactory {
    schema: Schema,
    columns: Box<[Option<AvroValueKind>]>,
    codec: Codec,
}

impl AvroEncoderFactory {
    fn codec(compression: AvroWriteCompression) -> Codec {
        match compression {
            AvroWriteCompression::Null => Codec::Null,
            AvroWriteCompression::Deflate => Codec::Deflate(Default::default()),
            AvroWriteCompression::Snappy => Codec::Snappy,
            AvroWriteCompression::Zstd => Codec::Zstandard(Default::default()),
        }
    }
}

impl ObjectFileEncoderFactory for AvroEncoderFactory {
    type Input = Row;
    type Encoder = AvroFileEncoder;

    fn file_suffix(&self) -> ObjectFileSuffix {
        AVRO_SUFFIX
    }

    fn open(&mut self, writer: StagedObjectWriter) -> Result<Self::Encoder, ConnectorError> {
        Ok(AvroFileEncoder::new(
            &self.schema,
            &self.columns,
            self.codec.clone(),
            writer,
        ))
    }
}

struct AvroFileEncoder {
    // `writer` is declared first so it releases its schema borrow before `schema` drops.
    writer: Writer<'static, StagedObjectWriter>,
    schema: Box<Schema>,
    columns: Box<[Option<AvroValueKind>]>,
    record: Value,
}

impl AvroFileEncoder {
    fn new(
        source_schema: &Schema,
        source_columns: &[Option<AvroValueKind>],
        codec: Codec,
        output: StagedObjectWriter,
    ) -> Self {
        let schema = Box::new(source_schema.clone());
        // SAFETY: `schema` is boxed, so its allocation remains stable when this
        // struct moves. `writer` is declared before `schema` and is consumed in
        // `finish`, therefore no writer reference can outlive the boxed schema.
        let schema_ref: &'static Schema = unsafe { &*(schema.as_ref() as *const Schema) };
        let writer = Writer::with_codec(schema_ref, output, codec);
        let Schema::Record(record) = schema_ref else {
            unreachable!("AvroWritePlan always constructs a record schema");
        };
        let fields = record
            .fields
            .iter()
            .map(|field| (field.name.clone(), Value::Union(0, Box::new(Value::Null))) )
            .collect();
        Self {
            writer,
            schema,
            columns: source_columns.to_vec().into_boxed_slice(),
            record: Value::Record(fields),
        }
    }
}

impl ObjectFileEncoder for AvroFileEncoder {
    type Input = Row;

    fn write(&mut self, row: &Self::Input) -> Result<FileWriteProgress, ConnectorError> {
        let Value::Record(fields) = &mut self.record else {
            unreachable!("AvroFileEncoder retains a record value");
        };
        debug_assert_eq!(
            fields.len(),
            self.columns.iter().filter(|column| column.is_some()).count(),
        );
        let mut output_fields = fields.iter_mut();
        for (cell, kind) in row.iter().zip(self.columns.iter()) {
            let Some(kind) = kind else {
                continue;
            };
            let (_, output) = output_fields
                .next()
                .expect("Avro row plan has one field per live relation attribute");
            let Value::Union(index, value) = output else {
                unreachable!("AvroFileEncoder initializes every field as a union");
            };
            match cell {
                None => {
                    *index = 0;
                    **value = Value::Null;
                }
                Some(cell) => {
                    *index = 1;
                    kind.encode_into(value, cell)?;
                }
            };
        }
        debug_assert!(output_fields.next().is_none());
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

pub(super) struct AvroObjectWriter {
    writer: Option<ObjectSetWriter<AvroEncoderFactory>>,
}

impl AvroObjectWriter {
    pub(super) fn new(
        output: ObjectOutput,
        plan: AvroWritePlan,
        compression: AvroWriteCompression,
    ) -> Self {
        Self {
            writer: Some(ObjectSetWriter::new(output, plan.into_factory(compression))),
        }
    }

    pub(super) fn write_row(&mut self, row: &Row) -> Result<(), ConnectorError> {
        self.writer
            .as_mut()
            .expect("the Avro writer is not used after finish")
            .write(row)
    }

    pub(super) fn finish(&mut self, emit_empty: bool) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        writer.finish(if emit_empty {
            EmptyOutputPolicy::EmitFile
        } else {
            EmptyOutputPolicy::Skip
        })
    }
}

pub(super) struct AvroWriteState {
    columns: Box<[AvroInsertColumn]>,
    row: Row,
    writer: AvroObjectWriter,
}

#[derive(Clone, Copy)]
struct AvroInsertColumn {
    source: SlotDatumIndex,
    output: usize,
    codec: ColumnDatumCodec,
}

impl AvroWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
        compression: AvroWriteCompression,
    ) -> Result<Self, ConnectorError> {
        let plan = AvroWritePlan::from_relation(relation)?;
        let attr_types = relation.attr_types();
        let columns = relation
            .live_columns()
            .map(|(attno, _)| {
                let index = (attno - 1) as usize;
                let source = SlotDatumIndex::new(index, relation.natts())
                    .expect("a live relation attribute is within its tuple width");
                let target = ColumnDatumTarget::from_oid(attr_types[index].0);
                Ok(AvroInsertColumn {
                    source,
                    output: index,
                    codec: ColumnDatumCodec::bind(target)?,
                })
            })
            .collect::<Result<Box<[_]>, ConnectorError>>()?;
        Ok(Self {
            columns,
            row: Row::with_capacity(relation.natts()),
            writer: AvroObjectWriter::new(output, plan, compression),
        })
    }
}

impl FormatWriteState for AvroWriteState {
    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        let datums = slot.tuple_row().datums();
        debug_assert_eq!(datums.len(), self.row.len());
        for column in self.columns.iter().copied() {
            // SAFETY: `source` was validated from this relation's live
            // descriptor at Begin, the executor callback uses that same
            // relation layout, and `row` was allocated to its full width.
            let (datum, is_null) = unsafe { datums.datum_at_bound(column.source) };
            let cell = unsafe { column.codec.datum_to_cell(datum, is_null) }?;
            unsafe { self.row.set_cell_at_bound(column.output, cell) };
        }
        self.writer.write_row(&self.row)?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Avro))
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Avro))
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.writer.finish(false)
    }
}
