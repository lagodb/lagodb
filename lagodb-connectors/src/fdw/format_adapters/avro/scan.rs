//! FDW planning and streaming scans of Avro object containers.

use std::io::BufReader;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use lagodb_core::fdw::{
    ForeignPathBuilder, ForeignPathContext, ForeignPathKeys, ForeignPathSpec,
    ForeignPlanContext, ForeignPlanSpec, ForeignRelSize, ForeignRelSizeContext,
    ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::fdw::format_adapters::{
    FormatScanPlanner, FormatScanPrivate, FormatScanState,
};
use crate::format::FormatKind;
use crate::format::avro::AvroReadColumn;
use crate::storage::{InputFile, InputFiles};

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
        context: &ForeignPlanContext<'_, LagodbConnectors>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        Ok(ForeignPlanSpec::new(context.path_private().to_owned()))
    }
}

struct ScanColumn {
    reader: AvroReadColumn,
    output: ScanOutputColumn,
}

pub(super) struct AvroScanState {
    files: InputFiles,
    reader: Option<Reader<'static, BufReader<InputFile>>>,
    schema: Option<Schema>,
    columns: Box<[ScanColumn]>,
}

impl AvroScanState {
    pub(super) fn begin(
        context: StartForeignScanContext<'_, LagodbConnectors>,
        mut files: InputFiles,
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
        let reader = Reader::new(BufReader::new(first?))?;
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
                            format!(
                                "column {name:?} is missing from the Avro schema"
                            ),
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
        let reader = Reader::new(BufReader::new(file?))?;
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
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(value) = reader.next()
            {
                let value = value?;
                let Value::Record(fields) = value else {
                    unreachable!(
                        "a record writer schema always decodes to a record value"
                    );
                };
                // SAFETY: each ScanColumn was created from this scan's output
                // layout and all by-reference Datum values are allocated in the
                // executor tuple context by its bound codec.
                let mut writer = unsafe { output.datum_writer() };
                for column in self.columns.iter() {
                    // SAFETY: `source` was resolved against this exact writer
                    // schema at Begin; apache-avro decodes a record with one
                    // value per writer-schema field in that order.
                    let source =
                        unsafe { fields.get_unchecked(column.reader.source()) };
                    let value = unsafe { column.reader.datum(&source.1) }?;
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
        _context: ReScanForeignScanContext<'_, LagodbConnectors>,
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
