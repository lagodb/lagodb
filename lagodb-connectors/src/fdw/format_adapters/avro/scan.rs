//! FDW planning and streaming scans of Avro object containers.

use std::ffi::CStr;
use std::io::BufReader;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use lagodb_core::fdw::{
    ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::fdw::format_adapters::FormatScanState;
use crate::format::FormatKind;
use crate::format::avro::AvroReadColumn;
use crate::storage::{InputFile, InputFiles};

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
        let attributes = context.relation.tuple_desc_handle().attributes();
        for attribute in attributes
            .iter()
            .filter(|attribute| !attribute.attisdropped)
        {
            // SAFETY: NameData is NUL-terminated and owned by the live
            // relation descriptor for this begin callback.
            let name = unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) };
            name.to_str().map_err(|_| {
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
        let columns = context
            .output_layout
            .columns()
            .iter()
            .copied()
            .map(|output| {
                let attribute = &attributes[(output.attno() - 1) as usize];
                // SAFETY: as above; scan outputs name live attributes only.
                let name = unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) }
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
                        attribute.atttypid,
                        attribute.atttypmod,
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
