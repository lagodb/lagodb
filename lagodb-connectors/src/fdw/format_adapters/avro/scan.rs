//! FDW planning and streaming scans of Avro object containers.

use std::ffi::CStr;
use std::io::BufReader;

use lagodb_core::fdw::{
    ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::fdw::format_adapters::FormatScanState;
use crate::format::FormatKind;
use crate::format::avro::{AvroObjectHeader, AvroObjectReader, AvroReadColumn};
use crate::storage::{InputFile, InputFiles};

struct ScanColumn {
    reader: AvroReadColumn,
    output: ScanOutputColumn,
}

pub(super) struct AvroScanState {
    files: InputFiles,
    /// `None` only when the prefix has no objects.
    reader: Option<AvroObjectReader<BufReader<InputFile>>>,
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
                columns: Box::new([]),
            });
        };
        let header = AvroObjectHeader::read(BufReader::new(first?))?;
        let record = header.record()?;
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
        let reader = AvroObjectReader::new(
            header,
            columns.iter().map(|column| column.reader.source()),
        )?;
        Ok(Self {
            files,
            reader: Some(reader),
            columns,
        })
    }

    /// Opens the next object under the prefix. Returns `false` when none remain.
    fn open_next_object(
        files: &mut InputFiles,
        reader: &mut AvroObjectReader<BufReader<InputFile>>,
    ) -> Result<bool, ConnectorError> {
        let Some(file) = files.next() else {
            return Ok(false);
        };
        reader.open_next(AvroObjectHeader::read(BufReader::new(file?))?)?;
        Ok(true)
    }
}

impl FormatScanState for AvroScanState {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(false);
        };
        loop {
            if let Some(row) = reader.next_row()? {
                // SAFETY: each ScanColumn was created from this scan's output
                // layout and all by-reference Datum values are allocated in the
                // executor tuple context by its bound codec.
                let mut writer = unsafe { output.datum_writer() };
                for column in self.columns.iter() {
                    // SAFETY: the executor tuple context is current, and the
                    // reader's plan bound every column's source field.
                    let value = unsafe { column.reader.datum(&row) }?;
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
            if !Self::open_next_object(&mut self.files, reader)? {
                return Ok(false);
            }
        }
    }

    fn rescan(
        &mut self,
        _context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ConnectorError> {
        self.files.reset();
        if let Some(reader) = self.reader.as_mut() {
            reader.close();
            Self::open_next_object(&mut self.files, reader)?;
        }
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.reader = None;
        Ok(())
    }
}
