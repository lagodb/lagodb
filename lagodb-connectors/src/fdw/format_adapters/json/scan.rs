//! Native NDJSON Foreign Table scan.

use std::ffi::CStr;

use lagodb_core::fdw::{
    ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::fdw::format_adapters::FormatScanState;
use crate::format::json::{JsonColumnPlan, JsonRecordDecoder, JsonRecordStream};
use crate::format::{FormatKind, StreamCompressionOptions};
use crate::gucs::ReadConfig;
use crate::storage::InputFiles;

pub(super) struct JsonScanState {
    stream: JsonRecordStream,
    plan: JsonColumnPlan,
    decoder: JsonRecordDecoder,
    outputs: Box<[ScanOutputColumn]>,
}

impl JsonScanState {
    pub(super) fn begin(
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
        compression: StreamCompressionOptions,
    ) -> Result<Self, ConnectorError> {
        let attributes = context.relation.tuple_desc_handle().attributes();
        let outputs = context.output_layout.columns().to_vec().into_boxed_slice();
        // The relation-shaped scan outputs every live attribute.
        let fields = outputs
            .iter()
            .map(|output| {
                let attribute = &attributes[(output.attno() - 1) as usize];
                // SAFETY: NameData is NUL-terminated and owned by the live
                // relation descriptor for this begin callback.
                let name = unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) };
                let name = name.to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Json,
                        "PostgreSQL column names must be valid UTF-8 for JSON",
                    )
                })?;
                Ok((name, attribute.atttypid, attribute.atttypmod))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let plan = JsonColumnPlan::bind(fields)?;
        let max_record_bytes = ReadConfig::from_guc().json_max_record_bytes();
        Ok(Self {
            stream: JsonRecordStream::new(files, compression, max_record_bytes),
            decoder: JsonRecordDecoder::new(plan.len()),
            plan,
            outputs,
        })
    }
}

impl FormatScanState for JsonScanState {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        let Some((logical_line, record)) = self.stream.next_record()? else {
            return Ok(false);
        };
        self.decoder.decode(&self.plan, record, logical_line)?;
        // SAFETY: outputs were copied from this scan's Begin-time layout and
        // every destination is written exactly once.
        let mut writer = unsafe { output.datum_writer() };
        for (index, (column, destination)) in self
            .plan
            .columns()
            .iter()
            .zip(self.outputs.iter().copied())
            .enumerate()
        {
            // SAFETY: record was decoded above with this plan, and
            // ForeignNext selected the per-tuple context for this callback.
            // PG input errors unwind to the core scan callback boundary.
            let datum = unsafe {
                self.decoder
                    .datum(record, column, index, logical_line, None)
            }?;
            // SAFETY: the input codec was bound to this destination's type.
            unsafe {
                writer.write(
                    destination,
                    datum.unwrap_or(pg_sys::Datum::from(0)),
                    datum.is_none(),
                );
            }
        }
        Ok(true)
    }

    fn rescan(
        &mut self,
        _context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ConnectorError> {
        self.stream.reset();
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.stream.close();
        Ok(())
    }
}
