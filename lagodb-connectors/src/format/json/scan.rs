//! Native NDJSON Foreign Table scan.

use std::ffi::CStr;
use std::panic::AssertUnwindSafe;

use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignPathBuilder, ForeignPathContext, ForeignPathKeys,
    ForeignPathSpec, ForeignPlanContext, ForeignPlanSpec, ForeignRelSize,
    ForeignRelSizeContext, ReScanForeignScanContext, ScanOutputColumn,
    ScanProjectionPolicy, ScanSlotWriter,
};
use pgrx::{PgTryBuilder, pg_sys};

use crate::error::ConnectorError;
use crate::fdw::Lakebase;
use crate::gucs::ReadConfig;
use crate::storage::ObjectFiles;

use super::record::{JsonColumnPlan, JsonInputValue, JsonRecordDecoder};
use super::stream::JsonRecordStream;
use crate::format::{
    FormatKind, FormatScanPlanner, FormatScanPrivate, FormatScanState,
    StreamCompression,
};

const DEFAULT_ESTIMATED_ROWS: f64 = 1_000.0;
const DEFAULT_ESTIMATED_WIDTH: i32 = 32;

pub(super) struct JsonScanPlanner;

impl FormatScanPlanner for JsonScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Ok(ForeignRelSize::new(
            DEFAULT_ESTIMATED_ROWS,
            DEFAULT_ESTIMATED_WIDTH,
        ))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        paths.push(ForeignPathSpec::new(
            DEFAULT_ESTIMATED_ROWS,
            0.0,
            DEFAULT_ESTIMATED_ROWS,
            FormatScanPrivate::new(FormatKind::Json),
        ));
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
        let mut plan = ForeignPlanSpec::new(context.path_private().to_owned());
        plan.projection_policy = ScanProjectionPolicy::RequireRelationShape;
        Ok(plan)
    }
}

pub(super) struct JsonScanState {
    stream: JsonRecordStream,
    plan: JsonColumnPlan,
    decoder: JsonRecordDecoder,
    outputs: Box<[ScanOutputColumn]>,
    c_string: Vec<u8>,
}

impl JsonScanState {
    pub(super) fn begin(
        context: BeginForeignScanContext<'_, Lakebase>,
        files: ObjectFiles,
        compression: StreamCompression,
    ) -> Result<Self, ConnectorError> {
        let live = context.relation.live_columns();
        let mut columns_by_attno = vec![None; context.relation.natts()];
        for column in live.iter() {
            column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Json,
                    "PostgreSQL column names must be valid UTF-8 for JSON",
                )
            })?;
            columns_by_attno[(column.attno() - 1) as usize] = Some(column);
        }
        let outputs = context.output_layout.columns().to_vec().into_boxed_slice();
        let fields = outputs.iter().map(|output| {
            let index = (output.attno() - 1) as usize;
            let column = columns_by_attno[index]
                .expect("a scan output column always names a live attribute");
            let name = column
                .name()
                .to_str()
                .expect("all live JSON column names were validated as UTF-8");
            (name, column.type_oid(), column.type_mod())
        });
        let plan = JsonColumnPlan::bind(fields)?;
        let max_record_bytes = ReadConfig::from_guc().json_max_record_bytes();
        Ok(Self {
            stream: JsonRecordStream::new(files, compression, max_record_bytes),
            decoder: JsonRecordDecoder::new(plan.len()),
            plan,
            outputs,
            c_string: Vec::new(),
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
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            // SAFETY: outputs were copied from this scan's Begin-time
            // layout and every destination is written exactly once.
            let mut writer = unsafe { output.datum_writer() };
            for (index, (column, destination)) in self
                .plan
                .columns()
                .iter()
                .zip(self.outputs.iter().copied())
                .enumerate()
            {
                let value =
                    self.decoder.value(record, column, index, logical_line)?;
                match value {
                    JsonInputValue::Null => unsafe {
                        writer.write(destination, pg_sys::Datum::from(0), true);
                    },
                    JsonInputValue::Bytes(value) => {
                        self.c_string.clear();
                        self.c_string.extend_from_slice(value);
                        self.c_string.push(0);
                        // SAFETY: serde_json validation excludes literal
                        // NUL bytes and this state appended one terminator.
                        let value = unsafe {
                            CStr::from_bytes_with_nul_unchecked(&self.c_string)
                        };
                        // SAFETY: the input plan was bound to this exact
                        // destination OID and typmod at Begin.
                        let datum = unsafe { column.input_datum(value) };
                        unsafe { writer.write(destination, datum, false) };
                    }
                    JsonInputValue::CStr(value) => {
                        // SAFETY: the decoder's reusable buffer is valid
                        // through this synchronous input-function call.
                        let datum = unsafe { column.input_datum(value) };
                        unsafe { writer.write(destination, datum, false) };
                    }
                }
            }
            Ok::<(), ConnectorError>(())
        }))
        .catch_others(|error| {
            Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
        })
        .execute();
        result?;
        Ok(true)
    }

    fn rescan(
        &mut self,
        _context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ConnectorError> {
        self.stream.reset();
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.stream.close();
        Ok(())
    }
}
