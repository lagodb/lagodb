//! Physical extension codec shared by leader dispatch and worker decode.

use std::fmt;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DispatchPlanSource, DistributedCodec, DistributedConfig, TaskKey,
};
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, DeduplicatingProtoConverter,
    PhysicalExtensionCodec, PhysicalPlanNodeExt, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{self, PhysicalPlanNode, proto_error};
use prost::Message;

use super::catalog::WorkerSourceCatalog;
use super::scan_exec::ParallelTableScanExec;
use crate::datafusion::integer_abs::PgIntegerAbsUdf;
use crate::datafusion::numeric_aggregate;
use lagodb_core::query_contract::ScanId;

const SOURCE_CODEC_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Message)]
struct ParallelScanProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    scan: u64,
    #[prost(message, optional, tag = "3")]
    schema: Option<protobuf::Schema>,
    #[prost(message, repeated, tag = "4")]
    assignments: Vec<WorkAssignmentProto>,
    #[prost(uint64, tag = "5")]
    maximum_batch_rows: u64,
}

#[derive(Clone, PartialEq, Message)]
struct WorkAssignmentProto {
    #[prost(uint32, repeated, tag = "1")]
    work_ids: Vec<u32>,
}

/// User codec installed at position one after the fork's distributed codec.
/// The position is stable in both dispatch encoding and worker decoding.
#[derive(Clone)]
pub(in crate::datafusion) struct LagoPhysicalCodec {
    sources: Option<WorkerSourceCatalog>,
}

impl fmt::Debug for LagoPhysicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LagoPhysicalCodec")
            .field("worker_sources", &self.sources.is_some())
            .finish()
    }
}

impl LagoPhysicalCodec {
    pub(super) const fn leader() -> Self {
        Self { sources: None }
    }

    pub(super) fn worker(sources: WorkerSourceCatalog) -> Self {
        Self {
            sources: Some(sources),
        }
    }

    pub(super) fn combined(self) -> ComposedPhysicalExtensionCodec {
        ComposedPhysicalExtensionCodec::new(vec![
            Arc::new(DistributedCodecHostingLagoFunctions(DistributedCodec {})),
            Arc::new(self),
        ])
    }
}

impl PhysicalExtensionCodec for LagoPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return Err(proto_error(format!(
                "ParallelTableScanExec is a leaf but received {} inputs",
                inputs.len(),
            )));
        }
        let proto = ParallelScanProto::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode parallel table scan: {error}"))
        })?;
        if proto.version != SOURCE_CODEC_VERSION {
            return Err(proto_error(format!(
                "unsupported parallel table scan codec version {}",
                proto.version,
            )));
        }
        let scan = usize::try_from(proto.scan)
            .map(ScanId::from_index)
            .map_err(|_| {
                proto_error(format!(
                    "parallel source id {} exceeds this platform",
                    proto.scan
                ))
            })?;
        let schema = proto
            .schema
            .as_ref()
            .ok_or_else(|| proto_error("parallel table scan is missing its schema"))
            .and_then(|schema| {
                Schema::try_from(schema).map(Arc::new).map_err(|error| {
                    proto_error(format!("invalid parallel scan schema: {error}"))
                })
            })?;
        let assignments: Arc<[Arc<[u32]>]> = proto
            .assignments
            .into_iter()
            .map(|assignment| Arc::from(assignment.work_ids))
            .collect::<Vec<_>>()
            .into();
        let sources = self.sources.as_ref().ok_or_else(|| {
            proto_error("parallel table scan decode requires worker-local sources")
        })?;
        ParallelTableScanExec::worker(
            scan,
            schema,
            assignments,
            proto.maximum_batch_rows,
            sources.get(scan)?,
            sources.host(),
            DistributedConfig::from_config_options(ctx.session_config().options())?
                .collect_metrics,
        )
        .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(scan) = node.downcast_ref::<ParallelTableScanExec>() else {
            return Err(DataFusionError::NotImplemented(format!(
                "LagoDB physical codec does not encode {}",
                node.name(),
            )));
        };
        let scan_id = u64::try_from(scan.scan().index()).map_err(|_| {
            DataFusionError::Internal("parallel source id exceeds u64".to_owned())
        })?;
        let schema =
            protobuf::Schema::try_from(scan.schema().as_ref()).map_err(|error| {
                proto_error(format!("failed to encode parallel scan schema: {error}"))
            })?;
        ParallelScanProto {
            version: SOURCE_CODEC_VERSION,
            scan: scan_id,
            schema: Some(schema),
            assignments: scan
                .assignments()
                .iter()
                .map(|assignment| WorkAssignmentProto {
                    work_ids: assignment.to_vec(),
                })
                .collect(),
            maximum_batch_rows: scan.maximum_batch_rows(),
        }
        .encode(buf)
        .map_err(|error| {
            proto_error(format!("failed to encode parallel table scan: {error}"))
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name != "abs" || buf.len() != 1 {
            return Err(DataFusionError::NotImplemented(format!(
                "LagoDB UDF {name:?} has no physical codec",
            )));
        }
        PgIntegerAbsUdf::from_codec_tag(buf[0])
            .map(Arc::new)
            .ok_or_else(|| {
                proto_error(format!("invalid PostgreSQL ABS codec tag {}", buf[0]))
            })
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let Some(tag) = PgIntegerAbsUdf::codec_tag(node) else {
            return Err(DataFusionError::NotImplemented(format!(
                "LagoDB UDF codec does not encode {}",
                node.name(),
            )));
        };
        buf.push(tag);
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, _buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        match name {
            numeric_aggregate::SUM_NAME => Ok(numeric_aggregate::sum()),
            numeric_aggregate::AVG_NAME => Ok(numeric_aggregate::avg()),
            _ => Err(DataFusionError::NotImplemented(format!(
                "LagoDB UDAF {name:?} has no physical codec",
            ))),
        }
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        match node.name() {
            numeric_aggregate::SUM_NAME | numeric_aggregate::AVG_NAME => Ok(()),
            name => Err(DataFusionError::NotImplemented(format!(
                "LagoDB UDAF codec does not encode {name}",
            ))),
        }
    }
}

/// The distributed codec must remain position zero for ordinary built-ins to
/// keep name-only encoding. It only declines LagoDB functions so position one
/// can carry their definitions.
#[derive(Debug)]
struct DistributedCodecHostingLagoFunctions(DistributedCodec);

impl PhysicalExtensionCodec for DistributedCodecHostingLagoFunctions {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        self.0.try_encode(node, buf, proto_converter)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        if PgIntegerAbsUdf::codec_tag(node).is_some() {
            return Err(DataFusionError::NotImplemented(
                "PostgreSQL ABS is encoded by the LagoDB codec".to_owned(),
            ));
        }
        Ok(())
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        if matches!(
            node.name(),
            numeric_aggregate::SUM_NAME | numeric_aggregate::AVG_NAME
        ) {
            return Err(DataFusionError::NotImplemented(
                "PostgreSQL NUMERIC aggregate is encoded by the LagoDB codec"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Per-query dispatcher for ready-to-run specialized stage plans.
#[derive(Default)]
pub(in crate::datafusion) struct LagoStagePlanDispatch;

impl DispatchPlanSource for LagoStagePlanDispatch {
    fn dispatch_plan_proto(
        &self,
        _task: &TaskKey,
        specialized: &Arc<dyn ExecutionPlan>,
    ) -> Option<Result<Vec<u8>>> {
        // The coordinator calls dispatch once for the task-specialized plan.
        // Do not retain another copy of each plan or key a rescan's bytes only
        // by stage/task IDs, which are reused by a new isolated run.
        Some(
            PhysicalPlanNode::try_from_physical_plan_with_converter(
                Arc::clone(specialized),
                &LagoPhysicalCodec::leader().combined(),
                &DeduplicatingProtoConverter::default(),
            )
            .map(|node| node.encode_to_vec()),
        )
    }
}
