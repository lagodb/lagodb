//! Physical extension codec shared by leader dispatch and worker decode.

use std::fmt;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::DistributedConfig;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{self, proto_error};
use prost::Message;

use super::catalog::WorkerSourceCatalog;
use super::scan_exec::ParallelTableScanExec;
use crate::datafusion::integer_abs::PgIntegerAbsUdf;
use crate::datafusion::join_planning::AdaptiveHashJoinExec;
use crate::datafusion::{real_sum, udaf_by_name};
use lagodb_core::query_contract::ScanId;

#[derive(Clone, PartialEq, Message)]
struct ParallelScanProto {
    #[prost(uint64, tag = "1")]
    scan: u64,
    #[prost(message, optional, tag = "2")]
    schema: Option<protobuf::Schema>,
    #[prost(message, repeated, tag = "3")]
    assignments: Vec<WorkAssignmentProto>,
    #[prost(uint64, tag = "4")]
    maximum_batch_rows: u64,
}

#[derive(Clone, PartialEq, Message)]
struct WorkAssignmentProto {
    #[prost(uint32, repeated, tag = "1")]
    work_ids: Vec<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct AdaptiveHashJoinProto {
    #[prost(uint64, tag = "1")]
    hash_memory_bytes: u64,
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

impl AdaptiveHashJoinProto {
    fn decode_plan(
        buf: &[u8],
        join: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proto = Self::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode adaptive hash join: {error}"))
        })?;
        // Leader and workers share this build and platform; the u64 field
        // carries a `usize` written by `encode_plan`.
        AdaptiveHashJoinExec::try_new(
            Arc::clone(join),
            proto.hash_memory_bytes as usize,
        )
        .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
    }

    fn encode_plan(join: &AdaptiveHashJoinExec, buf: &mut Vec<u8>) -> Result<()> {
        Self {
            hash_memory_bytes: join.hash_memory_bytes() as u64,
        }
        .encode(buf)
        .map_err(|error| {
            proto_error(format!("failed to encode adaptive hash join: {error}"))
        })
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
}

impl PhysicalExtensionCodec for LagoPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The two nodes this codec owns differ by arity: the scan is a leaf and
        // the adaptive join has its hash join as the only child.
        match inputs {
            [] => {}
            [join] => return AdaptiveHashJoinProto::decode_plan(buf, join),
            _ => {
                return Err(proto_error(format!(
                    "LagoDB physical codec received {} inputs",
                    inputs.len(),
                )));
            }
        }
        let proto = ParallelScanProto::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode parallel table scan: {error}"))
        })?;
        // The u64 field carries a same-platform `usize` written by
        // `try_encode`.
        let scan = ScanId::from_index(proto.scan as usize);
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
        if let Some(join) = node.downcast_ref::<AdaptiveHashJoinExec>() {
            return AdaptiveHashJoinProto::encode_plan(join, buf);
        }
        let Some(scan) = node.downcast_ref::<ParallelTableScanExec>() else {
            return Err(DataFusionError::NotImplemented(format!(
                "LagoDB physical codec does not encode {}",
                node.name(),
            )));
        };
        let schema =
            protobuf::Schema::try_from(scan.schema().as_ref()).map_err(|error| {
                proto_error(format!("failed to encode parallel scan schema: {error}"))
            })?;
        ParallelScanProto {
            scan: scan.scan().index() as u64,
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
        if name == real_sum::NAME && buf.is_empty() {
            return Ok(real_sum::result());
        }
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
        if node.name() == real_sum::NAME {
            return Ok(());
        }
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
        udaf_by_name(name).ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "LagoDB UDAF {name:?} has no physical codec",
            ))
        })
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        if udaf_by_name(node.name()).is_some() {
            return Ok(());
        }
        Err(DataFusionError::NotImplemented(format!(
            "LagoDB UDAF codec does not encode {}",
            node.name(),
        )))
    }
}
