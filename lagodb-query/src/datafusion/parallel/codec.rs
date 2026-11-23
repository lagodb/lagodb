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
use crate::HashJoinFallback;
use crate::datafusion::integer_abs::PgIntegerAbsUdf;
use crate::datafusion::join_planning::{
    AdaptiveHashJoinExec, JoinMemoryPolicy, NullSensitiveAntiJoinExec,
};
use crate::datafusion::pg_arithmetic::{
    PgArithmeticOperator, PgIntegerArithmeticUdf,
};
use crate::datafusion::{real_sum, udaf_by_name};
use crate::plan::NullSensitiveAntiMode;
use lagodb_core::query_contract::ScanId;

/// Every plan node this codec owns, tagged by kind: the adaptive join and the
/// NULL-sensitive anti join both have one child.
#[derive(Clone, PartialEq, Message)]
struct LagoPlanProto {
    #[prost(oneof = "lago_plan::Node", tags = "1, 2, 3")]
    node: Option<lago_plan::Node>,
}

mod lago_plan {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Node {
        #[prost(message, tag = "1")]
        ParallelScan(super::ParallelScanProto),
        #[prost(message, tag = "2")]
        AdaptiveHashJoin(super::AdaptiveHashJoinProto),
        #[prost(message, tag = "3")]
        NullSensitiveAntiJoin(super::NullSensitiveAntiJoinProto),
    }
}

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
    #[prost(uint64, tag = "2")]
    work_mem_bytes: u64,
    #[prost(bool, tag = "3")]
    hybrid_hash: bool,
}

#[derive(Clone, PartialEq, Message)]
struct NullSensitiveAntiJoinProto {
    /// `x IS NULL OR x NOT IN`, or else `x NOT IN`.
    #[prost(bool, tag = "1")]
    null_or_not_in: bool,
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

impl ParallelScanProto {
    fn encode_plan(scan: &ParallelTableScanExec) -> Result<Self> {
        let schema =
            protobuf::Schema::try_from(scan.schema().as_ref()).map_err(|error| {
                proto_error(format!("failed to encode parallel scan schema: {error}"))
            })?;
        Ok(Self {
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
        })
    }
}

impl AdaptiveHashJoinProto {
    fn decode_plan(
        self,
        join: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Leader and workers share this build and platform; the u64 fields
        // carry the `usize` values written by `encode_plan`.
        let policy = JoinMemoryPolicy {
            hash_memory_bytes: self.hash_memory_bytes as usize,
            work_mem_bytes: self.work_mem_bytes as usize,
            fallback: HashJoinFallback::from_hybrid_hash(self.hybrid_hash),
        };
        AdaptiveHashJoinExec::try_new(Arc::clone(join), policy)
            .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
    }

    fn encode_plan(join: &AdaptiveHashJoinExec) -> Self {
        let policy = join.policy();
        Self {
            hash_memory_bytes: policy.hash_memory_bytes as u64,
            work_mem_bytes: policy.work_mem_bytes as u64,
            hybrid_hash: policy.fallback.is_hybrid_hash(),
        }
    }
}

impl NullSensitiveAntiJoinProto {
    fn decode_plan(self, join: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mode = if self.null_or_not_in {
            NullSensitiveAntiMode::NullOrNotIn
        } else {
            NullSensitiveAntiMode::NotIn
        };
        Arc::new(NullSensitiveAntiJoinExec::new(mode, Arc::clone(join)))
    }

    fn encode_plan(join: &NullSensitiveAntiJoinExec) -> Self {
        Self {
            null_or_not_in: join.mode() == NullSensitiveAntiMode::NullOrNotIn,
        }
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

    /// The u64 fields carry same-platform `usize` values written by
    /// `ParallelScanProto::encode_plan`.
    fn decode_scan(
        &self,
        proto: ParallelScanProto,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
}

impl PhysicalExtensionCodec for LagoPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proto = LagoPlanProto::decode(buf).map_err(|error| {
            proto_error(format!("failed to decode LagoDB plan node: {error}"))
        })?;
        match (proto.node, inputs) {
            (Some(lago_plan::Node::ParallelScan(scan)), []) => {
                self.decode_scan(scan, ctx)
            }
            (Some(lago_plan::Node::AdaptiveHashJoin(join)), [input]) => {
                join.decode_plan(input)
            }
            (Some(lago_plan::Node::NullSensitiveAntiJoin(join)), [input]) => {
                Ok(join.decode_plan(input))
            }
            (None, _) => Err(proto_error("LagoDB plan node is empty")),
            _ => Err(proto_error(format!(
                "LagoDB plan node does not take {} inputs",
                inputs.len(),
            ))),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let node = if let Some(join) = node.downcast_ref::<AdaptiveHashJoinExec>() {
            lago_plan::Node::AdaptiveHashJoin(AdaptiveHashJoinProto::encode_plan(
                join,
            ))
        } else if let Some(join) = node.downcast_ref::<NullSensitiveAntiJoinExec>() {
            lago_plan::Node::NullSensitiveAntiJoin(
                NullSensitiveAntiJoinProto::encode_plan(join),
            )
        } else if let Some(scan) = node.downcast_ref::<ParallelTableScanExec>() {
            lago_plan::Node::ParallelScan(ParallelScanProto::encode_plan(scan)?)
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "LagoDB physical codec does not encode {}",
                node.name(),
            )));
        };
        LagoPlanProto { node: Some(node) }
            .encode(buf)
            .map_err(|error| {
                proto_error(format!("failed to encode LagoDB plan node: {error}"))
            })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name == real_sum::NAME && buf.is_empty() {
            return Ok(real_sum::result());
        }
        // NotImplemented hands a UDF this codec does not own to the next user
        // codec. The integer UDFs carry their PostgreSQL integer type as one
        // tag byte; the name identifies the function.
        let not_implemented = || {
            DataFusionError::NotImplemented(format!(
                "LagoDB UDF {name:?} has no physical codec",
            ))
        };
        let &[tag] = buf else {
            return Err(not_implemented());
        };
        let udf = match (name, PgArithmeticOperator::from_integer_name(name)) {
            ("abs", _) => PgIntegerAbsUdf::from_codec_tag(tag),
            (_, Some(operator)) => {
                PgIntegerArithmeticUdf::from_codec_tag(operator, tag)
            }
            (_, None) => return Err(not_implemented()),
        };
        udf.map(Arc::new).ok_or_else(|| {
            proto_error(format!("invalid LagoDB UDF {name:?} codec tag {tag}"))
        })
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if node.name() == real_sum::NAME {
            return Ok(());
        }
        let Some(tag) = PgIntegerAbsUdf::codec_tag(node)
            .or_else(|| PgIntegerArithmeticUdf::codec_tag(node))
        else {
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
