//! Event-driven transport for parallel query offload: the byte streams upstream's
//! gRPC `Worker` and `ChannelResolver` run on, backed by PostgreSQL dynamic shared
//! memory, plus the wakeups that make each participant's runtime parking sleepable.

mod reactor;
mod region;
mod resolver;
mod ring;
mod stream;
mod watch;

pub(in crate::datafusion) use reactor::{
    ParallelRuntime, ReactorAttachment, WakerSet,
};
pub(in crate::datafusion) use region::{TransportLayout, TransportRegion};
pub(in crate::datafusion) use resolver::{
    ParallelChannelResolver, ParallelWorkerResolver,
};
pub(in crate::datafusion) use stream::{ServerStreams, StreamContext};
pub(in crate::datafusion) use watch::{RunRole, RunSignal, RunWatch};
