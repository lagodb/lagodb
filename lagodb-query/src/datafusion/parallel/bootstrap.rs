//! Backend-independent inventory sent once to each attached participant.

use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub(super) struct ParallelBootstrap {
    #[prost(message, repeated, tag = "1")]
    pub sources: Vec<ParallelSource>,
    #[prost(uint64, tag = "2")]
    pub work_mem_bytes: u64,
    #[prost(uint32, tag = "3")]
    pub maximum_batch_rows: u32,
    #[prost(bool, tag = "4")]
    pub collect_metrics: bool,
    #[prost(uint64, tag = "5")]
    pub source_inventory_bytes: u64,
    #[prost(uint64, tag = "6")]
    pub hash_memory_bytes: u64,
    #[prost(bool, tag = "7")]
    pub spill_enabled: bool,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ParallelSource {
    #[prost(int32, tag = "1")]
    pub route_kind: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub route_name: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub payload_offset: u64,
    #[prost(uint64, tag = "4")]
    pub payload_len: u64,
}
