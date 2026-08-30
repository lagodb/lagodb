//! Runtime-owned registry for utility consumers.
//!
//! The ProcessUtility hook owns parent fallback and command lifecycle; this
//! module owns only the cold-path consumer directory and selection. The
//! selected callback still executes through the descriptor supplied by the
//! runtime ABI.

use crate::descriptor_directory::{
    DescriptorDirectory, DescriptorNode, DescriptorSnapshot,
};
use lagodb_core::hooks::HookError;
use lagodb_core::runtime_api::{
    UTILITY_ROUTE_CONSUMED, UTILITY_ROUTE_PASS_THROUGH, UtilityConsumerDescriptor,
};
use pgrx::pg_sys;

use crate::process_utility::ProcessUtilityArgs;

type UtilityConsumerDirectory = DescriptorDirectory<UtilityConsumerDescriptor>;

pub(crate) struct PreparedUtilityConsumers {
    // Each node is allocated before the runtime registration transaction is
    // committed. The box keeps its address stable while the directory stores
    // a raw backend-lifetime pointer; commit therefore cannot allocate or
    // publish only part of this prepared batch.
    #[allow(clippy::vec_box)]
    nodes: Vec<Box<DescriptorNode<UtilityConsumerDescriptor>>>,
}

#[derive(Clone, Copy)]
struct UtilityConsumerSnapshot {
    descriptors: DescriptorSnapshot<UtilityConsumerDescriptor>,
    tag: u32,
}

pub(crate) struct SelectedUtilityConsumer(UtilityConsumerDescriptor);

impl UtilityConsumerSnapshot {
    fn new(
        descriptors: DescriptorSnapshot<UtilityConsumerDescriptor>,
        tag: pg_sys::NodeTag,
    ) -> Self {
        Self {
            descriptors,
            tag: tag as u32,
        }
    }

    fn has_registered_consumer(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    unsafe fn select(
        self,
        args: ProcessUtilityArgs,
    ) -> Result<Option<SelectedUtilityConsumer>, HookError> {
        let mut selected = None;
        self.descriptors.try_for_each(|descriptor| {
            if descriptor.tag == self.tag {
                let route = unsafe {
                    (descriptor
                        .on_match
                        .expect("validated utility predicate callback"))(
                        descriptor.context,
                        args.pstmt,
                        args.query_string,
                        args.read_only_tree,
                        args.context,
                        args.params,
                        args.query_env,
                        args.dest,
                        args.completion_tag,
                    )
                };
                match route {
                    UTILITY_ROUTE_PASS_THROUGH => {}
                    UTILITY_ROUTE_CONSUMED => {
                        if selected.is_some() {
                            return Err(HookError::new(
                                "multiple COPY consumers claimed the same statement",
                            ));
                        }
                        selected = Some(descriptor);
                    }
                    other => {
                        return Err(HookError::new(format!(
                            "utility predicate returned invalid route value {other}"
                        )));
                    }
                }
            }
            Ok(())
        })?;
        Ok(selected.map(SelectedUtilityConsumer))
    }

    fn for_each(self, mut callback: impl FnMut(UtilityConsumerDescriptor)) {
        self.descriptors.for_each(|descriptor| {
            if descriptor.tag == self.tag {
                callback(descriptor);
            }
        });
    }
}

impl SelectedUtilityConsumer {
    pub(crate) unsafe fn consume(
        self,
        args: ProcessUtilityArgs,
    ) -> Result<(), HookError> {
        let descriptor = self.0;
        let route = unsafe {
            (descriptor
                .on_consume
                .expect("validated utility consumer callback"))(
                descriptor.context,
                args.pstmt,
                args.query_string,
                args.read_only_tree,
                args.context,
                args.params,
                args.query_env,
                args.dest,
                args.completion_tag,
            )
        };
        match route {
            UTILITY_ROUTE_CONSUMED => Ok(()),
            UTILITY_ROUTE_PASS_THROUGH => Err(HookError::new(
                "selected COPY consumer returned pass-through",
            )),
            other => Err(HookError::new(format!(
                "utility consumer returned invalid route value {other}"
            ))),
        }
    }
}

thread_local! {
    static UTILITY_CONSUMERS: UtilityConsumerDirectory =
        const { DescriptorDirectory::new() };
}

fn valid_descriptor(descriptor: &UtilityConsumerDescriptor) -> bool {
    descriptor.struct_size == std::mem::size_of::<UtilityConsumerDescriptor>() as u32
        && descriptor.tag == pg_sys::NodeTag::T_CopyStmt as u32
        && !descriptor.context.is_null()
        && descriptor.on_match.is_some()
        && descriptor.on_consume.is_some()
}

pub(crate) fn has_registered_consumer(tag: pg_sys::NodeTag) -> bool {
    UTILITY_CONSUMERS.with(|directory| {
        UtilityConsumerSnapshot::new(directory.snapshot(), tag)
            .has_registered_consumer()
    })
}

pub(crate) fn select(
    tag: pg_sys::NodeTag,
    args: ProcessUtilityArgs,
) -> Result<Option<SelectedUtilityConsumer>, HookError> {
    // SAFETY: callback arguments are owned by the current ProcessUtility
    // invocation and directory nodes are backend-lifetime allocations.
    unsafe {
        UTILITY_CONSUMERS.with(|directory| {
            UtilityConsumerSnapshot::new(directory.snapshot(), tag).select(args)
        })
    }
}

pub(crate) fn prepare_consumers(
    descriptors: &[UtilityConsumerDescriptor],
) -> Option<PreparedUtilityConsumers> {
    if !descriptors.iter().all(valid_descriptor) {
        return None;
    }
    Some(PreparedUtilityConsumers {
        nodes: descriptors
            .iter()
            .copied()
            .map(DescriptorNode::new)
            .collect(),
    })
}

pub(crate) fn commit_consumers(prepared: PreparedUtilityConsumers) {
    UTILITY_CONSUMERS.with(|directory| {
        let _ = directory.commit(prepared.nodes);
    });
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use super::*;

    unsafe extern "C-unwind" fn matches(
        _context: *mut c_void,
        _pstmt: *mut pg_sys::PlannedStmt,
        _query_string: *const std::ffi::c_char,
        _read_only_tree: bool,
        _process_context: pg_sys::ProcessUtilityContext::Type,
        _params: *mut pg_sys::ParamListInfoData,
        _query_env: *mut pg_sys::QueryEnvironment,
        _dest: *mut pg_sys::DestReceiver,
        _completion_tag: *mut pg_sys::QueryCompletion,
    ) -> u8 {
        UTILITY_ROUTE_PASS_THROUGH
    }

    unsafe extern "C-unwind" fn consume(
        _context: *mut c_void,
        _pstmt: *mut pg_sys::PlannedStmt,
        _query_string: *const std::ffi::c_char,
        _read_only_tree: bool,
        _process_context: pg_sys::ProcessUtilityContext::Type,
        _params: *mut pg_sys::ParamListInfoData,
        _query_env: *mut pg_sys::QueryEnvironment,
        _dest: *mut pg_sys::DestReceiver,
        _completion_tag: *mut pg_sys::QueryCompletion,
    ) -> u8 {
        UTILITY_ROUTE_CONSUMED
    }

    fn descriptor() -> UtilityConsumerDescriptor {
        UtilityConsumerDescriptor {
            struct_size: std::mem::size_of::<UtilityConsumerDescriptor>() as u32,
            tag: pg_sys::NodeTag::T_CopyStmt as u32,
            context: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            on_match: Some(matches),
            on_consume: Some(consume),
        }
    }

    #[test]
    fn descriptor_validation_requires_copy_tag_and_callbacks() {
        assert!(valid_descriptor(&descriptor()));
        let mut candidate = descriptor();
        candidate.struct_size =
            std::mem::size_of::<UtilityConsumerDescriptor>() as u32 + 1;
        assert!(!valid_descriptor(&candidate));
        candidate = descriptor();
        candidate.tag = pg_sys::NodeTag::T_CommentStmt as u32;
        assert!(!valid_descriptor(&candidate));
        candidate = descriptor();
        candidate.on_consume = None;
        assert!(!valid_descriptor(&candidate));
    }
}
