//! Runtime-owned registry for utility consumers.
//!
//! The ProcessUtility hook owns parent fallback and command lifecycle; this
//! module owns only the cold-path consumer directory and selection. The
//! selected callback still executes through the descriptor supplied by the
//! runtime ABI.

use std::cell::Cell;

use lagodb_core::hooks::HookError;
use lagodb_core::runtime_api::{
    UTILITY_ROUTE_CONSUMED, UTILITY_ROUTE_PASS_THROUGH, UtilityConsumerDescriptor,
};
use pgrx::pg_sys;

use crate::process_utility::ProcessUtilityArgs;

struct UtilityConsumerNode {
    descriptor: UtilityConsumerDescriptor,
    next: Cell<*const UtilityConsumerNode>,
}

struct UtilityConsumerDirectory {
    head: Cell<*const UtilityConsumerNode>,
    tail: Cell<*const UtilityConsumerNode>,
}

impl UtilityConsumerDirectory {
    const fn new() -> Self {
        Self {
            head: Cell::new(std::ptr::null()),
            tail: Cell::new(std::ptr::null()),
        }
    }

    fn append_node(&self, node: Box<UtilityConsumerNode>) {
        let node = Box::into_raw(node);
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: tail is a backend-lifetime node published by this
            // single-threaded backend directory.
            unsafe { (*tail).next.set(node) };
        }
    }

    fn commit(&self, prepared: PreparedUtilityConsumers) {
        for node in prepared.nodes {
            self.append_node(node);
        }
    }

    fn snapshot(&self, tag: pg_sys::NodeTag) -> UtilityConsumerSnapshot {
        UtilityConsumerSnapshot {
            first: self.head.get(),
            last: self.tail.get(),
            tag: tag as u32,
        }
    }
}

pub(crate) struct PreparedUtilityConsumers {
    // Each node is allocated before the runtime registration transaction is
    // committed. The box keeps its address stable while the directory stores
    // a raw backend-lifetime pointer; commit therefore cannot allocate or
    // publish only part of this prepared batch.
    #[allow(clippy::vec_box)]
    nodes: Vec<Box<UtilityConsumerNode>>,
}

#[derive(Clone, Copy)]
struct UtilityConsumerSnapshot {
    first: *const UtilityConsumerNode,
    last: *const UtilityConsumerNode,
    tag: u32,
}

pub(crate) struct SelectedUtilityConsumer(*const UtilityConsumerNode);

impl UtilityConsumerSnapshot {
    fn has_registered_consumer(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    unsafe fn select(
        self,
        args: ProcessUtilityArgs,
    ) -> Result<Option<SelectedUtilityConsumer>, HookError> {
        let mut selected: *const UtilityConsumerNode = std::ptr::null();
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: directory nodes are leaked for backend lifetime and the
            // snapshot's tail bounds this walk.
            let node = unsafe { &*current };
            if node.descriptor.tag == self.tag {
                let route = unsafe {
                    (node
                        .descriptor
                        .on_match
                        .expect("validated utility predicate callback"))(
                        node.descriptor.context,
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
                        if !selected.is_null() {
                            return Err(HookError::new(
                                "multiple COPY consumers claimed the same statement",
                            ));
                        }
                        selected = current;
                    }
                    other => {
                        return Err(HookError::new(format!(
                            "utility predicate returned invalid route value {other}"
                        )));
                    }
                }
            }
            if current == self.last {
                break;
            }
            current = node.next.get();
        }
        Ok((!selected.is_null()).then_some(SelectedUtilityConsumer(selected)))
    }

    fn for_each(self, mut callback: impl FnMut(UtilityConsumerDescriptor)) {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: directory nodes are leaked for backend lifetime and the
            // snapshot's tail bounds this walk.
            let node = unsafe { &*current };
            if node.descriptor.tag == self.tag {
                callback(node.descriptor);
            }
            if current == self.last {
                break;
            }
            current = node.next.get();
        }
    }
}

impl SelectedUtilityConsumer {
    pub(crate) unsafe fn consume(
        self,
        args: ProcessUtilityArgs,
    ) -> Result<(), HookError> {
        // SAFETY: the selection pointer comes from the backend-lifetime
        // directory snapshot and is used before that directory can change.
        let descriptor = unsafe { &(*self.0).descriptor };
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
        const { UtilityConsumerDirectory::new() };
}

fn valid_descriptor(descriptor: &UtilityConsumerDescriptor) -> bool {
    descriptor.struct_size == std::mem::size_of::<UtilityConsumerDescriptor>() as u32
        && descriptor.tag == pg_sys::NodeTag::T_CopyStmt as u32
        && !descriptor.context.is_null()
        && descriptor.on_match.is_some()
        && descriptor.on_consume.is_some()
}

pub(crate) fn has_registered_consumer(tag: pg_sys::NodeTag) -> bool {
    UTILITY_CONSUMERS
        .with(|directory| directory.snapshot(tag).has_registered_consumer())
}

pub(crate) fn select(
    tag: pg_sys::NodeTag,
    args: ProcessUtilityArgs,
) -> Result<Option<SelectedUtilityConsumer>, HookError> {
    // SAFETY: callback arguments are owned by the current ProcessUtility
    // invocation and directory nodes are backend-lifetime allocations.
    unsafe {
        UTILITY_CONSUMERS.with(|directory| directory.snapshot(tag).select(args))
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
            .map(|descriptor| {
                Box::new(UtilityConsumerNode {
                    descriptor,
                    next: Cell::new(std::ptr::null()),
                })
            })
            .collect(),
    })
}

pub(crate) fn commit_consumers(prepared: PreparedUtilityConsumers) {
    UTILITY_CONSUMERS.with(|directory| directory.commit(prepared));
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
