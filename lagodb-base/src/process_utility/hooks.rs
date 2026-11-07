//! ProcessUtility-owned registry for provider pre/post utility hooks.
//!
//! The ProcessUtility router owns statement copies, parent fallback and hook
//! invocation order; this module owns only the cold-path hook registry and
//! tag-filtered snapshots.

use crate::descriptor_registry::{
    DescriptorRegistry, DescriptorSnapshot, PreparedDescriptors,
};
use lagodb_core::runtime_api::{REGISTER_INVALID_DESCRIPTOR, UtilityHookDescriptor};
use pgrx::pg_sys;

type UtilityHookRegistry = DescriptorRegistry<UtilityHookDescriptor>;

pub(crate) struct PreparedUtilityHooks {
    hooks: PreparedDescriptors<UtilityHookDescriptor>,
}

#[derive(Clone, Copy)]
pub(super) struct UtilityHookSnapshot {
    descriptors: DescriptorSnapshot<UtilityHookDescriptor>,
    tag: u32,
}

impl UtilityHookSnapshot {
    fn new(
        descriptors: DescriptorSnapshot<UtilityHookDescriptor>,
        tag: pg_sys::NodeTag,
    ) -> Self {
        Self {
            descriptors,
            tag: tag as u32,
        }
    }

    pub(super) fn has_matching_hooks(self) -> bool {
        let mut matched = false;
        self.for_each(|_| matched = true);
        matched
    }

    pub(super) fn for_each(self, mut callback: impl FnMut(UtilityHookDescriptor)) {
        self.descriptors.for_each(|descriptor| {
            if descriptor.tag == self.tag {
                callback(descriptor);
            }
        });
    }
}

thread_local! {
    static UTILITY_HOOKS: UtilityHookRegistry = const { DescriptorRegistry::new() };
}

fn valid_descriptor(descriptor: &UtilityHookDescriptor) -> bool {
    !descriptor.context.is_null()
        && descriptor.on_pre.is_some()
        && descriptor.on_post.is_some()
}

/// Hooks registered for `tag` at the start of this ProcessUtility call.
pub(super) fn snapshot(tag: pg_sys::NodeTag) -> UtilityHookSnapshot {
    UTILITY_HOOKS.with(|registry| UtilityHookSnapshot::new(registry.snapshot(), tag))
}

impl PreparedUtilityHooks {
    pub(crate) fn prepare(
        descriptors: &[UtilityHookDescriptor],
    ) -> Result<Self, u32> {
        if !descriptors.iter().all(valid_descriptor) {
            return Err(REGISTER_INVALID_DESCRIPTOR);
        }
        Ok(Self {
            hooks: PreparedDescriptors::new(descriptors.iter().copied()),
        })
    }

    pub(crate) fn commit(self) {
        let _ = UTILITY_HOOKS.with(|registry| registry.commit(self.hooks));
    }
}

#[cfg(test)]
pub(crate) fn registered_hook_count() -> usize {
    UTILITY_HOOKS.with(|registry| {
        let mut count = 0;
        registry.snapshot().for_each(|_| count += 1);
        count
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::{c_char, c_void};

    use super::*;

    unsafe extern "C-unwind" fn pre(
        _context: *mut c_void,
        _planned_stmt: *mut pg_sys::PlannedStmt,
        _query_string: *const c_char,
    ) {
    }
    unsafe extern "C-unwind" fn post(
        _context: *mut c_void,
        _node: *mut pg_sys::Node,
    ) {
    }

    fn descriptor(tag: pg_sys::NodeTag) -> UtilityHookDescriptor {
        UtilityHookDescriptor {
            tag: tag as u32,
            context: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            on_pre: Some(pre),
            on_post: Some(post),
        }
    }

    #[test]
    fn descriptor_validation_rejects_missing_callbacks() {
        let mut candidate = descriptor(pg_sys::NodeTag::T_CommentStmt);
        assert!(valid_descriptor(&candidate));

        candidate.on_post = None;
        assert!(!valid_descriptor(&candidate));
    }

    #[test]
    fn snapshot_excludes_descriptors_appended_later() {
        let registry = UtilityHookRegistry::new();
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));
        let snapshot = UtilityHookSnapshot::new(
            registry.snapshot(),
            pg_sys::NodeTag::T_CommentStmt,
        );
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        let mut count = 0;
        snapshot.for_each(|_| count += 1);
        assert_eq!(count, 1);
        assert!(snapshot.has_matching_hooks());
    }

    #[test]
    fn snapshot_filters_by_node_tag() {
        let registry = UtilityHookRegistry::new();
        registry.append(descriptor(pg_sys::NodeTag::T_CommentStmt));

        assert!(
            UtilityHookSnapshot::new(
                registry.snapshot(),
                pg_sys::NodeTag::T_CommentStmt,
            )
            .has_matching_hooks()
        );
        assert!(
            !UtilityHookSnapshot::new(
                registry.snapshot(),
                pg_sys::NodeTag::T_CreateStmt,
            )
            .has_matching_hooks()
        );
    }

    #[test]
    fn snapshot_runs_matching_hooks_in_fifo_registration_order() {
        let registry = UtilityHookRegistry::new();
        let mut first_context = 1_u8;
        let mut second_context = 2_u8;
        let mut first = descriptor(pg_sys::NodeTag::T_CommentStmt);
        first.context = std::ptr::from_mut(&mut first_context).cast();
        let mut second = descriptor(pg_sys::NodeTag::T_CommentStmt);
        second.context = std::ptr::from_mut(&mut second_context).cast();
        registry.append(first);
        registry.append(second);

        let mut order = Vec::new();
        UtilityHookSnapshot::new(registry.snapshot(), pg_sys::NodeTag::T_CommentStmt)
            .for_each(|descriptor| order.push(descriptor.context));

        assert_eq!(order, vec![first.context, second.context]);
    }
}
