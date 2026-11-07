//! Append-only backend-local registries for stable descriptor snapshots.

use std::cell::Cell;
use std::ops::ControlFlow;
use std::ptr;

struct DescriptorNode<T: Copy> {
    descriptor: T,
    next: Cell<*const DescriptorNode<T>>,
}

/// Descriptor nodes allocated before an atomic registration commits.
///
/// The fixed-length slice is the only allocation. Committing leaks it whole,
/// so every node keeps a stable backend-lifetime address without a per-node
/// box, and the commit itself neither allocates nor fails.
pub(crate) struct PreparedDescriptors<T: Copy> {
    nodes: Box<[DescriptorNode<T>]>,
}

impl<T: Copy> PreparedDescriptors<T> {
    pub(crate) fn new(descriptors: impl IntoIterator<Item = T>) -> Self {
        Self {
            nodes: descriptors
                .into_iter()
                .map(|descriptor| DescriptorNode {
                    descriptor,
                    next: Cell::new(ptr::null()),
                })
                .collect(),
        }
    }
}

pub(crate) struct DescriptorRegistry<T: Copy> {
    head: Cell<*const DescriptorNode<T>>,
    tail: Cell<*const DescriptorNode<T>>,
}

impl<T: Copy> DescriptorRegistry<T> {
    pub(crate) const fn new() -> Self {
        Self {
            head: Cell::new(ptr::null()),
            tail: Cell::new(ptr::null()),
        }
    }

    fn append_node(&self, node: &'static DescriptorNode<T>) {
        let node = ptr::from_ref(node);
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: the tail is a backend-lifetime node published by this
            // single-threaded registry.
            unsafe { (*tail).next.set(node) };
        }
    }

    /// Publish prepared nodes in order and report whether they are the
    /// registry's first descriptors.
    pub(crate) fn commit(&self, prepared: PreparedDescriptors<T>) -> bool
    where
        T: 'static,
    {
        let was_empty = self.head.get().is_null();
        let nodes: &'static [DescriptorNode<T>] = Box::leak(prepared.nodes);
        for node in nodes {
            self.append_node(node);
        }
        was_empty && !nodes.is_empty()
    }

    pub(crate) fn snapshot(&self) -> DescriptorSnapshot<T> {
        DescriptorSnapshot {
            first: self.head.get(),
            last: self.tail.get(),
        }
    }

    #[cfg(test)]
    pub(crate) fn append(&self, descriptor: T)
    where
        T: 'static,
    {
        let _ = self.register(descriptor);
    }

    #[cfg(test)]
    pub(crate) fn register(&self, descriptor: T) -> bool
    where
        T: 'static,
    {
        self.commit(PreparedDescriptors::new(Some(descriptor)))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DescriptorSnapshot<T: Copy> {
    first: *const DescriptorNode<T>,
    last: *const DescriptorNode<T>,
}

impl<T: Copy + 'static> DescriptorSnapshot<T> {
    pub(crate) fn is_empty(self) -> bool {
        self.first.is_null()
    }

    pub(crate) fn for_each(self, mut callback: impl FnMut(T)) {
        let _: ControlFlow<(), ()> = self.walk(|descriptor| {
            callback(*descriptor);
            ControlFlow::Continue(())
        });
    }

    pub(crate) fn for_each_if(
        self,
        mut matches: impl FnMut(T) -> bool,
        mut callback: impl FnMut(T),
    ) {
        self.for_each(|descriptor| {
            if matches(descriptor) {
                callback(descriptor);
            }
        });
    }

    pub(crate) fn try_for_each<E>(
        self,
        mut callback: impl FnMut(T) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.walk(|descriptor| match callback(*descriptor) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    /// Visit the registered descriptors in place, in registration order.
    ///
    /// Committed nodes are never reclaimed, so registries that hand descriptor
    /// addresses to runtime consumers may publish these `'static` borrows.
    pub(crate) fn walk<B>(
        self,
        mut callback: impl FnMut(&'static T) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: committed nodes are leaked and remain live for the
            // backend lifetime. The captured tail keeps recursive registration
            // outside this event snapshot.
            let node: &'static DescriptorNode<T> = unsafe { &*current };
            match callback(&node.descriptor) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(error) => return ControlFlow::Break(error),
            }
            if current == self.last {
                break;
            }
            current = node.next.get();
        }
        ControlFlow::Continue(())
    }
}
