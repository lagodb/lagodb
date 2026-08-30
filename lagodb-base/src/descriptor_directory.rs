//! Append-only backend-local directories for stable descriptor snapshots.

use std::cell::Cell;
use std::ops::ControlFlow;
use std::ptr;

pub(crate) struct DescriptorNode<T: Copy> {
    descriptor: T,
    next: Cell<*const DescriptorNode<T>>,
}

impl<T: Copy> DescriptorNode<T> {
    pub(crate) fn new(descriptor: T) -> Box<Self> {
        Box::new(Self {
            descriptor,
            next: Cell::new(ptr::null()),
        })
    }
}

pub(crate) struct DescriptorDirectory<T: Copy> {
    head: Cell<*const DescriptorNode<T>>,
    tail: Cell<*const DescriptorNode<T>>,
}

impl<T: Copy> DescriptorDirectory<T> {
    pub(crate) const fn new() -> Self {
        Self {
            head: Cell::new(ptr::null()),
            tail: Cell::new(ptr::null()),
        }
    }

    fn append_node(&self, node: Box<DescriptorNode<T>>) {
        let node = Box::into_raw(node);
        let tail = self.tail.replace(node);
        if tail.is_null() {
            self.head.set(node);
        } else {
            // SAFETY: the tail is a backend-lifetime node published by this
            // single-threaded directory.
            unsafe { (*tail).next.set(node) };
        }
    }

    pub(crate) fn commit<I>(&self, nodes: I) -> bool
    where
        I: IntoIterator<Item = Box<DescriptorNode<T>>>,
    {
        let was_empty = self.head.get().is_null();
        let mut appended = false;
        for node in nodes {
            appended = true;
            self.append_node(node);
        }
        was_empty && appended
    }

    pub(crate) fn snapshot(&self) -> DescriptorSnapshot<T> {
        DescriptorSnapshot {
            first: self.head.get(),
            last: self.tail.get(),
        }
    }

    #[cfg(test)]
    pub(crate) fn append(&self, descriptor: T) {
        let _ = self.commit(Some(DescriptorNode::new(descriptor)));
    }

    #[cfg(test)]
    pub(crate) fn register(&self, descriptor: T) -> bool {
        self.commit(Some(DescriptorNode::new(descriptor)))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DescriptorSnapshot<T: Copy> {
    first: *const DescriptorNode<T>,
    last: *const DescriptorNode<T>,
}

impl<T: Copy> DescriptorSnapshot<T> {
    pub(crate) fn for_each(self, mut callback: impl FnMut(T)) {
        let _: ControlFlow<(), ()> = self.walk(|descriptor| {
            callback(descriptor);
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
        match self.walk(|descriptor| match callback(descriptor) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    fn walk<B>(
        self,
        mut callback: impl FnMut(T) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let mut current = self.first;
        while !current.is_null() {
            // SAFETY: nodes remain live for the backend lifetime. The captured
            // tail keeps recursive registration outside this event snapshot.
            let node = unsafe { &*current };
            match callback(node.descriptor) {
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
