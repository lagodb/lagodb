//! Command-local CREATE outcome tracking across utility and object-access hooks.

use std::cell::RefCell;
use std::rc::Rc;

use lagodb_core::transaction::{self, TransactionResource};
use pgrx::pg_sys;

#[derive(Debug, Clone, Copy)]
struct CreateCommand {
    created_relation: Option<pg_sys::Oid>,
    nest_level: i32,
}

#[derive(Debug, Default)]
struct CreateCommandResource {
    commands: RefCell<Vec<CreateCommand>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<CreateCommandResource>>> = const { RefCell::new(None) };
}

pub(crate) struct CreateCommandStack;

impl CreateCommandStack {
    pub(crate) fn begin() {
        let resource = Self::current();
        resource.commands.borrow_mut().push(CreateCommand {
            created_relation: None,
            nest_level: unsafe { pg_sys::GetCurrentTransactionNestLevel() },
        });
    }

    pub(crate) fn has_pending() -> bool {
        CURRENT.with_borrow(|resource| {
            resource.as_ref().is_some_and(|resource| {
                resource
                    .commands
                    .borrow()
                    .last()
                    .is_some_and(|command| command.created_relation.is_none())
            })
        })
    }

    pub(crate) fn record_created(oid: pg_sys::Oid) {
        CURRENT.with_borrow(|resource| {
            let resource = resource
                .as_ref()
                .expect("relation post-create event must have command resource");
            let mut commands = resource.commands.borrow_mut();
            let command = commands
                .last_mut()
                .expect("relation post-create event must have command state");
            command.created_relation = Some(oid);
        });
    }

    pub(crate) fn finish() -> Option<pg_sys::Oid> {
        CURRENT.with_borrow(|resource| {
            let resource = resource
                .as_ref()
                .expect("successful CREATE must have command resource");
            let command = resource
                .commands
                .borrow_mut()
                .pop()
                .expect("successful CREATE must have pre-hook state");
            command.created_relation
        })
    }

    fn current() -> Rc<CreateCommandResource> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(resource) = slot.as_ref() {
                return Rc::clone(resource);
            }
            let resource = Rc::new(CreateCommandResource::default());
            transaction::register_resource(
                Rc::clone(&resource) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&resource));
            resource
        })
    }
}

impl TransactionResource for CreateCommandResource {
    fn nest_level(&self) -> i32 {
        // The resource spans the transaction; each command owns its savepoint.
        1
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_commit(&self) {
        CURRENT.with_borrow_mut(|slot| *slot = None);
    }

    fn on_abort(&self) {
        CURRENT.with_borrow_mut(|slot| *slot = None);
    }

    fn on_prepare(&self) {
        CURRENT.with_borrow_mut(|slot| *slot = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        for command in self.commands.borrow_mut().iter_mut() {
            if command.nest_level >= current_nest_level {
                command.nest_level = current_nest_level - 1;
            }
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.commands
            .borrow_mut()
            .retain(|command| command.nest_level < current_nest_level);
    }
}
