//! Object-access hook event filters shared by providers and the runtime.

use pgrx::pg_sys;

pub const OBJECT_ACCESS_POST_CREATE: u32 = 1 << 0;
pub const OBJECT_ACCESS_DROP: u32 = 1 << 1;
pub const OBJECT_ACCESS_POST_ALTER: u32 = 1 << 2;
pub const OBJECT_ACCESS_NAMESPACE_SEARCH: u32 = 1 << 3;
pub const OBJECT_ACCESS_FUNCTION_EXECUTE: u32 = 1 << 4;
pub const OBJECT_ACCESS_TRUNCATE: u32 = 1 << 5;
pub const OBJECT_ACCESS_EVENTS_KNOWN: u32 = OBJECT_ACCESS_POST_CREATE
    | OBJECT_ACCESS_DROP
    | OBJECT_ACCESS_POST_ALTER
    | OBJECT_ACCESS_NAMESPACE_SEARCH
    | OBJECT_ACCESS_FUNCTION_EXECUTE
    | OBJECT_ACCESS_TRUNCATE;

#[must_use]
pub fn object_access_event_mask(
    access: pg_sys::ObjectAccessType::Type,
) -> Option<u32> {
    match access {
        pg_sys::ObjectAccessType::OAT_POST_CREATE => Some(OBJECT_ACCESS_POST_CREATE),
        pg_sys::ObjectAccessType::OAT_DROP => Some(OBJECT_ACCESS_DROP),
        pg_sys::ObjectAccessType::OAT_POST_ALTER => Some(OBJECT_ACCESS_POST_ALTER),
        pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH => {
            Some(OBJECT_ACCESS_NAMESPACE_SEARCH)
        }
        pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE => {
            Some(OBJECT_ACCESS_FUNCTION_EXECUTE)
        }
        pg_sys::ObjectAccessType::OAT_TRUNCATE => Some(OBJECT_ACCESS_TRUNCATE),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAccessFilter {
    event_mask: u32,
    class_id: pg_sys::Oid,
}

impl ObjectAccessFilter {
    #[must_use]
    pub const fn new(event_mask: u32) -> Self {
        assert!(
            event_mask != 0 && event_mask & !OBJECT_ACCESS_EVENTS_KNOWN == 0,
            "object-access filter contains no events or unknown event bits"
        );
        Self {
            event_mask,
            class_id: pg_sys::InvalidOid,
        }
    }

    #[must_use]
    pub const fn for_class(mut self, class_id: pg_sys::Oid) -> Self {
        self.class_id = class_id;
        self
    }

    pub const fn event_mask(self) -> u32 {
        self.event_mask
    }

    pub const fn class_id(self) -> pg_sys::Oid {
        self.class_id
    }
}
