//! PostgreSQL hooks framework
//!
//! This module provides safe wrappers around various PostgreSQL hooks:
//! - `utility_hook`: ProcessUtility hook for DDL statements
//! - `object_access_hook`: Object access hook for permission and access control

pub mod object_access_hook;
pub mod utility_hook;

pub use object_access_hook::{
    ObjectAccessContext, ObjectAccessHook, ObjectAccessHookError,
    register_object_access_hook,
};
pub use utility_hook::{
    UtilityHook, UtilityHookError, UtilityNode, register_utility_hook,
};
