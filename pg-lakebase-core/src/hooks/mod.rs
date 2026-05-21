//! PostgreSQL hooks framework
//!
//! This module provides safe wrappers around various PostgreSQL hooks:
//! - `utility_hook`: ProcessUtility hook for DDL statements
//! - `object_access_hook`: Object access hook for permission and access control

mod error;
pub mod object_access_hook;
pub mod utility_hook;

pub use error::{HookError, ObjectAccessHookError, UtilityHookError};

pub use object_access_hook::{
    ObjectAccessEvent, ObjectAccessHook, ObjectAccessStrEvent, ObjectAccessStrHook,
    register_object_access_hook, register_object_access_str_hook,
};
pub use utility_hook::{
    AlterTableMoveAllStmtNode, AlterTableSpaceOptionsStmtNode,
    AlterTableStmtNode, CopyStmtNode, CreateStmtNode, CreateTableAsStmtNode,
    CreateTableSpaceStmtNode, PostUtilityContext, RenameStmtNode, UtilityHook,
    UtilityNode, UtilityStmtNode, register_utility_hook,
};
