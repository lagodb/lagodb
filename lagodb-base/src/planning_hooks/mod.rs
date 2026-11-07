//! Sole owner and router of PostgreSQL planning hook pointers.

mod pathlist;
mod planner;
mod query_tree_preparation;
mod registry;

use std::sync::OnceLock;

use pgrx::pg_sys;

pub(crate) use registry::PreparedPlanningHooks;

static PREV_PLANNER: OnceLock<pg_sys::planner_hook_type> = OnceLock::new();
static PREV_SET_REL_PATHLIST: OnceLock<pg_sys::set_rel_pathlist_hook_type> =
    OnceLock::new();
static PREV_SET_JOIN_PATHLIST: OnceLock<pg_sys::set_join_pathlist_hook_type> =
    OnceLock::new();
static PREV_CREATE_UPPER_PATHS: OnceLock<pg_sys::create_upper_paths_hook_type> =
    OnceLock::new();

pub(crate) fn init() {
    // SAFETY: `lagodb-base` installs each process-global slot once during
    // single-threaded shared-preload initialization, before provider loading.
    unsafe {
        PREV_PLANNER.get_or_init(|| {
            let previous = pg_sys::planner_hook;
            pg_sys::planner_hook = Some(planner::planner);
            previous
        });
        PREV_SET_REL_PATHLIST.get_or_init(|| {
            let previous = pg_sys::set_rel_pathlist_hook;
            pg_sys::set_rel_pathlist_hook = Some(pathlist::set_rel_pathlist);
            previous
        });
        PREV_SET_JOIN_PATHLIST.get_or_init(|| {
            let previous = pg_sys::set_join_pathlist_hook;
            pg_sys::set_join_pathlist_hook = Some(pathlist::set_join_pathlist);
            previous
        });
        PREV_CREATE_UPPER_PATHS.get_or_init(|| {
            let previous = pg_sys::create_upper_paths_hook;
            pg_sys::create_upper_paths_hook = Some(pathlist::create_upper_paths);
            previous
        });
    }
}
