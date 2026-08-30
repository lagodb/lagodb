//! Runtime-owned object-access hook routers.

use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use crate::descriptor_directory::{DescriptorDirectory, DescriptorNode};
use crate::{hooks, storage::volume_config::on_object_access};
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    OBJECT_ACCESS_EVENTS_KNOWN, ObjectAccessHookDescriptor,
    ObjectAccessStrHookDescriptor, object_access_event_mask,
};
use pgrx::{pg_guard, pg_sys};

type ObjectAccessHookDirectory = DescriptorDirectory<ObjectAccessHookDescriptor>;
type ObjectAccessStrHookDirectory =
    DescriptorDirectory<ObjectAccessStrHookDescriptor>;

thread_local! {
    static OBJECT_ACCESS_HOOKS: ObjectAccessHookDirectory = const { DescriptorDirectory::new() };
    static OBJECT_ACCESS_STR_HOOKS: ObjectAccessStrHookDirectory = const { DescriptorDirectory::new() };
}

static PREV_OBJECT_ACCESS_HOOK: OnceLock<pg_sys::object_access_hook_type> =
    OnceLock::new();
static PREV_OBJECT_ACCESS_STR_HOOK: OnceLock<pg_sys::object_access_hook_type_str> =
    OnceLock::new();

pub(crate) struct PreparedObjectAccessHooks {
    // Both node families are allocated completely before atomic registration
    // starts publishing stable backend-lifetime addresses.
    #[allow(clippy::vec_box)]
    nodes: Vec<Box<DescriptorNode<ObjectAccessHookDescriptor>>>,
    #[allow(clippy::vec_box)]
    str_nodes: Vec<Box<DescriptorNode<ObjectAccessStrHookDescriptor>>>,
}

pub(crate) fn init() {
    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        install_router();
    }
}

fn valid_filter(event_mask: u32) -> bool {
    event_mask != 0 && event_mask & !OBJECT_ACCESS_EVENTS_KNOWN == 0
}

fn valid_descriptor(descriptor: &ObjectAccessHookDescriptor) -> bool {
    descriptor.struct_size == std::mem::size_of::<ObjectAccessHookDescriptor>() as u32
        && valid_filter(descriptor.event_mask)
        && !descriptor.context.is_null()
        && descriptor.on_access.is_some()
}

fn valid_str_descriptor(descriptor: &ObjectAccessStrHookDescriptor) -> bool {
    descriptor.struct_size
        == std::mem::size_of::<ObjectAccessStrHookDescriptor>() as u32
        && valid_filter(descriptor.event_mask)
        && !descriptor.context.is_null()
        && descriptor.on_access.is_some()
}

pub(crate) fn prepare_hooks(
    descriptors: &[ObjectAccessHookDescriptor],
    str_descriptors: &[ObjectAccessStrHookDescriptor],
) -> Option<PreparedObjectAccessHooks> {
    if !descriptors.iter().all(valid_descriptor)
        || !str_descriptors.iter().all(valid_str_descriptor)
    {
        return None;
    }
    Some(PreparedObjectAccessHooks {
        nodes: descriptors
            .iter()
            .copied()
            .map(DescriptorNode::new)
            .collect(),
        str_nodes: str_descriptors
            .iter()
            .copied()
            .map(DescriptorNode::new)
            .collect(),
    })
}

pub(crate) fn commit_hooks(prepared: PreparedObjectAccessHooks) {
    let install =
        OBJECT_ACCESS_HOOKS.with(|directory| directory.commit(prepared.nodes));
    let install_str = OBJECT_ACCESS_STR_HOOKS
        .with(|directory| directory.commit(prepared.str_nodes));
    if install {
        install_router();
    }
    if install_str {
        PREV_OBJECT_ACCESS_STR_HOOK.get_or_init(|| unsafe {
            let previous = pg_sys::object_access_hook_str;
            pg_sys::object_access_hook_str = Some(object_access_str_router);
            previous
        });
    }
}

fn install_router() {
    PREV_OBJECT_ACCESS_HOOK.get_or_init(|| unsafe {
        let previous = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(object_access_router);
        previous
    });
}

#[cfg(test)]
pub(crate) fn registered_hook_counts() -> (usize, usize) {
    fn ordinary(directory: &ObjectAccessHookDirectory) -> usize {
        let mut count = 0;
        directory.snapshot().for_each(|_| count += 1);
        count
    }
    fn string(directory: &ObjectAccessStrHookDirectory) -> usize {
        let mut count = 0;
        directory.snapshot().for_each(|_| count += 1);
        count
    }
    (
        OBJECT_ACCESS_HOOKS.with(ordinary),
        OBJECT_ACCESS_STR_HOOKS.with(string),
    )
}

unsafe fn namespace_result(
    access: pg_sys::ObjectAccessType::Type,
    arg: *mut c_void,
) -> Option<bool> {
    (access == pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH && !arg.is_null()).then(
        || unsafe { (*arg.cast::<pg_sys::ObjectAccessNamespaceSearch>()).result },
    )
}

unsafe fn preserve_namespace_denial(
    access: pg_sys::ObjectAccessType::Type,
    arg: *mut c_void,
    denied: bool,
) {
    if denied
        && access == pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH
        && !arg.is_null()
    {
        unsafe {
            (*arg.cast::<pg_sys::ObjectAccessNamespaceSearch>()).result = false;
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn object_access_router(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        if let Some(Some(previous)) = PREV_OBJECT_ACCESS_HOOK.get() {
            previous(access, class_id, object_id, sub_id, arg);
        }
        let mut denied = namespace_result(access, arg) == Some(false);

        if access == pg_sys::ObjectAccessType::OAT_DROP
            && class_id == pg_sys::ExtensionRelationId
            && let Err(error) = hooks::drop_extension_workers(object_id)
        {
            PgReportError::from_domain_error(error).report();
        }
        if let Err(error) = on_object_access(access, class_id, object_id, sub_id) {
            PgReportError::from_domain_error(error).report();
        }
        if let Some(event) = object_access_event_mask(access) {
            let hooks = OBJECT_ACCESS_HOOKS.with(|directory| directory.snapshot());
            hooks.for_each_if(
                |descriptor| {
                    descriptor.event_mask & event != 0
                        && (descriptor.class_id == pg_sys::InvalidOid
                            || descriptor.class_id == class_id)
                },
                |descriptor| {
                    descriptor.on_access.expect("validated object-access hook")(
                        descriptor.context,
                        access,
                        class_id,
                        object_id,
                        sub_id,
                        arg,
                    );
                    preserve_namespace_denial(access, arg, denied);
                    denied |= namespace_result(access, arg) == Some(false);
                },
            );
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn object_access_str_router(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const c_char,
    sub_id: i32,
    arg: *mut c_void,
) {
    unsafe {
        if let Some(Some(previous)) = PREV_OBJECT_ACCESS_STR_HOOK.get() {
            previous(access, class_id, object_name, sub_id, arg);
        }
        let mut denied = namespace_result(access, arg) == Some(false);
        if let Some(event) = object_access_event_mask(access) {
            let hooks =
                OBJECT_ACCESS_STR_HOOKS.with(|directory| directory.snapshot());
            hooks.for_each_if(
                |descriptor| {
                    descriptor.event_mask & event != 0
                        && (descriptor.class_id == pg_sys::InvalidOid
                            || descriptor.class_id == class_id)
                },
                |descriptor| {
                    descriptor
                        .on_access
                        .expect("validated object-access-str hook")(
                        descriptor.context,
                        access,
                        class_id,
                        object_name,
                        sub_id,
                        arg,
                    );
                    preserve_namespace_denial(access, arg, denied);
                    denied |= namespace_result(access, arg) == Some(false);
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lagodb_core::runtime_api::{OBJECT_ACCESS_DROP, OBJECT_ACCESS_POST_CREATE};

    unsafe extern "C-unwind" fn object_callback(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_id: pg_sys::Oid,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    unsafe extern "C-unwind" fn str_callback(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_name: *const c_char,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    fn descriptor(
        event_mask: u32,
        class_id: pg_sys::Oid,
    ) -> ObjectAccessHookDescriptor {
        ObjectAccessHookDescriptor {
            struct_size: std::mem::size_of::<ObjectAccessHookDescriptor>() as u32,
            event_mask,
            class_id,
            context: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            on_access: Some(object_callback),
        }
    }

    fn str_descriptor(
        event_mask: u32,
        class_id: pg_sys::Oid,
    ) -> ObjectAccessStrHookDescriptor {
        ObjectAccessStrHookDescriptor {
            struct_size: std::mem::size_of::<ObjectAccessStrHookDescriptor>() as u32,
            event_mask,
            class_id,
            context: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            on_access: Some(str_callback),
        }
    }

    #[test]
    fn descriptor_validation_rejects_unknown_and_empty_masks() {
        let class_id = pg_sys::Oid::from(1259);
        assert!(valid_descriptor(&descriptor(OBJECT_ACCESS_DROP, class_id)));
        assert!(valid_str_descriptor(&str_descriptor(
            OBJECT_ACCESS_DROP,
            class_id
        )));
        assert!(!valid_descriptor(&descriptor(0, class_id)));
        assert!(!valid_descriptor(&descriptor(
            OBJECT_ACCESS_EVENTS_KNOWN << 1,
            class_id
        )));

        let mut candidate = descriptor(OBJECT_ACCESS_DROP, class_id);
        candidate.struct_size = 0;
        assert!(!valid_descriptor(&candidate));
        candidate.struct_size =
            std::mem::size_of::<ObjectAccessHookDescriptor>() as u32 + 1;
        assert!(!valid_descriptor(&candidate));
        candidate.struct_size =
            std::mem::size_of::<ObjectAccessHookDescriptor>() as u32;
        candidate.on_access = None;
        assert!(!valid_descriptor(&candidate));
        candidate.on_access = Some(object_callback);
        candidate.context = std::ptr::null_mut();
        assert!(!valid_descriptor(&candidate));
    }

    #[test]
    fn directories_request_installation_only_for_first_descriptor() {
        let class_id = pg_sys::Oid::from(1259);
        let directory = ObjectAccessHookDirectory::new();
        assert!(directory.register(descriptor(OBJECT_ACCESS_DROP, class_id)));
        assert!(!directory.register(descriptor(OBJECT_ACCESS_DROP, class_id)));

        let str_directory = ObjectAccessStrHookDirectory::new();
        assert!(str_directory.register(str_descriptor(OBJECT_ACCESS_DROP, class_id)));
        assert!(
            !str_directory.register(str_descriptor(OBJECT_ACCESS_DROP, class_id))
        );
    }

    #[test]
    fn snapshot_filters_event_and_class() {
        let relation_class = pg_sys::Oid::from(1259);
        let procedure_class = pg_sys::Oid::from(1255);
        let directory = ObjectAccessHookDirectory::new();
        directory.append(descriptor(OBJECT_ACCESS_DROP, relation_class));
        directory.append(descriptor(OBJECT_ACCESS_POST_CREATE, pg_sys::InvalidOid));
        let snapshot = directory.snapshot();

        let mut matches = 0;
        let event = object_access_event_mask(pg_sys::ObjectAccessType::OAT_DROP)
            .expect("known object-access event");
        snapshot.for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == relation_class)
            },
            |_| matches += 1,
        );

        snapshot.for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == procedure_class)
            },
            |_| matches += 1,
        );
        assert_eq!(matches, 1);
    }

    #[test]
    fn snapshot_excludes_recursive_append_for_both_hook_families() {
        let class_id = pg_sys::Oid::from(1259);
        let directory = ObjectAccessHookDirectory::new();
        directory.append(descriptor(OBJECT_ACCESS_DROP, class_id));
        let snapshot = directory.snapshot();
        directory.append(descriptor(OBJECT_ACCESS_DROP, class_id));
        let mut ordinary = 0;
        let event = object_access_event_mask(pg_sys::ObjectAccessType::OAT_DROP)
            .expect("known object-access event");
        snapshot.for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == class_id)
            },
            |_| ordinary += 1,
        );
        assert_eq!(ordinary, 1);

        let str_directory = ObjectAccessStrHookDirectory::new();
        str_directory.append(str_descriptor(OBJECT_ACCESS_DROP, class_id));
        let str_snapshot = str_directory.snapshot();
        str_directory.append(str_descriptor(OBJECT_ACCESS_DROP, class_id));
        let mut string = 0;
        str_snapshot.for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == class_id)
            },
            |_| string += 1,
        );
        assert_eq!(string, 1);
    }

    #[test]
    fn namespace_denial_is_monotonic() {
        let mut search = pg_sys::ObjectAccessNamespaceSearch {
            ereport_on_violation: false,
            result: false,
        };
        let arg = (&mut search as *mut pg_sys::ObjectAccessNamespaceSearch).cast();

        search.result = true;
        unsafe {
            preserve_namespace_denial(
                pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH,
                arg,
                true,
            );
        }
        assert!(!search.result);

        search.result = true;
        unsafe {
            preserve_namespace_denial(
                pg_sys::ObjectAccessType::OAT_POST_CREATE,
                arg,
                true,
            );
        }
        assert!(search.result);
    }

    #[test]
    fn snapshots_run_matching_hooks_in_fifo_registration_order() {
        let class_id = pg_sys::Oid::from(1259);
        let mut first_context = 1_u8;
        let mut second_context = 2_u8;

        let directory = ObjectAccessHookDirectory::new();
        let mut first = descriptor(OBJECT_ACCESS_DROP, class_id);
        first.context = std::ptr::from_mut(&mut first_context).cast();
        let mut second = descriptor(OBJECT_ACCESS_DROP, class_id);
        second.context = std::ptr::from_mut(&mut second_context).cast();
        directory.append(first);
        directory.append(second);
        let mut ordinary_order = Vec::new();
        let event = object_access_event_mask(pg_sys::ObjectAccessType::OAT_DROP)
            .expect("known object-access event");
        directory.snapshot().for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == class_id)
            },
            |descriptor| ordinary_order.push(descriptor.context),
        );
        assert_eq!(ordinary_order, vec![first.context, second.context]);

        let str_directory = ObjectAccessStrHookDirectory::new();
        let mut first = str_descriptor(OBJECT_ACCESS_DROP, class_id);
        first.context = std::ptr::from_mut(&mut first_context).cast();
        let mut second = str_descriptor(OBJECT_ACCESS_DROP, class_id);
        second.context = std::ptr::from_mut(&mut second_context).cast();
        str_directory.append(first);
        str_directory.append(second);
        let mut string_order = Vec::new();
        str_directory.snapshot().for_each_if(
            |descriptor| {
                descriptor.event_mask & event != 0
                    && (descriptor.class_id == pg_sys::InvalidOid
                        || descriptor.class_id == class_id)
            },
            |descriptor| string_order.push(descriptor.context),
        );
        assert_eq!(string_order, vec![first.context, second.context]);
    }
}
