use std::ffi::CStr;
use std::marker::PhantomData;
use std::str::Utf8Error;

use pgrx::pg_sys;

use super::identity::ForeignStoreIdentity;

/// One borrowed PostgreSQL foreign option.
#[derive(Clone, Copy, Debug)]
pub struct ForeignOption<'a> {
    name: &'a CStr,
    value: &'a CStr,
}

impl<'a> ForeignOption<'a> {
    pub fn name(&self) -> &'a CStr {
        self.name
    }

    pub fn value(&self) -> &'a CStr {
        self.value
    }

    pub fn value_str(&self) -> Result<&'a str, Utf8Error> {
        self.value.to_str()
    }
}

/// Borrowed view of one PostgreSQL `List` of `DefElem` options.
#[derive(Clone, Copy, Debug)]
pub struct ForeignOptionView<'a> {
    list: *mut pg_sys::List,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ForeignOptionView<'a> {
    pub(crate) const fn new(list: *mut pg_sys::List) -> Self {
        Self {
            list,
            _marker: PhantomData,
        }
    }

    /// Finds an option without allocating or copying its value.
    pub fn get(&self, name: &str) -> Option<ForeignOption<'a>> {
        // SAFETY: `list` is borrowed from a live PostgreSQL ForeignServer or
        // UserMapping object for the duration of this view.
        let option_count = unsafe { pg_sys::list_length(self.list) };
        for index in 0..option_count {
            // SAFETY: `index` is within list_length and foreign option lists
            // contain non-null DefElem pointers.
            let def =
                unsafe { pg_sys::list_nth(self.list, index) as *mut pg_sys::DefElem };
            // SAFETY: PostgreSQL owns both NUL-terminated strings for the
            // lifetime represented by `'a`.
            let def_name = unsafe { CStr::from_ptr((*def).defname) };
            if def_name.to_bytes() != name.as_bytes() {
                continue;
            }
            // SAFETY: `def` is a valid DefElem from the option list.
            let value = unsafe { pg_sys::defGetString(def) };
            // SAFETY: defGetString returns a PostgreSQL-owned NUL-terminated
            // string valid with the surrounding catalog object.
            let value = unsafe { CStr::from_ptr(value) };
            return Some(ForeignOption {
                name: def_name,
                value,
            });
        }
        None
    }
}

/// Borrowed server and user-mapping options passed to a shared store-config builder.
///
/// Foreign-table options are intentionally not part of this type. A store is
/// cached by effective user mapping, so allowing table options here would let
/// one table silently reuse another table's configuration.
#[derive(Clone, Copy, Debug)]
pub struct ForeignStoreOptions<'a> {
    server: ForeignOptionView<'a>,
    mapping: ForeignOptionView<'a>,
}

impl<'a> ForeignStoreOptions<'a> {
    pub(crate) const fn new(
        server: ForeignOptionView<'a>,
        mapping: ForeignOptionView<'a>,
    ) -> Self {
        Self { server, mapping }
    }

    pub fn server(&self) -> ForeignOptionView<'a> {
        self.server
    }

    pub fn mapping(&self) -> ForeignOptionView<'a> {
        self.mapping
    }
}

/// Catalog values needed to attach a configured foreign storage context.
pub(crate) struct ForeignCatalog {
    server: pg_sys::ForeignServer,
    mapping: pg_sys::UserMapping,
    identity: ForeignStoreIdentity,
    server_hashvalue: u32,
    mapping_hashvalue: u32,
}

impl ForeignCatalog {
    pub(crate) fn load(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Self {
        // SAFETY: the FDW begin callback supplies a valid foreign relation OID;
        // PostgreSQL owns the returned catalog object.
        let server_oid = unsafe { (*pg_sys::GetForeignTable(relation_oid)).serverid };
        // SAFETY: `server_oid` came from the live ForeignTable catalog entry.
        let server = unsafe { *pg_sys::GetForeignServer(server_oid) };
        // SAFETY: PostgreSQL resolves the effective user, including PUBLIC
        // fallback, and owns the returned UserMapping object.
        let mapping =
            unsafe { *pg_sys::GetUserMapping(effective_user, server.serverid) };

        let server_hashvalue = Self::syscache_hash(
            pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32,
            server.serverid,
        );

        let mapping_hashvalue = Self::syscache_hash(
            pg_sys::SysCacheIdentifier::USERMAPPINGOID as i32,
            mapping.umid,
        );

        // SAFETY: MyDatabaseId is initialized in a connected PostgreSQL backend.
        let database_oid = unsafe { pg_sys::MyDatabaseId };
        let identity =
            ForeignStoreIdentity::new(database_oid, server.serverid, mapping.umid);
        Self {
            server,
            mapping,
            identity,
            server_hashvalue,
            mapping_hashvalue,
        }
    }

    pub(crate) fn identity(&self) -> &ForeignStoreIdentity {
        &self.identity
    }

    pub(crate) fn server_hashvalue(&self) -> u32 {
        self.server_hashvalue
    }

    pub(crate) fn mapping_hashvalue(&self) -> u32 {
        self.mapping_hashvalue
    }

    pub(crate) fn options(&self) -> ForeignStoreOptions<'_> {
        ForeignStoreOptions::new(
            ForeignOptionView::new(self.server.options),
            ForeignOptionView::new(self.mapping.options),
        )
    }

    fn syscache_hash(cache_id: i32, oid: pg_sys::Oid) -> u32 {
        // SAFETY: both registered syscaches use a single OID lookup key; the
        // remaining Datum arguments are ignored by PostgreSQL.
        unsafe {
            pg_sys::GetSysCacheHashValue(
                cache_id,
                pg_sys::Datum::from(u32::from(oid) as usize),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(0usize),
                pg_sys::Datum::from(0usize),
            )
        }
    }
}
