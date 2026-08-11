use std::ffi::{CStr, c_char};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::{CopyColumnLayout, CopyError, pg};

#[derive(Clone, Copy, Debug)]
pub struct CopyOption<'a> {
    name: &'a CStr,
    def: *mut pg_sys::DefElem,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> CopyOption<'a> {
    pub fn name(&self) -> &'a CStr {
        self.name
    }

    /// Decode this option as PostgreSQL's string-like COPY value.
    ///
    /// The parse tree also contains options whose arguments are a list, a
    /// star, or are omitted entirely.  Iterating a [`CopyOptionView`] never
    /// decodes those arguments; callers should call this method only for an
    /// option whose contract requires a scalar value.  PostgreSQL's
    /// `defGetString` remains the authority for the accepted scalar syntax.
    pub fn value(&self) -> &'a CStr {
        let value = unsafe { pg_sys::defGetString(self.def) };
        unsafe { CStr::from_ptr(value) }
    }

    pub fn value_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        self.value().to_str()
    }
}

/// Completion information returned by a consuming COPY implementation.
///
/// The consumer reports only the number of processed rows. The core utility
/// adapter owns the PostgreSQL command tag and writes it exactly once after
/// the consumer has completed successfully.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopyCompletion {
    processed: u64,
}

impl CopyCompletion {
    #[must_use]
    pub const fn new(processed: u64) -> Self {
        Self { processed }
    }

    pub const fn processed(self) -> u64 {
        self.processed
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CopyOptionView<'a> {
    list: *mut pg_sys::List,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> CopyOptionView<'a> {
    fn new(list: *mut pg_sys::List) -> Self {
        Self {
            list,
            _lifetime: PhantomData,
        }
    }

    pub fn get(&self, name: &str) -> Option<CopyOption<'a>> {
        self.iter()
            .find(|option| option.name.to_bytes() == name.as_bytes())
    }

    pub fn iter(&self) -> CopyOptionIter<'a> {
        CopyOptionIter {
            list: self.list,
            index: 0,
            length: unsafe { pg_sys::list_length(self.list) },
            _lifetime: PhantomData,
        }
    }

    /// Copy the option list cells while removing provider-owned options.
    ///
    /// PostgreSQL's COPY entry points expect a `List *` and run their own
    /// option validation. The returned list contains the original `DefElem`
    /// nodes but has fresh list cells, so a consumer can remove its private
    /// options without mutating the parse tree. PostgreSQL owns the returned
    /// list cells through the current memory context; callers must use it only
    /// during the current utility command.
    pub fn without_names(&self, names: &[&[u8]]) -> *mut pg_sys::List {
        let mut filtered = unsafe { pg_sys::list_copy(self.list) };
        let mut index = 0;
        while index < unsafe { pg_sys::list_length(filtered) } {
            let def =
                unsafe { pg_sys::list_nth(filtered, index) as *mut pg_sys::DefElem };
            let name = unsafe { CStr::from_ptr((*def).defname) };
            if names.iter().any(|candidate| *candidate == name.to_bytes()) {
                filtered = unsafe { pg_sys::list_delete_ptr(filtered, def.cast()) };
            } else {
                index += 1;
            }
        }
        filtered
    }
}

pub struct CopyOptionIter<'a> {
    list: *mut pg_sys::List,
    index: i32,
    length: i32,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> Iterator for CopyOptionIter<'a> {
    type Item = CopyOption<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }
        let def = unsafe {
            pg_sys::list_nth(self.list, self.index) as *mut pg_sys::DefElem
        };
        self.index += 1;
        let name = unsafe { CStr::from_ptr((*def).defname) };
        Some(CopyOption {
            name,
            def,
            _lifetime: PhantomData,
        })
    }
}

/// A borrowed view of the raw PostgreSQL COPY parse node.
pub struct CopyStatement<'a> {
    raw: &'a pg_sys::CopyStmt,
}

impl<'a> CopyStatement<'a> {
    /// # Safety
    ///
    /// `node` must be the live `T_CopyStmt` node supplied by PostgreSQL for the
    /// duration of the returned view.
    pub(crate) unsafe fn from_node_unchecked(node: *mut pg_sys::Node) -> Self {
        Self {
            // The runtime routes this type only after matching T_CopyStmt.
            raw: unsafe { &*node.cast::<pg_sys::CopyStmt>() },
        }
    }

    pub fn is_from(&self) -> bool {
        self.raw.is_from
    }

    pub fn is_to(&self) -> bool {
        !self.raw.is_from
    }

    pub fn is_program(&self) -> bool {
        self.raw.is_program
    }

    pub fn relation(&self) -> Option<&pg_sys::RangeVar> {
        unsafe { self.raw.relation.as_ref() }
    }

    /// Returns the raw query node for `COPY (query) TO`.
    pub fn query(&self) -> Option<*mut pg_sys::Node> {
        (!self.raw.query.is_null()).then_some(self.raw.query)
    }

    pub fn attlist(&self) -> *mut pg_sys::List {
        self.raw.attlist
    }

    pub fn options(&self) -> *mut pg_sys::List {
        self.raw.options
    }

    pub fn option_view(&self) -> CopyOptionView<'a> {
        CopyOptionView::new(self.raw.options)
    }

    /// `None` means PostgreSQL's protocol stream (`STDIN`/`STDOUT`).
    pub fn filename(&self) -> Option<&CStr> {
        unsafe { self.raw.filename.as_ref() }
            .map(|_| unsafe { CStr::from_ptr(self.raw.filename.cast::<c_char>()) })
    }

    pub(crate) fn as_raw(&self) -> *const pg_sys::CopyStmt {
        self.raw
    }
}

/// The non-statement arguments supplied to `ProcessUtility` for one COPY.
///
/// These pointers are borrowed from PostgreSQL and are valid only while the
/// utility consumer callback is executing. They are intentionally exposed as
/// raw pointers because PostgreSQL owns their concrete lifetimes and query
/// context semantics.
#[derive(Clone, Copy)]
pub struct CopyProcessContext {
    planned_stmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    process_context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
}

impl CopyProcessContext {
    pub fn planned_stmt(self) -> *mut pg_sys::PlannedStmt {
        self.planned_stmt
    }

    pub fn query_string(&self) -> Option<&CStr> {
        unsafe { self.query_string.as_ref() }
            .map(|_| unsafe { CStr::from_ptr(self.query_string) })
    }

    pub fn read_only_tree(self) -> bool {
        self.read_only_tree
    }

    pub fn process_context(self) -> pg_sys::ProcessUtilityContext::Type {
        self.process_context
    }

    pub fn params(self) -> *mut pg_sys::ParamListInfoData {
        self.params
    }

    pub fn query_env(self) -> *mut pg_sys::QueryEnvironment {
        self.query_env
    }

    pub fn dest(self) -> *mut pg_sys::DestReceiver {
        self.dest
    }

    pub(crate) fn statement_location(self) -> i32 {
        // SAFETY: ProcessUtility supplies a live PlannedStmt for the entire
        // callback and this context never outlives that callback.
        unsafe { (*self.planned_stmt).stmt_location }
    }

    pub(crate) fn statement_length(self) -> i32 {
        // SAFETY: ProcessUtility supplies a live PlannedStmt for the entire
        // callback and this context never outlives that callback.
        unsafe { (*self.planned_stmt).stmt_len }
    }

    fn set_completion(self, processed: u64) {
        if !self.completion_tag.is_null() {
            // SAFETY: PostgreSQL supplies a live completion tag whenever this
            // pointer is non-null and keeps it live for the complete
            // ProcessUtility callback.
            unsafe {
                pg_sys::SetQueryCompletion(
                    self.completion_tag,
                    pg_sys::CommandTag::CMDTAG_COPY,
                    processed,
                );
            }
        }
    }
}

/// Typed COPY context passed to a consuming utility provider.
pub struct CopyContext<'a> {
    statement: CopyStatement<'a>,
    process: CopyProcessContext,
}

impl<'a> CopyContext<'a> {
    /// # Safety
    ///
    /// `pstmt` and every callback argument must be the live values PostgreSQL
    /// supplied to `ProcessUtility`. The utility node must be `T_CopyStmt`.
    pub(crate) unsafe fn from_raw(
        pstmt: *mut pg_sys::PlannedStmt,
        query_string: *const c_char,
        read_only_tree: bool,
        process_context: pg_sys::ProcessUtilityContext::Type,
        params: *mut pg_sys::ParamListInfoData,
        query_env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        completion_tag: *mut pg_sys::QueryCompletion,
    ) -> Self {
        let node = unsafe { (*pstmt).utilityStmt };
        Self {
            statement: unsafe { CopyStatement::from_node_unchecked(node) },
            process: CopyProcessContext {
                planned_stmt: pstmt,
                query_string,
                read_only_tree,
                process_context,
                params,
                query_env,
                dest,
                completion_tag,
            },
        }
    }

    pub fn statement(&self) -> &CopyStatement<'a> {
        &self.statement
    }

    pub fn process(&self) -> CopyProcessContext {
        self.process
    }

    pub(crate) fn complete(&mut self, completion: CopyCompletion) {
        self.process.set_completion(completion.processed());
    }

    /// Build the parse state required by PostgreSQL's public COPY API.
    ///
    /// The standard `DoCopy` path prepares this state before calling
    /// `BeginCopyFrom`/`BeginCopyTo`. A consuming utility provider must do the
    /// same; the returned guard keeps it alive until the driver has finished.
    pub fn parse_state(&self) -> CopyParseState {
        CopyParseState::new(self.process)
    }

    /// Prepare the PostgreSQL relation, permission metadata, and transformed
    /// `COPY FROM ... WHERE` expression required by [`CopyFromDriver`].
    ///
    /// The preparation owns the opened target relation until the driver has
    /// finished. It also rejects the same row-security case as PostgreSQL's
    /// standard `DoCopy` path.
    ///
    /// # Errors
    ///
    /// Returns [`CopyError`] when PostgreSQL rejects relation resolution,
    /// permissions, expression transformation, or row-security policy.
    pub fn prepare_from<'statement, 'parse>(
        &'statement self,
        parse_state: &'parse CopyParseState,
    ) -> Result<CopyFromPreparation<'statement, 'parse>, CopyError> {
        let mut raw = MaybeUninit::<pg::LakebaseCopyPreparation>::uninit();
        let pstate = parse_state.as_raw();
        let statement = self.statement.as_raw();
        let location = self.process.statement_location();
        let length = self.process.statement_length();
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::prepare_from(
                    pstate,
                    statement,
                    location,
                    length,
                    raw.as_mut_ptr(),
                );
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;

        // The C preparation function writes every field before returning.
        let raw = unsafe { raw.assume_init() };
        Ok(CopyFromPreparation {
            raw,
            _statement_lifetime: PhantomData,
            _parse_lifetime: PhantomData,
        })
    }

    /// Prepare a standard PostgreSQL `COPY TO` relation or query execution.
    ///
    /// Relation-form COPY over a foreign table is normalized to a query form,
    /// which is the execution shape required for the external-object COPY
    /// interface. Row-security relation copies use the same query form as
    /// PostgreSQL's `DoCopy` implementation.
    ///
    /// # Errors
    ///
    /// Returns [`CopyError`] when PostgreSQL rejects relation resolution,
    /// permissions, or query preparation metadata.
    pub fn prepare_to<'statement, 'parse>(
        &'statement self,
        parse_state: &'parse CopyParseState,
    ) -> Result<CopyToPreparation<'statement, 'parse>, CopyError> {
        let mut raw = MaybeUninit::<pg::LakebaseCopyPreparation>::uninit();
        let pstate = parse_state.as_raw();
        let statement = self.statement.as_raw();
        let location = self.process.statement_location();
        let length = self.process.statement_length();
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::prepare_to(
                    pstate,
                    statement,
                    location,
                    length,
                    raw.as_mut_ptr(),
                );
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;

        // The C preparation function writes every field before returning.
        let raw = unsafe { raw.assume_init() };
        Ok(CopyToPreparation {
            raw,
            _statement_lifetime: PhantomData,
            _parse_lifetime: PhantomData,
        })
    }
}

/// Parse state owned by one consuming COPY execution.
pub struct CopyParseState {
    raw: *mut pg_sys::ParseState,
}

impl CopyParseState {
    fn new(process: CopyProcessContext) -> Self {
        let raw = unsafe { pg_sys::make_parsestate(std::ptr::null_mut()) };
        // SAFETY: `make_parsestate` returned a live PostgreSQL parse state;
        // the query string and query environment are owned by the current
        // ProcessUtility invocation.
        unsafe {
            (*raw).p_sourcetext = process.query_string;
            (*raw).p_queryEnv = process.query_env;
        }
        Self { raw }
    }

    pub fn as_raw(&self) -> *mut pg_sys::ParseState {
        self.raw
    }

    /// Release the PostgreSQL parse state and return any PostgreSQL error to
    /// the COPY coordinator.
    ///
    /// `free_parsestate` is normally infallible, but PostgreSQL still reports
    /// an internal error when the parse state accumulated more target
    /// attributes than it can represent.  This explicit operation keeps that
    /// error in the normal COPY `Result` path.  `Drop` remains only a
    /// best-effort fallback for early returns and therefore never reports an
    /// error itself.
    pub fn dispose(mut self) -> Result<(), PgError> {
        self.free()
    }

    fn free(&mut self) -> Result<(), PgError> {
        let raw = std::mem::replace(&mut self.raw, std::ptr::null_mut());
        if raw.is_null() {
            return Ok(());
        }

        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg_sys::free_parsestate(raw);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }
}

impl Drop for CopyParseState {
    fn drop(&mut self) {
        // SAFETY: this guard owns the parse state returned by
        // `make_parsestate` and drops it once after the COPY driver has
        // released all PostgreSQL COPY state that references it.
        // Drop cannot return an error.  The COPY coordinator calls
        // `dispose` on successful execution so an error from PostgreSQL stays
        // in the single outer utility error path; early-return cleanup here
        // deliberately preserves the original error instead.
        let _ = self.free();
    }
}

/// PostgreSQL-owned preparation for one COPY FROM execution.
pub struct CopyFromPreparation<'statement, 'parse> {
    raw: pg::LakebaseCopyPreparation,
    _statement_lifetime: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse_lifetime: PhantomData<&'parse CopyParseState>,
}

impl CopyFromPreparation<'_, '_> {
    pub(super) fn relation(&self) -> pg_sys::Relation {
        self.raw.relation
    }

    pub(super) fn where_clause(&self) -> *mut pg_sys::Node {
        self.raw.where_clause
    }

    pub fn column_layout(
        &self,
        statement: &CopyStatement<'_>,
    ) -> Result<CopyColumnLayout, CopyError> {
        let attnums = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::copy_attnums(
                    self.raw.relation,
                    statement.attlist(),
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        unsafe {
            CopyColumnLayout::from_descriptor((*self.raw.relation).rd_att, attnums)
        }
    }
}

impl Drop for CopyFromPreparation<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: the C preparation owns only the relation reference it
        // opened; disposing it after COPY state has ended releases that
        // reference and leaves PostgreSQL's lock held with NoLock semantics.
        let _ = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::dispose_preparation(&mut self.raw);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
    }
}

/// PostgreSQL-owned preparation for one COPY TO execution.
pub struct CopyToPreparation<'statement, 'parse> {
    raw: pg::LakebaseCopyPreparation,
    _statement_lifetime: PhantomData<&'statement pg_sys::CopyStmt>,
    _parse_lifetime: PhantomData<&'parse CopyParseState>,
}

impl CopyToPreparation<'_, '_> {
    pub(super) fn relation(&self) -> pg_sys::Relation {
        self.raw.relation
    }

    pub(super) fn raw_query(&self) -> *mut pg_sys::RawStmt {
        self.raw.raw_query
    }

    pub(super) fn query_relation(&self) -> pg_sys::Oid {
        self.raw.query_rel_id
    }
}

impl Drop for CopyToPreparation<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: the C preparation owns only the relation reference it
        // opened; disposing it after COPY state has ended releases that
        // reference and leaves PostgreSQL's lock held with NoLock semantics.
        let _ = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::dispose_preparation(&mut self.raw);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        };
    }
}
