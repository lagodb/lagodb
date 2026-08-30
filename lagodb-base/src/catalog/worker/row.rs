use std::ffi::CStr;
use std::fmt;

use lagodb_core::handles::{HeapTupleGuard, HeapTupleRef};
use pgrx::{FromDatum, IntoDatum, pg_sys};

use super::WorkerId;

#[derive(Clone, Copy)]
#[repr(i16)]
enum Column {
    WorkerId = 1,
    ExtensionName = 2,
    WorkerName = 3,
    EntrypointSchema = 4,
    EntrypointFunction = 5,
}

impl Column {
    const COUNT: usize = 5;

    const fn attno(self) -> pg_sys::AttrNumber {
        self as pg_sys::AttrNumber
    }

    const fn index(self) -> usize {
        self as usize - 1
    }
}

#[derive(Clone, Copy)]
enum NameColumn {
    ExtensionName,
    EntrypointSchema,
    EntrypointFunction,
}

impl NameColumn {
    const fn column(self) -> Column {
        match self {
            Self::ExtensionName => Column::ExtensionName,
            Self::EntrypointSchema => Column::EntrypointSchema,
            Self::EntrypointFunction => Column::EntrypointFunction,
        }
    }
}

/// An owned PostgreSQL `name` value in the database server encoding.
#[derive(Clone)]
pub(crate) struct CatalogName {
    data: pg_sys::NameData,
}

impl CatalogName {
    pub(crate) fn from_c_str(value: &CStr) -> Self {
        let mut data = pg_sys::NameData::default();
        // SAFETY: `value` is NUL-terminated. PostgreSQL `namestrcpy` copies it
        // into a complete NameData value using the server-encoding byte
        // representation without converting it to UTF-8.
        unsafe { pg_sys::namestrcpy(&mut data, value.as_ptr()) };
        Self { data }
    }

    /// # Safety
    ///
    /// `data` must be a PostgreSQL-produced `NameData` value containing a NUL
    /// terminator within its fixed-width buffer.
    pub(crate) const unsafe fn from_name_data(data: pg_sys::NameData) -> Self {
        Self { data }
    }

    pub(crate) fn as_c_str(&self) -> &CStr {
        // SAFETY: values are created either by `namestrcpy` or by copying a
        // PostgreSQL NameData value; both guarantee NUL termination.
        unsafe { CStr::from_ptr(self.data.data.as_ptr()) }
    }

    fn datum(&self) -> pg_sys::Datum {
        // SAFETY: `data` is a live PostgreSQL NameData value for the duration
        // of the caller's `heap_form_tuple` call.
        unsafe { pg_sys::NameGetDatum(&self.data) }
    }
}

impl fmt::Debug for CatalogName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_c_str().fmt(formatter)
    }
}

#[derive(Debug)]
pub(crate) struct WorkerRegistrationRow {
    pub(crate) worker_id: WorkerId,
    pub(crate) extension_name: CatalogName,
    pub(crate) worker_name: String,
    pub(crate) entrypoint_schema: CatalogName,
    pub(crate) entrypoint_function: CatalogName,
}

pub(crate) struct NewWorkerRegistration<'a> {
    pub(crate) extension_name: &'a CStr,
    pub(crate) worker_name: &'a str,
    pub(crate) entrypoint_schema: &'a CStr,
    pub(crate) entrypoint_function: &'a CStr,
}

/// Typed view and codec for one tuple from `lagodb.workers`.
pub(super) struct WorkerTuple<'a> {
    tuple: HeapTupleRef<'a>,
    tuple_desc: pg_sys::TupleDesc,
}

impl<'a> WorkerTuple<'a> {
    pub(super) const fn worker_id_attno() -> pg_sys::AttrNumber {
        Column::WorkerId.attno()
    }

    pub(super) const fn worker_name_attno() -> pg_sys::AttrNumber {
        Column::WorkerName.attno()
    }

    /// # Safety
    ///
    /// `tuple_desc` must be the descriptor for the live `lagodb.workers`
    /// tuple. Its five columns are the NOT NULL types declared in
    /// `bootstrap.sql`.
    pub(super) unsafe fn new(
        tuple: HeapTupleRef<'a>,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Self {
        Self { tuple, tuple_desc }
    }

    pub(super) fn decode(&self) -> WorkerRegistrationRow {
        WorkerRegistrationRow {
            worker_id: self.worker_id(),
            extension_name: self.catalog_name(NameColumn::ExtensionName),
            worker_name: self.worker_name(),
            entrypoint_schema: self.catalog_name(NameColumn::EntrypointSchema),
            entrypoint_function: self.catalog_name(NameColumn::EntrypointFunction),
        }
    }

    pub(super) fn worker_id(&self) -> WorkerId {
        // SAFETY: WorkerId is the int4 column fixed by the tuple schema.
        WorkerId(unsafe { pg_sys::DatumGetInt32(self.datum(Column::WorkerId)) })
    }

    pub(super) fn extension_name_eq(&self, extension_name: &CStr) -> bool {
        self.name(NameColumn::ExtensionName).to_bytes() == extension_name.to_bytes()
    }

    fn catalog_name(&self, column: NameColumn) -> CatalogName {
        // SAFETY: NameColumn can only select one of the PostgreSQL `name`
        // columns in lagodb.workers, whose NameData value is NUL-terminated.
        unsafe { CatalogName::from_name_data(*self.name_data(column)) }
    }

    fn worker_name(&self) -> String {
        let datum = self.datum(Column::WorkerName);
        // SAFETY: WorkerName is the NOT NULL text column fixed by the tuple
        // schema and worker registration accepts Rust UTF-8 strings.
        unsafe { String::from_datum(datum, false) }
            .expect("lagodb.workers.worker_name must contain a valid text Datum")
    }

    fn name(&self, column: NameColumn) -> &CStr {
        let name = self.name_data(column);
        // SAFETY: the selected column is a PostgreSQL `name` column. The tuple
        // borrow keeps its fixed-width, NUL-terminated NameData live.
        unsafe { CStr::from_ptr(name.data.as_ptr()) }
    }

    fn name_data(&self, column: NameColumn) -> &pg_sys::NameData {
        let name = self
            .datum(column.column())
            .cast_mut_ptr::<pg_sys::NameData>();
        // SAFETY: the selected column is a PostgreSQL `name` column and the
        // tuple borrow keeps its fixed-width Datum live.
        unsafe { &*name }
    }

    fn datum(&self, column: Column) -> pg_sys::Datum {
        let mut is_null = false;
        // SAFETY: the constructor requires a live tuple and its matching
        // descriptor. All worker catalog columns are declared NOT NULL.
        unsafe {
            pg_sys::heap_getattr(
                self.tuple.as_raw(),
                column.attno().into(),
                self.tuple_desc,
                &mut is_null,
            )
        }
    }

    /// # Safety
    ///
    /// `tuple_desc` must be the live descriptor for `lagodb.workers`.
    pub(super) unsafe fn encode(
        registration: &NewWorkerRegistration<'_>,
        worker_id: WorkerId,
        tuple_desc: pg_sys::TupleDesc,
    ) -> HeapTupleGuard {
        let extension_name = CatalogName::from_c_str(registration.extension_name);
        let entrypoint_schema =
            CatalogName::from_c_str(registration.entrypoint_schema);
        let entrypoint_function =
            CatalogName::from_c_str(registration.entrypoint_function);
        let mut values = [pg_sys::Datum::from(0_usize); Column::COUNT];
        let mut nulls = [false; Column::COUNT];
        values[Column::WorkerId.index()] = pg_sys::Datum::from(worker_id.as_i32());
        values[Column::ExtensionName.index()] = extension_name.datum();
        values[Column::WorkerName.index()] = registration
            .worker_name
            .into_datum()
            .expect("str converts to Datum");
        values[Column::EntrypointSchema.index()] = entrypoint_schema.datum();
        values[Column::EntrypointFunction.index()] = entrypoint_function.datum();
        // SAFETY: values and nulls match the five NOT NULL lagodb.workers
        // columns, and heap_form_tuple copies pass-by-reference inputs.
        unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            ))
        }
    }
}
