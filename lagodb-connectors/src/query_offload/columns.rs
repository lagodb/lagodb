//! Declared columns of a Parquet table scan and their canonical Arrow schema.

use std::ffi::CStr;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::format::parquet::{DeclaredColumn, ParquetColumnarPlan};

use super::error::Error;

/// Projected columns in output order: attribute numbers, declared types and
/// the Arrow schema the scan emits.
pub(super) struct ScanColumns {
    attnos: Box<[pg_sys::AttrNumber]>,
    types: Box<[PgType]>,
    schema: SchemaRef,
}

impl ScanColumns {
    /// `None` when a projected declaration has no columnar representation.
    /// The caller opened `relation` under the lock its phase requires.
    pub(super) fn load(
        relation: &RelationHandle<'_>,
        attnos: &[pg_sys::AttrNumber],
    ) -> Result<Option<Self>, Error> {
        let attributes = relation.tuple_desc_handle().attributes();
        let mut declared = Vec::with_capacity(attnos.len());
        for attno in attnos {
            // TupleDescAttr(attno - 1). The projection names live user
            // columns; the held relation lock keeps them from being dropped.
            let attribute = usize::try_from(*attno)
                .ok()
                .and_then(|attno| attno.checked_sub(1))
                .and_then(|index| attributes.get(index))
                .filter(|attribute| !attribute.attisdropped)
                .ok_or(Error::Invariant(
                    "a projected attribute is not a live column",
                ))?;
            // SAFETY: NameData is NUL-terminated and owned by the open
            // relation's descriptor, which outlives this call.
            let name = unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) }
                .to_str()
                .map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "PostgreSQL column names must be valid UTF-8 for Parquet",
                    )
                })?;
            declared.push(DeclaredColumn {
                name,
                pg_type: PgType::new(attribute.atttypid, attribute.atttypmod),
            });
        }
        let types = declared.iter().map(|column| column.pg_type).collect();
        let Some(schema) = ParquetColumnarPlan::declared_schema(declared)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            attnos: attnos.into(),
            types,
            schema,
        }))
    }

    /// The columns at `indices`, a subprojection in output order.
    pub(super) fn project(&self, indices: &[usize]) -> Result<Self, Error> {
        let column = |index: &usize| {
            self.attnos.get(*index).zip(self.types.get(*index)).ok_or(
                Error::Invariant("a scan projection exceeds the bound schema"),
            )
        };
        let columns = indices.iter().map(column).collect::<Result<Vec<_>, _>>()?;
        let schema = self.schema.project(indices).map_err(ConnectorError::from)?;
        Ok(Self {
            attnos: columns.iter().map(|(attno, _)| **attno).collect(),
            types: columns.iter().map(|(_, pg_type)| **pg_type).collect(),
            schema: Arc::new(schema),
        })
    }

    pub(super) fn attnos(&self) -> &[pg_sys::AttrNumber] {
        &self.attnos
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn is_empty(&self) -> bool {
        self.attnos.is_empty()
    }

    /// Bind this projection to one file's schema.
    pub(super) fn bind(
        &self,
        file: &Schema,
    ) -> Result<ParquetColumnarPlan, ConnectorError> {
        ParquetColumnarPlan::bind(file, &self.schema, &self.types)
    }
}
