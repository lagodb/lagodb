//! Schema-bound write plans shared by FDW INSERT and COPY TO.

use std::ffi::CStr;
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use lagodb_arrow::{
    BoundDatumBuffer, BoundDatumColumnPlan, BoundDatumPlan, BoundWriteBuffer,
    BoundWriteColumnPlan,
};
use lagodb_core::datum::PgType;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::type_mapping::ParquetTypeMapping;

pub(crate) struct ParquetWritePlan {
    schema: Arc<Schema>,
    columns: Vec<(usize, BoundDatumPlan)>,
}

impl ParquetWritePlan {
    pub(crate) fn bind<'a>(
        columns: impl IntoIterator<Item = (&'a CStr, PgType, usize)>,
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::new();
        let mut plans = Vec::new();
        for (name, ty, source) in columns {
            let name = name.to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet",
                )
            })?;
            if fields.iter().any(|field: &Field| field.name() == name) {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "Parquet output column names must be unique",
                ));
            }
            let field = ParquetTypeMapping::write_field(name, ty)?;
            // The field was generated from this declaration, so it is already
            // provider-approved. Only the codec remains to be bound.
            plans.push((source, BoundDatumPlan::bind(&field, ty)?));
            fields.push(field);
        }
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            columns: plans,
        })
    }

    pub(crate) fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub(crate) fn into_slot_buffer(
        self,
        width: usize,
    ) -> Result<BoundWriteBuffer, ConnectorError> {
        let columns = self
            .columns
            .into_iter()
            .map(|(source, plan)| BoundWriteColumnPlan::bind(plan, source, width))
            .collect::<Result<Box<[_]>, _>>()?;
        Ok(BoundWriteBuffer::new(self.schema, columns)?)
    }

    /// COPY binds a newly generated schema in exactly its output layout order.
    pub(super) fn into_datum_buffer(
        self,
    ) -> Result<BoundDatumBuffer, ConnectorError> {
        let columns = self
            .columns
            .into_iter()
            .map(|(_, plan)| BoundDatumColumnPlan::new(plan))
            .collect();
        Ok(BoundDatumBuffer::new(self.schema, columns)?)
    }
}
