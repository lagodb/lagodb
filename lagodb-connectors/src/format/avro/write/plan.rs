//! Avro schema construction and PostgreSQL-to-Avro field binding.

use apache_avro::Schema;
use lagodb_core::datum::{ColumnDatumTarget, DatumConversionError};
use lagodb_core::handles::RelationColumn;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::super::AvroValueKind;

pub(crate) struct AvroWritePlan {
    schema: Schema,
    fields: Box<[AvroValueKind]>,
}

impl AvroWritePlan {
    pub(crate) fn from_relation_columns(
        columns: &[RelationColumn],
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Avro,
                    "PostgreSQL column names must be valid UTF-8 for Avro",
                )
            })?;
            fields.push((
                name.to_owned(),
                AvroValueKind::from_oid(column.type_oid(), column.type_mod())?,
            ));
        }
        Self::from_fields(fields)
    }

    pub(crate) fn from_copy_columns<'a>(
        columns: impl Iterator<Item = Result<(&'a str, pg_sys::Oid, i32), ConnectorError>>,
        count: usize,
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::with_capacity(count);
        for column in columns {
            let (name, oid, typmod) = column?;
            fields.push((name.to_owned(), AvroValueKind::from_oid(oid, typmod)?));
        }
        Self::from_fields(fields)
    }

    fn from_fields(
        fields: Vec<(String, AvroValueKind)>,
    ) -> Result<Self, ConnectorError> {
        if fields.iter().any(|(_, kind)| {
            matches!(kind, AvroValueKind::String | AvroValueKind::Name)
        }) {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(DatumConversionError::from)?;
        }
        let kinds = fields
            .iter()
            .map(|(_, kind)| *kind)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let schema_fields = fields
            .into_iter()
            .map(|(name, kind)| {
                serde_json::json!({
                    "name": name,
                    "type": ["null", kind.schema_json()],
                    "default": null,
                })
            })
            .collect::<Vec<_>>();
        let schema = serde_json::json!({
            "type": "record",
            "name": "lagodb_record",
            "fields": schema_fields,
        });
        Ok(Self {
            schema: Schema::parse_str(&schema.to_string())?,
            fields: kinds,
        })
    }

    pub(super) fn into_parts(self) -> (Schema, Box<[AvroValueKind]>) {
        (self.schema, self.fields)
    }
}
