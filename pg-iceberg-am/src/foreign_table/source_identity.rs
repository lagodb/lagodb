//! Plan-scoped identity for one loaded Iceberg table generation.

use iceberg_lite::table::Table;
use pg_lakebase_core::plan_data::{PlanDataReader, PlanDataWriter};
use uuid::{Uuid, fmt::Hyphenated};

use super::error::IcebergFdwError;
use crate::error::IcebergError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanSourceIdentity {
    table_uuid: Uuid,
    schema_id: i32,
}

impl PlanSourceIdentity {
    pub(crate) fn from_table(table: &Table) -> Self {
        Self {
            table_uuid: table.metadata().uuid(),
            schema_id: table.metadata().current_schema().schema_id(),
        }
    }

    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        let mut encoded_uuid = [0_u8; Hyphenated::LENGTH];
        writer
            .append_str(self.table_uuid.hyphenated().encode_lower(&mut encoded_uuid))
            .append_i32(self.schema_id);
    }

    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, IcebergFdwError> {
        let table_uuid = Uuid::parse_str(&reader.read_str()?)
            .map_err(IcebergError::UuidConversionError)?;
        Ok(Self {
            table_uuid,
            schema_id: reader.read_i32()?,
        })
    }
}
