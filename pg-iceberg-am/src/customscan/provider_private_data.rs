//! Iceberg provider payload stored in `CustomScan.custom_private`.

use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
use pg_lakebase_core::customscan::provider::CustomScanError;
use pgrx::pg_sys;

/// `CustomScan.custom_private` payload: the scan target's tablespace OID
/// (copyObject-safe, single `T_Integer`). Metadata binds at provider `begin`
/// against `estate.es_snapshot`.
#[derive(Debug, Clone)]
pub struct IcebergPrivateData {
    /// Captured at `create_path` for use in [`ScanSpec`](crate::access::scan::ScanSpec) construction.
    pub tablespace_oid: pg_sys::Oid,
}

impl CustomScanPrivate for IcebergPrivateData {
    fn encode(&self, writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        writer.append_oid(self.tablespace_oid);
        Ok(())
    }

    /// Fails closed on empty or malformed payloads; `InvalidOid` is a valid value.
    fn decode(reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        let tablespace_oid = reader.read_oid()?;
        Ok(Self { tablespace_oid })
    }
}
