//! Copy-object-safe writable-table identity for a foreign modify plan.

use lagodb_core::fdw::{
    ForeignModifyError, ForeignModifyPrivate, ForeignPrivateReader,
    ForeignPrivateWriter,
};

use super::super::options::{ForeignTableIdentity, ForeignTableMode};

#[derive(Debug, Clone)]
pub(crate) struct IcebergFdwModifyPrivate {
    identity: ForeignTableIdentity,
}

impl IcebergFdwModifyPrivate {
    pub(crate) fn new(identity: ForeignTableIdentity) -> Self {
        Self { identity }
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }
}

impl ForeignModifyPrivate for IcebergFdwModifyPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignModifyError> {
        writer
            .append_str(self.identity.catalog_name())
            .append_str(self.identity.namespace())
            .append_str(self.identity.table_name())
            .append_str(self.identity.mode().as_str());
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError> {
        let catalog_name = reader.read_str()?;
        let namespace = reader.read_str()?;
        let table_name = reader.read_str()?;
        let mode = ForeignTableMode::parse(&reader.read_str()?)?;
        Ok(Self::new(ForeignTableIdentity::with_mode(
            catalog_name,
            namespace,
            table_name,
            mode,
        )))
    }
}
