//! Copy-object-safe REST table identity carried by an FDW plan.

use lagodb_core::fdw::{
    ForeignPlanPrivate, ForeignPrivateReader, ForeignPrivateWriter, ForeignScanError,
};

use super::super::options::{ForeignTableIdentity, ForeignTableMode};
use super::super::source_identity::PlanSourceIdentity;

#[derive(Debug, Clone)]
pub(crate) struct IcebergFdwScanPrivate {
    identity: ForeignTableIdentity,
    source: Option<PlanSourceIdentity>,
}

impl IcebergFdwScanPrivate {
    pub(crate) fn new(identity: ForeignTableIdentity) -> Self {
        Self {
            identity,
            source: None,
        }
    }

    pub(crate) fn with_source(
        identity: ForeignTableIdentity,
        source: Option<PlanSourceIdentity>,
    ) -> Self {
        Self { identity, source }
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn source(&self) -> Option<&PlanSourceIdentity> {
        self.source.as_ref()
    }
}

impl ForeignPlanPrivate for IcebergFdwScanPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignScanError> {
        writer
            .append_str(self.identity.catalog_name())
            .append_str(self.identity.namespace())
            .append_str(self.identity.table_name())
            .append_str(self.identity.mode().as_str())
            .append_bool(self.source.is_some());
        if let Some(source) = &self.source {
            source.encode(writer);
        }
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        let catalog_name = reader.read_str()?;
        let namespace = reader.read_str()?;
        let table_name = reader.read_str()?;
        let mode = ForeignTableMode::parse(&reader.read_str()?)?;
        let identity = ForeignTableIdentity::with_mode(
            catalog_name,
            namespace,
            table_name,
            mode,
        );
        let source = reader
            .read_bool()?
            .then(|| PlanSourceIdentity::decode(reader))
            .transpose()?;
        Ok(Self::with_source(identity, source))
    }
}
