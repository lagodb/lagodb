//! Copy-object-safe REST table identity carried by an FDW plan.

use lagodb_core::fdw::{
    ForeignPlanPrivate, ForeignPrivateReader, ForeignPrivateWriter, ForeignScanError,
};

use super::super::options::ForeignTableIdentity;
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
        self.identity.encode(writer);
        writer.append_bool(self.source.is_some());
        if let Some(source) = &self.source {
            source.encode(writer);
        }
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignScanError> {
        let identity = ForeignTableIdentity::decode(reader)?;
        let source = reader
            .read_bool()?
            .then(|| PlanSourceIdentity::decode(reader))
            .transpose()?;
        Ok(Self::with_source(identity, source))
    }
}
