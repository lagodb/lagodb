//! Copy-object-safe writable-table identity for a foreign modify plan.

use lagodb_core::fdw::{
    ForeignModifyError, ForeignModifyPrivate, ForeignPrivateReader,
    ForeignPrivateWriter,
};

use super::super::options::ForeignTableIdentity;

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
        self.identity.encode(writer);
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError> {
        Ok(Self::new(ForeignTableIdentity::decode(reader)?))
    }
}
