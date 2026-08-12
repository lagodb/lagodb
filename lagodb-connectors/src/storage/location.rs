//! Exact-object and prefix classification.

use crate::error::ConnectorError;
use crate::format::FormatKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectLocationKind {
    Exact,
    Prefix,
}

impl ObjectLocationKind {
    pub(crate) fn classify(
        key: &str,
        format: FormatKind,
    ) -> Result<Self, ConnectorError> {
        match FormatKind::infer_from_key(key) {
            Some(found) if found != format => Err(ConnectorError::invalid_option(
                "path",
                "object suffix conflicts with the selected format",
            )),
            Some(_) if format.matches_object_key(key) && !key.ends_with('/') => {
                Ok(Self::Exact)
            }
            Some(_) => Err(ConnectorError::invalid_option(
                "path",
                "stream compression suffixes are not valid for Parquet or Avro objects",
            )),
            _ => Ok(Self::Prefix),
        }
    }
}
