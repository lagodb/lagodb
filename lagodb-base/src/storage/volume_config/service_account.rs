use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Validated canonical JSON for the provider-defined service-account object.
///
/// Keeping the canonical string avoids retaining an untyped mutable JSON map
/// in the runtime domain while preserving the public object-shaped JSON API.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ServiceAccountJson(String);

impl ServiceAccountJson {
    pub(crate) fn as_json(&self) -> &str {
        &self.0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0 == "{}"
    }
}

impl Serialize for ServiceAccountJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value: Value =
            serde_json::from_str(&self.0).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ServiceAccountJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let object = serde_json::Map::<String, Value>::deserialize(deserializer)?;
        let canonical =
            serde_json::to_string(&object).map_err(serde::de::Error::custom)?;
        Ok(Self(canonical))
    }
}
