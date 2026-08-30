use serde::{Deserialize, Serialize};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};

/// Validated canonical JSON for the provider-defined service-account object.
///
/// Owning the raw canonical value avoids retaining an untyped mutable JSON map
/// and lets serde_json serialize the object without reparsing it.
#[derive(Clone)]
pub(crate) struct ServiceAccountJson(Box<RawValue>);

impl PartialEq for ServiceAccountJson {
    fn eq(&self, other: &Self) -> bool {
        self.as_json() == other.as_json()
    }
}

impl Eq for ServiceAccountJson {}

impl ServiceAccountJson {
    pub(crate) fn as_json(&self) -> &str {
        self.0.get()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.as_json() == "{}"
    }
}

impl Serialize for ServiceAccountJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ServiceAccountJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let object = serde_json::Map::<String, Value>::deserialize(deserializer)?;
        let raw = to_raw_value(&object).map_err(serde::de::Error::custom)?;
        Ok(Self(raw))
    }
}
