use iceberg_lite::{Error, ErrorKind, Result};

/// Resolve an Iceberg URI to its namespace-relative, volume-rooted key offset.
pub(super) fn resolve_object_uri(
    effective_base_uri: &str,
    uri: &str,
) -> Result<usize> {
    let (_, namespace_and_root) = effective_base_uri
        .split_once("://")
        .expect("tablespace binding validated the effective base URI");
    let (_, root) = namespace_and_root
        .split_once('/')
        .expect("tablespace binding validated the effective root key");

    if uri.contains("://") {
        let Some(suffix) = uri.strip_prefix(effective_base_uri) else {
            return Err(outside_root(uri, effective_base_uri));
        };
        if !suffix.starts_with('/') || suffix.len() == 1 {
            return Err(outside_root(uri, effective_base_uri));
        }
        return Ok(effective_base_uri.len() - root.len());
    }

    if uri
        .strip_prefix(root)
        .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
    {
        return Ok(0);
    }

    Err(outside_root(uri, effective_base_uri))
}

fn outside_root(uri: &str, effective_base_uri: &str) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!(
            "object URI {:?} is outside storage root {:?}",
            uri, effective_base_uri
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "s3://my-lake/lagodb/7";

    #[test]
    fn strips_the_effective_root() {
        let uri = "s3://my-lake/lagodb/7/metadata/v1.json";
        let offset = resolve_object_uri(ROOT, uri).unwrap();
        assert_eq!(&uri[offset..], "lagodb/7/metadata/v1.json");
    }

    #[test]
    fn rejects_root_and_foreign_uris() {
        assert!(resolve_object_uri(ROOT, ROOT).is_err());
        assert!(resolve_object_uri(ROOT, "s3://my-lake/lagodb/70/file").is_err());
        assert!(resolve_object_uri(ROOT, "gs://my-lake/lagodb/7/file").is_err());
    }

    #[test]
    fn accepts_only_a_volume_rooted_relative_key() {
        assert_eq!(
            resolve_object_uri(ROOT, "lagodb/7/metadata/v1.json").unwrap(),
            0
        );
        assert!(resolve_object_uri(ROOT, "metadata/v1.json").is_err());
        assert!(resolve_object_uri(ROOT, "").is_err());
    }
}
