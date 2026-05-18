//! Shared filesystem-path encoding used by both the cache ([`crate::cache::path::CachePathResolver`])
//! and the staging area ([`crate::staging::path::StagingPathResolver`]).
//!
//! Both places need to turn an [`super::ObjectLocation`] into a deterministic on-disk path that:
//!
//! * stays inside a configured root (no directory escape via `..` segments, odd bytes, etc.),
//! * is reversible for diagnostics / orphan scans,
//! * keeps each segment and the full path within portable length limits.
//!
//! The functions in this module are the pure helpers shared by the two resolvers; they
//! intentionally do not know anything about the cache or staging roots, the file-name prefix, or
//! the suffix that distinguishes `.complete` / `.part` / `.staging` files.

use std::fmt::Write;
use std::path::{Component, Path};

use crate::error::{StorageError, StorageResult};

use super::ObjectLocation;

/// Component placeholder for empty key path segments (e.g. `"a//b"`). We refuse to collapse these
/// silently because doing so would break reversibility.
pub(crate) const EMPTY_SEGMENT: &str = "%empty";

/// Portable upper bound on any single path component.
pub(crate) const MAX_COMPONENT_LEN: usize = 255;

/// Portable upper bound on the total path length (including the configured root).
pub(crate) const MAX_PATH_LEN: usize = 4095;

/// Percent-encodes a single key segment so it is safe to use as one path component. The encoding
/// is reversible by [`decode_segment`] and guarantees the result never matches `"."` or `".."` in
/// contexts that would allow directory escape.
pub(crate) fn encode_segment(segment: &str) -> String {
    if segment.is_empty() {
        return EMPTY_SEGMENT.into();
    }

    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' => {
                encoded.push(*byte as char)
            }
            b'.' if segment != "." && segment != ".." => encoded.push('.'),
            other => {
                write!(encoded, "%{other:02x}").expect("infallible write to String")
            }
        }
    }
    encoded
}

/// Inverse of [`encode_segment`]. Returns `None` if the input was not produced by that function
/// (malformed percent escape, non-UTF-8 result, …).
pub(crate) fn decode_segment(segment: &str) -> Option<String> {
    if segment == EMPTY_SEGMENT {
        return Some(String::new());
    }

    let mut bytes = Vec::with_capacity(segment.len());
    let raw = segment.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%' {
            let hex = raw.get(index + 1..index + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            bytes.push(raw[index]);
            index += 1;
        }
    }
    String::from_utf8(bytes).ok()
}

/// Collects the normal components of `path` as UTF-8 strings, refusing `.`, `..`, root, and any
/// non-UTF-8 component. Used by reverse parsing to recover the encoded segments from a filesystem
/// path.
pub(crate) fn normal_components(path: &Path) -> Option<Vec<&str>> {
    let mut components = Vec::new();
    for component in path.components() {
        let Component::Normal(value) = component else {
            return None;
        };
        components.push(value.to_str()?);
    }
    Some(components)
}

/// Builds the encoded directory structure for an [`ObjectLocation`] and appends a leaf filename.
///
/// Both the cache and staging resolvers share this layout: `<store_id>/<bucket>/<key_dirs>/<leaf>`,
/// where each component is percent-encoded by [`encode_segment`]. The `leaf_prefix` and optional
/// `leaf_suffix` are prepended/appended to the encoded final key segment to form the filename.
pub(crate) fn build_encoded_object_path(
    key: &ObjectLocation,
    leaf_prefix: &str,
    leaf_suffix: &str,
) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(encode_segment(key.store_id().as_str()));
    path.push(encode_segment(key.bucket()));
    let key_str = key.key();
    let (dir_part, file_segment) = match key_str.rsplit_once('/') {
        Some((dir, file)) => (Some(dir), file),
        None => (None, key_str),
    };
    if let Some(dir) = dir_part {
        for segment in dir.split('/') {
            path.push(encode_segment(segment));
        }
    }
    path.push(format!(
        "{leaf_prefix}{}{leaf_suffix}",
        encode_segment(file_segment)
    ));
    path
}

/// Ensures `path` stays within [`MAX_COMPONENT_LEN`] per component and [`MAX_PATH_LEN`] overall.
///
/// Opens reject cache / staging keys whose derived on-disk path would exceed these bounds rather
/// than silently truncating or hashing; the concrete resolvers call this from their `path_for`
/// entry points.
pub(crate) fn validate_portable_path(
    key: &ObjectLocation,
    path: &Path,
) -> StorageResult<()> {
    if path.as_os_str().len() >= MAX_PATH_LEN {
        return Err(StorageError::invalid_path(format!(
            "path for {key} exceeds maximum path length of {MAX_PATH_LEN} bytes"
        )));
    }
    for component in path.components() {
        if let Component::Normal(value) = component
            && value.len() > MAX_COMPONENT_LEN
        {
            return Err(StorageError::invalid_path(format!(
                "path component for {key} exceeds maximum component length of {MAX_COMPONENT_LEN} bytes"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn encode_decode_roundtrip_for_alphanumeric_segments() {
        for input in ["file", "file.txt", "MixedCase-1_2.tar", ""] {
            let encoded = encode_segment(input);
            assert_eq!(decode_segment(&encoded).as_deref(), Some(input));
        }
    }

    #[test]
    fn encode_neutralises_parent_directory_segments() {
        assert_eq!(encode_segment(".."), "%2e%2e");
        assert_eq!(encode_segment("."), "%2e");
        assert_eq!(decode_segment(&encode_segment("..")).as_deref(), Some(".."));
    }

    #[test]
    fn decode_rejects_malformed_percent_escape() {
        assert!(decode_segment("%9").is_none());
        assert!(decode_segment("%xy").is_none());
    }

    #[test]
    fn validate_portable_path_rejects_overlong_components() {
        let key = ObjectLocation::new("store", "bucket", "file").unwrap();
        let long_component = "x".repeat(MAX_COMPONENT_LEN + 1);
        let path = PathBuf::from("/tmp").join(&long_component);
        let error = validate_portable_path(&key, &path).unwrap_err();
        assert!(matches!(error, StorageError::InvalidPath { .. }));
    }

    #[test]
    fn normal_components_rejects_non_normal_entries() {
        assert!(normal_components(Path::new("/absolute/path")).is_none());
        assert!(normal_components(Path::new("../escape")).is_none());
        assert_eq!(
            normal_components(Path::new("a/b/c")).as_deref(),
            Some(&["a", "b", "c"][..])
        );
    }
}
