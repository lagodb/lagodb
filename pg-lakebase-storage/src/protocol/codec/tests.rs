use crate::backend::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3StoreConfig,
    SecretString, StoreConfig,
};
use crate::error::StorageErrorKind;
use crate::handle::{FileHandle, OpenFlags};

use super::super::model::{
    WireRequest, WireRequestPayload, WireResponse, WireResponsePayload,
};
use super::{
    decode_request, decode_response, encode_read_request, encode_request,
    encode_response,
};

#[test]
fn decode_rejects_open_store_id_length_claim_beyond_frame() {
    let mut frame = encode_request(&WireRequest {
        request_id: 42,
        payload: WireRequestPayload::Open {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
            flags: OpenFlags::READ_ONLY,
        },
    })
    .unwrap();
    // After fixed header (15 B): op (2) + flags (1), then store id length u32.
    const STORE_ID_LEN_OFFSET: usize = 15 + 2 + 1;
    frame[STORE_ID_LEN_OFFSET..STORE_ID_LEN_OFFSET + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_request(&frame).is_err());
}

#[test]
fn decode_rejects_previous_protocol_version() {
    let mut frame = encode_request(&WireRequest {
        request_id: 42,
        payload: WireRequestPayload::Close {
            handle: FileHandle(7),
        },
    })
    .unwrap();
    frame[4..6].copy_from_slice(&2_u16.to_be_bytes());

    let error = decode_request(&frame).unwrap_err();

    assert!(
        error
            .wire_message()
            .contains("unsupported protocol version 2")
    );
}

#[test]
fn decode_rejects_stage_create_payload_length_claim_beyond_frame() {
    let mut frame = encode_request(&WireRequest {
        request_id: 1,
        payload: WireRequestPayload::StageCreate {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
        },
    })
    .unwrap();
    // Header (15) + op (2), then store id length u32.
    const STORE_ID_LEN_OFFSET: usize = 15 + 2;
    frame[STORE_ID_LEN_OFFSET..STORE_ID_LEN_OFFSET + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_request(&frame).is_err());
}

#[test]
fn decode_rejects_open_key_over_string_field_cap() {
    const CAP: usize = 1024 * 1024;
    let request = WireRequest {
        request_id: 1,
        payload: WireRequestPayload::Open {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "x".repeat(CAP + 1),
            flags: OpenFlags::READ_ONLY,
        },
    };
    let frame = encode_request(&request).unwrap();
    assert!(decode_request(&frame).is_err());
}

#[test]
fn request_payloads_roundtrip() {
    for payload in [
        WireRequestPayload::Open {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
            flags: OpenFlags::READ_ONLY,
        },
        WireRequestPayload::Head {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
        },
        WireRequestPayload::Read {
            handle: FileHandle(7),
            offset: 11,
            len: 13,
        },
        WireRequestPayload::Close {
            handle: FileHandle(7),
        },
        WireRequestPayload::StageCreate {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "upload.txt".to_string(),
        },
        WireRequestPayload::Commit {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "upload.txt".to_string(),
        },
        WireRequestPayload::Abort {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "upload.txt".to_string(),
        },
        WireRequestPayload::RegisterStore {
            store_id: "store-a".to_string(),
            config: StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: Some("us-east-1".to_string()),
                access_key_id: Some(SecretString::new("access")),
                secret_access_key: Some(SecretString::new("secret")),
                token: None,
                allow_http: true,
                virtual_hosted_style_request: false,
                skip_signature: false,
            }),
        },
        WireRequestPayload::UnregisterStore {
            store_id: "store-a".to_string(),
        },
        WireRequestPayload::PurgeStoreCache {
            store_id: "store-a".to_string(),
        },
        WireRequestPayload::InvalidateObjectCache {
            store_id: "store-a".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
        },
        WireRequestPayload::Delete {
            store_id: "store-a".to_string(),
            bucket: "bucket".to_string(),
            key: "doomed".to_string(),
        },
        WireRequestPayload::DeletePrefix {
            store_id: "store-a".to_string(),
            bucket: "bucket".to_string(),
            prefix: "scope/".to_string(),
        },
        WireRequestPayload::List {
            store_id: "store-a".to_string(),
            bucket: "bucket".to_string(),
            prefix: Some("scope/".to_string()),
            page_size: 1000,
            cursor: None,
        },
        WireRequestPayload::List {
            store_id: "store-a".to_string(),
            bucket: "bucket".to_string(),
            prefix: None,
            page_size: 0,
            cursor: Some(crate::protocol::ListCursor::from_wire(
                "ls-deadbeef".to_string(),
            )),
        },
    ] {
        let request = WireRequest {
            request_id: 42,
            payload,
        };
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }
}

#[test]
fn response_payloads_roundtrip() {
    for payload in [
        WireResponsePayload::Open {
            handle: FileHandle(7),
            size: 99,
            direct_io: true,
        },
        WireResponsePayload::Head {
            size: 99,
            etag: Some("abc123".to_string()),
        },
        WireResponsePayload::Read {
            data: b"abc".to_vec(),
            eof: true,
        },
        WireResponsePayload::Close,
        WireResponsePayload::StageCreate {
            staging_path: "/tmp/cache/staging/default/bucket/pgl-staging.file"
                .to_string(),
        },
        WireResponsePayload::Commit {
            size: 42,
            etag: Some("abc123".to_string()),
        },
        WireResponsePayload::Abort,
        WireResponsePayload::RegisterStore { replaced: true },
        WireResponsePayload::UnregisterStore { removed: true },
        WireResponsePayload::PurgeStoreCache,
        WireResponsePayload::InvalidateObjectCache { removed: true },
        WireResponsePayload::Delete,
        WireResponsePayload::DeletePrefix { deleted: 7 },
        WireResponsePayload::List {
            entries: vec![
                crate::protocol::WireListEntry {
                    key: "scope/a".to_string(),
                    size: 11,
                    etag: Some("etag-a".to_string()),
                },
                crate::protocol::WireListEntry {
                    key: "scope/b".to_string(),
                    size: 22,
                    etag: None,
                },
            ],
            next_cursor: Some(crate::protocol::ListCursor::from_wire(
                "ls-cafef00d".to_string(),
            )),
        },
        WireResponsePayload::List {
            entries: Vec::new(),
            next_cursor: None,
        },
        WireResponsePayload::Error {
            kind: StorageErrorKind::Busy,
            message: "cache object is active".to_string(),
        },
    ] {
        let response = WireResponse {
            request_id: 42,
            payload,
        };
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded).unwrap(), response);
    }
}

#[test]
fn register_store_config_variants_roundtrip() {
    for config in [
        StoreConfig::S3(S3StoreConfig {
            region: Some("us-west-2".to_string()),
            endpoint: Some("https://s3.us-west-2.amazonaws.com".to_string()),
            access_key_id: Some(SecretString::new("aws-access")),
            secret_access_key: Some(SecretString::new("aws-secret")),
            token: Some(SecretString::new("aws-token")),
            allow_http: false,
            virtual_hosted_style_request: true,
            skip_signature: false,
        }),
        StoreConfig::S3Compatible(S3CompatibleStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: Some("us-east-1".to_string()),
            access_key_id: Some(SecretString::new("compat-access")),
            secret_access_key: Some(SecretString::new("compat-secret")),
            token: None,
            allow_http: true,
            virtual_hosted_style_request: false,
            skip_signature: false,
        }),
        StoreConfig::Gcs(GcsStoreConfig {
            base_url: Some("https://storage.googleapis.com".to_string()),
            service_account_path: Some("/tmp/service-account.json".to_string()),
            service_account_key: Some(SecretString::new(
                "{\"type\":\"service_account\"}",
            )),
            application_credentials_path: Some(
                "/tmp/application-default.json".to_string(),
            ),
            skip_signature: true,
        }),
        StoreConfig::Azure(AzureStoreConfig {
            account: Some("account".to_string()),
            endpoint: Some("http://127.0.0.1:10000".to_string()),
            access_key: Some(SecretString::new("azure-access")),
            bearer_token: Some(SecretString::new("azure-token")),
            client_id: Some("client-id".to_string()),
            client_secret: Some(SecretString::new("client-secret")),
            tenant_id: Some("tenant-id".to_string()),
            allow_http: true,
            use_emulator: false,
        }),
    ] {
        let request = WireRequest {
            request_id: 42,
            payload: WireRequestPayload::RegisterStore {
                store_id: "store-a".to_string(),
                config,
            },
        };
        let encoded = encode_request(&request).unwrap();

        assert_eq!(decode_request(&encoded).unwrap(), request);
    }
}

#[test]
fn request_open_golden_frame() {
    let request = WireRequest {
        request_id: 42,
        payload: WireRequestPayload::Open {
            store_id: "default".to_string(),
            bucket: "bucket".to_string(),
            key: "file".to_string(),
            flags: OpenFlags::READ_ONLY,
        },
    };

    assert_eq!(
        encode_request(&request).unwrap(),
        vec![
            0x53, 0x54, 0x47, 0x31, // magic
            0x00, 0x03, // version
            0x01, // request kind
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // request id
            0x00, 0x01, // open op
            0x01, // read-only flags
            0x00, 0x00, 0x00, 0x07, // store id len
            b'd', b'e', b'f', b'a', b'u', b'l', b't', 0x00, 0x00, 0x00,
            0x06, // bucket len
            b'b', b'u', b'c', b'k', b'e', b't', 0x00, 0x00, 0x00,
            0x04, // key len
            b'f', b'i', b'l', b'e',
        ]
    );
}

#[test]
fn request_read_golden_frame() {
    let request = WireRequest {
        request_id: 42,
        payload: WireRequestPayload::Read {
            handle: FileHandle(7),
            offset: 11,
            len: 13,
        },
    };

    assert_eq!(
        encode_request(&request).unwrap(),
        vec![
            0x53, 0x54, 0x47, 0x31, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00,
            0x0d,
        ]
    );
}

#[test]
fn fixed_read_request_encoder_matches_general_encoder() {
    let request = WireRequest {
        request_id: 42,
        payload: WireRequestPayload::Read {
            handle: FileHandle(7),
            offset: 11,
            len: 13,
        },
    };

    assert_eq!(
        encode_read_request(42, FileHandle(7), 11, 13).as_slice(),
        encode_request(&request).unwrap()
    );
}

#[test]
fn response_open_golden_frame() {
    let response = WireResponse {
        request_id: 42,
        payload: WireResponsePayload::Open {
            handle: FileHandle(7),
            size: 99,
            direct_io: true,
        },
    };

    assert_eq!(
        encode_response(&response).unwrap(),
        vec![
            0x53, 0x54, 0x47, 0x31, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x01,
        ]
    );
}

#[test]
fn response_read_golden_frame() {
    let response = WireResponse {
        request_id: 42,
        payload: WireResponsePayload::Read {
            data: b"abc".to_vec(),
            eof: true,
        },
    };

    assert_eq!(
        encode_response(&response).unwrap(),
        vec![
            0x53, 0x54, 0x47, 0x31, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x03, b'a', b'b',
            b'c',
        ]
    );
}

#[test]
fn response_read_prefix_decodes_without_body() {
    let prefix = super::encode_read_response_prefix(42, true, 3).unwrap();
    let header = super::ResponseFrameHeader::decode(
        &prefix[..super::ResponseFrameHeader::ENCODED_LEN],
    )
    .unwrap();
    assert!(header.is_read());
    assert_eq!(header.request_id, 42);

    let read = super::ReadResponsePrefix::decode_tail(
        header,
        &prefix[super::ResponseFrameHeader::ENCODED_LEN..],
    )
    .unwrap();
    assert!(read.eof);
    assert_eq!(read.data_len, 3);
}

#[test]
fn response_error_golden_frame() {
    let response = WireResponse {
        request_id: 42,
        payload: WireResponsePayload::Error {
            kind: StorageErrorKind::InvalidPath,
            message: "missing bucket".to_string(),
        },
    };

    assert_eq!(
        encode_response(&response).unwrap(),
        vec![
            0x53, 0x54, 0x47, 0x31, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x2a, 0x03, 0xe8, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, b'm',
            b'i', b's', b's', b'i', b'n', b'g', b' ', b'b', b'u', b'c', b'k', b'e',
            b't',
        ]
    );
}
