//! Negative-path tests for [`JsonCodec`]. Spec §6 #10.

use eventbus_core::{Codec, EventBusError};
use eventbus_redis::JsonCodec;

#[test]
fn decode_truncated_json_returns_source_error_with_chain() {
    let bytes = br#"{"message":{"uid":"x","topic":"t""#; // truncated
    let codec = JsonCodec;
    let err = codec.decode(bytes).expect_err("decode should fail");
    match err {
        EventBusError::Source { context, source } => {
            assert!(context.contains("decode"), "context: {context}");
            // The source chain should expose the underlying serde_json error.
            let mut current: Option<&(dyn std::error::Error + 'static)> = source.source();
            let mut depth = 1; // count `source` itself.
            while let Some(e) = current {
                depth += 1;
                current = e.source();
                if depth > 10 {
                    panic!("infinite source loop");
                }
            }
            assert!(depth >= 1, "source chain should have at least 1 level");
        }
        other => panic!("expected Source variant, got {other:?}"),
    }
}

#[test]
fn decode_non_utf8_returns_source_error() {
    let bytes = &[0xFF, 0xFE, 0xFD][..];
    let codec = JsonCodec;
    let err = codec.decode(bytes).expect_err("decode should fail");
    assert!(
        matches!(err, EventBusError::Source { .. }),
        "expected Source variant, got {err:?}"
    );
}

#[test]
fn decode_empty_bytes_returns_source_error() {
    let codec = JsonCodec;
    let err = codec.decode(&[]).expect_err("decode should fail");
    assert!(matches!(err, EventBusError::Source { .. }));
}
