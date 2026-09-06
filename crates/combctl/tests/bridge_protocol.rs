//! Protocol parse/validate and handler seam against an in-memory store.

#[path = "../src/bridge/mod.rs"]
mod bridge;

use bridge::handler::{mint_writer, Bridge};
use bridge::limits::Limits;
use bridge::protocol::{
    extract_id, parse_payload_hex, parse_request, seq_string, ErrorCode, Response,
};
use comb_core::DigestKey;
use comb_object::memory::MemoryBackend;
use combctl::store::Store;
use serde_json::{json, Value};
use std::sync::Arc;

fn limits() -> Limits {
    Limits::default()
}

fn parse(v: Value) -> Result<bridge::protocol::Request, Response> {
    parse_request(&serde_json::to_vec(&v).unwrap(), &limits())
}

fn parse_err(v: Value) -> Value {
    match parse(v) {
        Err(resp) => serde_json::from_str(&resp.to_jsonl()).unwrap(),
        Ok(_) => panic!("expected parse error"),
    }
}

fn mem_bridge() -> Bridge {
    Bridge::new(
        Store {
            backend: Arc::new(MemoryBackend::new()),
            tenant: "org_t".into(),
            key: DigestKey::from_bytes([5u8; 32]),
            cache_dir: None,
        },
        "comb-bridge".into(),
        60,
        Limits::default(),
    )
}

async fn rpc(bridge: &Bridge, v: Value) -> Value {
    let resp = bridge.handle_frame(&serde_json::to_vec(&v).unwrap()).await;
    serde_json::from_str(&resp.to_jsonl()).unwrap()
}

#[test]
fn hello_and_unknown_version() {
    parse(json!({"v":1,"id":"h","op":"hello"})).unwrap();
    let err = parse_err(json!({"v":2,"id":"h","op":"hello"}));
    assert_eq!(err["ok"], false);
    assert_eq!(err["id"], "h");
    assert_eq!(err["error"]["code"], "unsupported");
    assert_eq!(err["error"]["capability"], "protocol");
}

#[test]
fn zero_timeout_is_a_poll_but_page_limits_stay_positive() {
    parse(json!({"v":1,"id":"f","op":"follow","log":"doc","cursor":"1","timeout_ms":0})).unwrap();
    for field in ["max_events", "max_bytes"] {
        let mut request =
            json!({"v":1,"id":"f","op":"follow","log":"doc","cursor":"1","timeout_ms":0});
        request[field] = json!(0);
        assert_eq!(parse_err(request)["error"]["code"], "invalid_request");
    }
    assert_eq!(
        parse_err(
            json!({"v":1,"id":"f","op":"follow","log":"doc","cursor":"1","timeout_ms":30001})
        )["error"]["code"],
        "invalid_request"
    );
}

#[test]
fn maximum_raw_append_fits_wire_with_maximum_identity_fields() {
    let limits = limits();
    let mut request = json!({
        "v":1,"id":"\"".repeat(limits.max_id_len),"op":"append",
        "log":"x".repeat(limits.max_log_name_len),
        "idempotency_key":"ab".repeat(limits.max_idempotency_key_len / 2),
        "payload_hex":"ff".repeat(limits.max_append_bytes)
    });
    assert!(serde_json::to_vec(&request).unwrap().len() <= limits.max_frame_bytes);
    parse(request.clone()).unwrap();
    request["payload_hex"] = json!("ff".repeat(limits.max_append_bytes + 1));
    assert_eq!(parse_err(request)["error"]["code"], "invalid_request");
}

#[test]
fn maximum_raw_page_leaves_space_for_all_event_metadata() {
    use bridge::protocol::{OkBody, WireEvent};
    let limits = limits();
    let events = (0..limits.max_read_events)
        .map(|index| WireEvent {
            seq: u64::MAX.to_string(),
            at: "+262142-12-31T23:59:59.999999999+23:59".into(),
            payload_hex: "ff".repeat(if index == 0 {
                limits.max_read_bytes - (limits.max_read_events - 1)
            } else {
                1
            }),
        })
        .collect();
    let response = Response::ok(
        "\"".repeat(limits.max_id_len),
        "follow",
        OkBody::Page {
            log: "x".repeat(limits.max_log_name_len),
            events,
            next_cursor: u64::MAX.to_string(),
            at_head: false,
            timed_out: Some(false),
        },
    );
    assert!(response.to_jsonl().len() <= limits.max_frame_bytes);
}

#[tokio::test]
async fn head_without_representable_next_cursor_returns_error() {
    let store = Store {
        backend: Arc::new(MemoryBackend::new()),
        tenant: "org_head_overflow".into(),
        key: DigestKey::from_bytes([7; 32]),
        cache_dir: None,
    };
    let manifest = json!({
        "schema":"comb.log.partition-manifest/v1", "log":"log/doc/p0",
        "epoch":0, "head_seq":u64::MAX, "chunks":[], "segments":[], "trim_before_seq":0,
    });
    let (digest, _) = store
        .put_blob(serde_json::to_vec(&manifest).unwrap())
        .await
        .unwrap();
    store.set_target("log/doc/p0", digest, None).await.unwrap();
    let bridge = Bridge::new(store, "test-writer".into(), 60, limits());
    let response = rpc(
        &bridge,
        json!({"v":1,"id":"head-overflow","op":"head","log":"doc"}),
    )
    .await;
    assert_eq!(response["id"], "head-overflow");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "backend_unavailable");
    assert!(response.get("cursor").is_none());
}

#[test]
fn unknown_op_is_invalid_request() {
    let err = parse_err(json!({"v":1,"id":"x","op":"trim"}));
    assert_eq!(err["id"], "x");
    assert_eq!(err["error"]["code"], "invalid_request");
}

#[test]
fn append_requires_top_level_key_and_single_payload() {
    let err = parse_err(json!({"v":1,"id":"a","op":"append","log":"doc","payload_hex":"00"}));
    assert_eq!(err["id"], "a");
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"00",
        "events":[{"payload_hex":"00"}]
    }));
    assert_eq!(err["id"], "a");
    assert_eq!(err["error"]["code"], "invalid_request");

    parse(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"00ff","payload_hex":"00ff"
    }))
    .unwrap();
}

#[test]
fn idempotency_key_is_opaque_hex_not_ascii() {
    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"doc:hash","payload_hex":"00"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"k","payload_hex":"00"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");

    let too_long = "aa".repeat(513);
    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key": too_long,"payload_hex":"00"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");

    parse(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"AA","payload_hex":"00"
    }))
    .unwrap();
}

#[test]
fn rejects_invalid_hex_empty_payload_and_bad_names() {
    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"00","payload_hex":"zz"
    }));
    assert_eq!(err["id"], "a");
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"doc","idempotency_key":"00","payload_hex":"abc"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"../etc","idempotency_key":"00","payload_hex":"00"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({
        "v":1,"id":"a","op":"append","log":"a/b","idempotency_key":"00","payload_hex":"00"
    }));
    assert_eq!(err["error"]["code"], "invalid_request");
}

#[test]
fn cursor_must_be_decimal_string() {
    let err = parse_err(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":1}));
    assert_eq!(err["id"], "r");
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"0"}));
    assert_eq!(err["error"]["code"], "invalid_request");

    let err = parse_err(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"01"}));
    assert_eq!(err["error"]["code"], "invalid_request");

    parse(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"1"})).unwrap();
    parse(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"18446744073709551615"})).unwrap();
    let err =
        parse_err(json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"18446744073709551616"}));
    assert_eq!(err["error"]["code"], "invalid_request");
}

#[test]
fn payload_hex_roundtrips_arbitrary_bytes() {
    let bytes: Vec<u8> = (0u8..=255).collect();
    let hex = hex::encode(&bytes);
    let (normalized, decoded) = parse_payload_hex(&hex).unwrap();
    assert_eq!(normalized, hex);
    assert_eq!(decoded, bytes);
    let (from_upper, decoded_upper) = parse_payload_hex(&hex.to_uppercase()).unwrap();
    assert_eq!(from_upper, hex);
    assert_eq!(decoded_upper, bytes);
}

#[test]
fn mint_writer_is_unique_per_call() {
    let a = mint_writer();
    let b = mint_writer();
    assert_ne!(a, b);
    assert!(a.starts_with("comb-bridge-"));
    assert_eq!(a.len(), "comb-bridge-".len() + 32);
}

#[test]
fn seq_strings_are_decimal() {
    assert_eq!(seq_string(0), "0");
    assert_eq!(seq_string(1), "1");
    assert_eq!(seq_string(u64::MAX), "18446744073709551615");
}

#[test]
fn malformed_json_keeps_id_when_present() {
    let err = match parse_request(br#"{"v":1,"id":"keep-me","op":"hello" trailing"#, &limits()) {
        Err(resp) => serde_json::from_str::<Value>(&resp.to_jsonl()).unwrap(),
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err["error"]["code"], "invalid_request");
    assert_eq!(err["id"], "keep-me");
    assert_eq!(extract_id(br#"{"v":1,"id":"x","op":"hello""#), "x");
}

#[tokio::test]
async fn hello_advertises_honest_capabilities() {
    let b = mem_bridge();
    let v = rpc(&b, json!({"v":1,"id":"h","op":"hello"})).await;
    assert_eq!(v["ok"], true);
    assert_eq!(v["op"], "hello");
    assert_eq!(v["protocol"], 1);
    assert_eq!(v["capabilities"]["durable_idempotency"], false);
    assert_eq!(v["capabilities"]["bounded_memory_read"], false);
    assert_eq!(v["capabilities"]["payload_hex"], true);
    assert_eq!(v["limits"]["max_append_events"], 1);
    assert_eq!(v["limits"]["max_idempotency_key_len"], 1024);
    assert!(v["limits"]["max_frame_bytes"].as_u64().unwrap() > 0);
    assert!(v["limits"]["max_append_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn hello_require_unavailable_is_unsupported() {
    let b = mem_bridge();
    let v = rpc(
        &b,
        json!({"v":1,"id":"h","op":"hello","require":["durable_idempotency"]}),
    )
    .await;
    assert_eq!(v["id"], "h");
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "unsupported");
    assert_eq!(v["error"]["capability"], "durable_idempotency");
}

#[tokio::test]
async fn keyed_append_is_unsupported_and_does_not_append() {
    let b = mem_bridge();
    let v = rpc(
        &b,
        json!({
            "v":1,
            "id":"a",
            "op":"append",
            "log":"doc1",
            "idempotency_key":"cafebabe",
            "payload_hex":"00ff"
        }),
    )
    .await;
    assert_eq!(v["id"], "a");
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "unsupported");
    assert_eq!(v["error"]["capability"], "durable_idempotency");

    let head = rpc(&b, json!({"v":1,"id":"h","op":"head","log":"doc1"})).await;
    assert_eq!(head["id"], "h");
    assert_eq!(head["ok"], true);
    assert_eq!(head["head"], "0");
}

#[tokio::test]
async fn read_and_follow_are_unsupported_and_keep_id() {
    let b = mem_bridge();
    let read = rpc(
        &b,
        json!({"v":1,"id":"r","op":"read","log":"doc1","cursor":"1"}),
    )
    .await;
    assert_eq!(read["id"], "r");
    assert_eq!(read["ok"], false);
    assert_eq!(read["error"]["code"], "unsupported");
    assert_eq!(read["error"]["capability"], "bounded_memory_read");

    let follow = rpc(
        &b,
        json!({"v":1,"id":"f","op":"follow","log":"doc1","cursor":"1","timeout_ms":30}),
    )
    .await;
    assert_eq!(follow["id"], "f");
    assert_eq!(follow["ok"], false);
    assert_eq!(follow["error"]["code"], "unsupported");
    assert_eq!(follow["error"]["capability"], "bounded_memory_read");
}

#[test]
fn error_code_wire_names() {
    let json = serde_json::to_value(ErrorCode::EventTooLarge).unwrap();
    assert_eq!(json, "event_too_large");
    let json = serde_json::to_value(ErrorCode::UnknownOperation).unwrap();
    assert_eq!(json, "unknown_operation");
}

#[tokio::test]
async fn fixture_shaped_append_is_unsupported() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/testing/fixtures/foundation-bridge.json");
    if !path.exists() {
        return;
    }
    let raw = std::fs::read_to_string(&path).unwrap();
    let fixture: Value = serde_json::from_str(&raw).unwrap();
    let b = mem_bridge();
    let change = &fixture["changes"][0];
    let v = rpc(
        &b,
        json!({
            "v":1,
            "id":"a0",
            "op":"append",
            "log":"foundation",
            "idempotency_key": change["idempotency_key"],
            "payload_hex": change["payload_hex"]
        }),
    )
    .await;
    assert_eq!(v["id"], "a0");
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["capability"], "durable_idempotency");
}

#[test]
fn extracting_id_from_large_json_preserves_json_escaping() {
    let id = "quoted\"request\\id";
    let frame = serde_json::to_vec(&json!({"id":id,"padding":"x".repeat(9000)})).unwrap();
    assert_eq!(extract_id(&frame), id);
}
