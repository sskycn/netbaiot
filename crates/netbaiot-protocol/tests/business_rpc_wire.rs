//! Frozen V3 wire fixtures exercised through the version-independent public API.
use netbaiot_protocol::business_rpc::wire::*;
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T: DeserializeOwned + Serialize>(json: &str) {
    let decoded: T = serde_json::from_str(json).unwrap();
    assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
}

#[test]
fn hello_and_ready_keep_exact_json_and_version_three() {
    let hello = r#"{"type":"hello","version":3,"token":"fixture-token","limits":{"max_frame_payload_bytes":8192,"max_concurrent_streams":256,"initial_stream_window_bytes":262144,"initial_connection_window_bytes":4194304,"heartbeat_ms":5000}}"#;
    let ready = r#"{"type":"ready","version":3,"connection_epoch":42,"limits":{"max_frame_payload_bytes":8192,"max_concurrent_streams":256,"initial_stream_window_bytes":262144,"initial_connection_window_bytes":4194304,"heartbeat_ms":5000}}"#;
    assert_eq!(BUSINESS_RPC_PROTOCOL_VERSION, 3);
    for json in [hello, ready] {
        round_trip::<RpcBootstrap>(json);
        serde_json::from_str::<RpcBootstrap>(json)
            .unwrap()
            .validate()
            .unwrap();
        for version in [0, 1, 2, 4, u16::MAX] {
            let invalid = json.replace("\"version\":3", &format!("\"version\":{version}"));
            assert_eq!(
                serde_json::from_str::<RpcBootstrap>(&invalid)
                    .unwrap()
                    .validate(),
                Err(RpcWireError::Limits)
            );
        }
    }
}

#[test]
fn header_bytes_and_resource_constants_keep_the_v3_contract() {
    assert_eq!(RPC_HEADER_BYTES, 12);
    assert_eq!(RPC_END_STREAM, 1);
    assert_eq!(RPC_MAX_STREAM_ID, 0x7fff_ffff);
    assert_eq!(RPC_MAX_METADATA_BYTES, 4096);
    assert_eq!(RPC_HARD_MAX_FRAME_PAYLOAD_BYTES, 16384);
    assert_eq!(RPC_MAX_MESSAGE_BYTES, 8 * 1024 * 1024);
    let header = RpcFrameHeader {
        payload_len: 258,
        stream_id: 0x1234_5678,
        frame_type: RpcFrameType::Data,
        flags: RPC_END_STREAM,
    };
    let bytes = [0, 0, 1, 2, 0x12, 0x34, 0x56, 0x78, 4, 1, 0, 0];
    assert_eq!(header.encode(), bytes);
    assert_eq!(RpcFrameHeader::parse(&bytes, 8192), Ok(header));
    let types = [
        RpcFrameType::Open,
        RpcFrameType::Accept,
        RpcFrameType::Response,
        RpcFrameType::Data,
        RpcFrameType::WindowUpdate,
        RpcFrameType::ResetStream,
        RpcFrameType::CloseStream,
        RpcFrameType::Ping,
        RpcFrameType::Pong,
        RpcFrameType::GoAway,
    ];
    for (index, frame_type) in types.into_iter().enumerate() {
        let number = (index + 1) as u8;
        assert_eq!(frame_type as u8, number);
        assert_eq!(RpcFrameType::try_from(number), Ok(frame_type));
    }
}

#[test]
fn metadata_ack_nack_and_error_names_keep_exact_json() {
    round_trip::<RpcOpen>(r#"{"kind":"provider","provider_id":"primary"}"#);
    round_trip::<RpcOpen>(
        r#"{"kind":"rpc","parent_stream_id":null,"request_id":"00000000-0000-0000-0000-000000000001","method":"device.command.send","deadline_ms":5000,"content_length":42}"#,
    );
    round_trip::<RpcOpen>(
        r#"{"kind":"event_delivery","parent_stream_id":1,"delivery_id":"00000000-0000-0000-0000-000000000001","event_id":"00000000-0000-0000-0000-000000000002","attempt":1,"content_length":42}"#,
    );
    round_trip::<RpcAccept>(r#"{"provider_epoch":42,"sync_required":true}"#);
    round_trip::<RpcResponse>(r#"{"content_length":0,"error":null}"#);
    round_trip::<RpcResponse>(
        r#"{"content_length":42,"error":{"code":"unavailable","message":"application did not commit"}}"#,
    );
    for status in ["ok", "error"] {
        round_trip::<RpcEventAck>(&format!(
            r#"{{"status":"{status}","delivery_id":"00000000-0000-0000-0000-000000000001","event_id":"00000000-0000-0000-0000-000000000002"}}"#
        ));
    }
    for code in [
        "cancel",
        "refused_stream",
        "protocol_error",
        "flow_control_error",
        "message_too_large",
        "stream_closed",
        "overloaded",
        "internal",
    ] {
        round_trip::<RpcReset>(&format!(r#"{{"code":"{code}","message":"fixture"}}"#));
    }
    for code in [
        "no_error",
        "protocol_error",
        "flow_control_error",
        "unauthorized",
        "shutdown",
    ] {
        round_trip::<RpcGoAway>(&format!(
            r#"{{"last_stream_id":7,"code":"{code}","message":"fixture"}}"#
        ));
    }
}
