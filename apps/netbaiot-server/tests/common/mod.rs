use std::io::{Error, ErrorKind};
use tokio::net::{TcpListener, UdpSocket};

fn retryable_fixture_bind_error(error: &Error, windows: bool) -> bool {
    error.kind() == ErrorKind::AddrInUse || (windows && error.kind() == ErrorKind::PermissionDenied)
}

pub async fn reserve_tcp_udp_pair() -> (TcpListener, UdpSocket) {
    // Windows TCP :0 may choose a port excluded for UDP (WSAEACCES). Select
    // from UDP first, then verify TCP while keeping both reservations alive.
    // Callers still release these sockets before the server binds (TOCTOU).
    for attempt in 1..=32 {
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = udp.local_addr().unwrap();
        match TcpListener::bind(address).await {
            Ok(tcp) => return (tcp, udp),
            Err(error) => {
                let diagnostic = format!(
                    "fixture TCP candidate {address}, UDP reserved, attempt {attempt}/32: kind={:?}, raw_os_error={:?}, error={error}",
                    error.kind(),
                    error.raw_os_error()
                );
                if retryable_fixture_bind_error(&error, cfg!(windows)) {
                    eprintln!("{diagnostic}");
                    continue;
                }
                panic!("{diagnostic}");
            }
        }
    }
    panic!("no available TCP/UDP fixture pair after 32 candidates")
}

#[test]
fn fixture_bind_error_classification_is_platform_explicit() {
    for windows in [false, true] {
        assert!(retryable_fixture_bind_error(
            &ErrorKind::AddrInUse.into(),
            windows
        ));
        assert_eq!(
            retryable_fixture_bind_error(&ErrorKind::PermissionDenied.into(), windows),
            windows
        );
        for kind in [ErrorKind::InvalidInput, ErrorKind::Other] {
            assert!(!retryable_fixture_bind_error(&kind.into(), windows));
        }
    }
}
