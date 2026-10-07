use std::io::{Error, ErrorKind};
use tokio::net::{TcpListener, UdpSocket};

fn retryable_fixture_bind_error(error: &Error, windows: bool) -> bool {
    error.kind() == ErrorKind::AddrInUse || (windows && error.kind() == ErrorKind::PermissionDenied)
}

// Spread candidates across the dynamic port range. Windows UDP :0 allocation can
// walk consecutive ports inside a TCP exclusion, exhausting all 32 attempts.
// 509 is coprime to the 16,384-port range; retries never repeat a candidate.
fn next_fixture_port(port: u16) -> u16 {
    const FIRST: u32 = 49_152;
    const COUNT: u32 = 16_384;
    (FIRST + (u32::from(port).saturating_sub(FIRST) + 509) % COUNT) as u16
}

pub async fn reserve_tcp_udp_pair() -> (TcpListener, UdpSocket) {
    // Keep both reservations alive until the caller releases them for startup.
    // Callers still have a release-to-bind window (TOCTOU).
    let mut candidate = 0;
    for attempt in 1..=32 {
        let udp = match UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, candidate)).await {
            Ok(udp) => udp,
            Err(error) if candidate != 0 && retryable_fixture_bind_error(&error, cfg!(windows)) => {
                eprintln!(
                    "fixture UDP candidate 127.0.0.1:{candidate}, attempt {attempt}/32: kind={:?}, raw_os_error={:?}",
                    error.kind(),
                    error.raw_os_error(),
                );
                candidate = next_fixture_port(candidate);
                continue;
            }
            Err(error) => {
                panic!("fixture UDP candidate {candidate}, attempt {attempt}/32: {error}")
            }
        };
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
                    candidate = next_fixture_port(address.port());
                    continue;
                }
                panic!("{diagnostic}");
            }
        }
    }
    panic!("no available TCP/UDP fixture pair after 32 candidates")
}

#[test]
fn fixture_candidates_escape_consecutive_tcp_exclusions_without_more_attempts() {
    // The failed Windows run returned 49859..=49890 on 32 consecutive UDP :0
    // allocations. An explicit second candidate escapes that entire interval.
    for first in 49_859..=49_890 {
        assert!(!(49_859..=49_890).contains(&next_fixture_port(first)));
    }
    for first in [49_152, 49_859, 65_535] {
        let mut seen = std::collections::HashSet::new();
        let mut port = first;
        for _ in 0..32 {
            assert!((49_152..=65_535).contains(&port));
            assert!(seen.insert(port));
            port = next_fixture_port(port);
        }
    }
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
