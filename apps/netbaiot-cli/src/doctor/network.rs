use super::*;
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_rustls::{TlsConnector, rustls};
pub(super) async fn bindings(config: &Config) -> Vec<Check> {
    let mut results = Vec::new();
    for (id, address, udp) in [
        ("device_port", Some(config.device_ingress), true),
        ("management_port", Some(config.management_http), false),
        ("business_port", config.business_tcp, false),
    ] {
        let Some(address) = address else {
            results.push(Check::new(
                id,
                Status::Skip,
                "NBI-DOC-002",
                "Listener not configured",
            ));
            continue;
        };
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let tcp = TcpListener::bind(address).await?;
            if udp {
                let _udp = UdpSocket::bind(tcp.local_addr()?).await?;
            }
            Ok::<(), std::io::Error>(())
        })
        .await;
        let pass = matches!(result, Ok(Ok(())));
        results.push(Check::new(
            id,
            if pass { Status::Pass } else { Status::Fail },
            "NBI-DOC-002",
            if pass {
                format!(
                    "{address} available now; sockets immediately released; not reserved for serve"
                )
            } else {
                format!("{address} cannot bind now or deadline exceeded")
            },
        ));
    }
    results
}
pub(super) async fn probe(id: &'static str, url: &str) -> Check {
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let url = reqwest::Url::parse(url).map_err(|_| "Invalid endpoint URL")?;
        if !netbaiot_server::safe_http_url(&url) {
            return Err("Unsafe endpoint URL");
        }
        let host = url
            .host_str()
            .ok_or("Endpoint host missing")?
            .trim_matches(['[', ']']);
        let port = url.port_or_known_default().ok_or("Endpoint port missing")?;
        let addresses = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            vec![std::net::SocketAddr::new(ip, port)]
        } else {
            let mut builder = hickory_resolver::TokioResolver::builder_tokio()
                .map_err(|_| "System DNS configuration unavailable")?;
            bounded_dns_options(builder.options_mut(), Duration::from_secs(1));
            let resolver = builder
                .build()
                .map_err(|_| "DNS resolver initialization failed")?;
            resolve_addresses(&resolver, host, port).await?
        };
        let mut connected = None;
        for address in addresses {
            if let Ok(socket) = TcpStream::connect(address).await {
                connected = Some(socket);
                break;
            }
        }
        let socket = connected.ok_or("TCP connection failed")?;
        if url.scheme() == "https" {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| "TLS profile unavailable")?
            .with_root_certificates(roots)
            .with_no_client_auth();
            let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|_| "TLS hostname invalid")?;
            TlsConnector::from(Arc::new(config))
                .connect(name, socket)
                .await
                .map_err(|_| "TLS verification/handshake failed")?;
        }
        Ok::<(), &str>(())
    })
    .await;
    match result {
        Ok(Ok(())) => Check::new(
            id,
            Status::Pass,
            "NBI-DOC-003",
            "DNS/TCP/TLS endpoint reachable; HTTP/authentication/ACK semantics NOT TESTED",
        ),
        Ok(Err(message)) => Check::new(id, Status::Fail, "NBI-DOC-003", message),
        Err(_) => Check::new(
            id,
            Status::Fail,
            "NBI-DOC-003",
            "Network probe exceeded the three-second overall deadline",
        ),
    }
}
fn bounded_dns_options(options: &mut hickory_resolver::config::ResolverOpts, timeout: Duration) {
    options.timeout = timeout;
    options.attempts = 1;
    options.num_concurrent_reqs = 1;
    options.cache_size = 0;
}
async fn resolve_addresses(
    resolver: &hickory_resolver::TokioResolver,
    host: &str,
    port: u16,
) -> Result<Vec<std::net::SocketAddr>, &'static str> {
    Ok(resolver
        .lookup_ip(host)
        .await
        .map_err(|_| "Asynchronous DNS lookup failed")?
        .iter()
        .take(16)
        .map(|ip| std::net::SocketAddr::new(ip, port))
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn asynchronous_dns_timeout_uses_only_a_local_mock() {
        use hickory_resolver::{
            config::{ConnectionConfig, NameServerConfig, ResolverConfig},
            net::runtime::TokioRuntimeProvider,
        };
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let mut connection = ConnectionConfig::udp();
        connection.port = address.port();
        let config = ResolverConfig::from_parts(
            None,
            vec![],
            vec![NameServerConfig::new(address.ip(), false, vec![connection])],
        );
        let mut builder = hickory_resolver::TokioResolver::builder_with_config(
            config,
            TokioRuntimeProvider::default(),
        );
        bounded_dns_options(builder.options_mut(), Duration::from_millis(100));
        let resolver = builder.build().unwrap();
        let received = tokio::spawn(async move {
            let mut bytes = [0; 512];
            socket.recv_from(&mut bytes).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            resolve_addresses(&resolver, "doctor-timeout.test.", 443),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        received.abort();
    }
    #[tokio::test]
    async fn tls_probe_is_bounded_without_any_business_http_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "https://{}/secret-path-not-printed",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let start = tokio::time::Instant::now();
        let report = probe("business_sink_network", &url).await;
        assert_eq!(report.status as u8, Status::Fail as u8);
        assert!(start.elapsed() < Duration::from_secs(4));
        assert!(!report.message.contains("secret-path"));
        task.abort();
    }
}
