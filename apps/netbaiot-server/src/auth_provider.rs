use super::*;

pub(crate) struct HttpAuthProvider {
    client: reqwest::Client,
    url: reqwest::Url,
    authorization: Option<reqwest::header::HeaderValue>,
    slots: Arc<tokio::sync::Semaphore>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifierResponse {
    identity: AuthenticatedDevice,
    verifier_key_hex: String,
}

impl HttpAuthProvider {
    pub(crate) fn new(url: &str, limits: &Limits) -> Result<Arc<Self>> {
        let token = match std::env::var("NETBAIOT_AUTH_PROVIDER_TOKEN") {
            Ok(token) => Some(token),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => return Err(Error::Configuration),
        };
        Self::with_token(url, limits, token.as_deref())
    }

    fn with_token(url: &str, limits: &Limits, token: Option<&str>) -> Result<Arc<Self>> {
        let authorization = token
            .map(|token| {
                if token.is_empty()
                    || token.len() > 4096
                    || !token.bytes().all(|byte| byte.is_ascii_graphic())
                {
                    return Err(Error::Configuration);
                }
                let mut header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| Error::Configuration)?;
                header.set_sensitive(true);
                Ok(header)
            })
            .transpose()?;
        let url = reqwest::Url::parse(url).map_err(|_| Error::Configuration)?;
        if !crate::diagnostics::safe_http_url(&url) {
            return Err(Error::Configuration);
        }
        if authorization.is_none()
            && !url.host_str().is_some_and(|host| {
                host == "localhost"
                    || host
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            })
        {
            tracing::warn!(
                "HTTP auth provider has no service token; configure NETBAIOT_AUTH_PROVIDER_TOKEN"
            );
        }
        Ok(Arc::new(Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_millis(limits.authentication_timeout_ms))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| Error::Configuration)?,
            url,
            authorization,
            slots: Arc::new(tokio::sync::Semaphore::new(
                limits.max_auth_provider_requests,
            )),
        }))
    }
    fn request(&self) -> reqwest::RequestBuilder {
        let request = self.client.post(self.url.clone());
        match &self.authorization {
            Some(value) => request.header(reqwest::header::AUTHORIZATION, value.clone()),
            None => request,
        }
    }
}

#[async_trait]
impl DeviceAuthenticator for HttpAuthProvider {
    async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let value = match request {
            AuthenticationRequest::Secret {
                credential_id,
                secret,
            } => serde_json::json!({
                "kind": "secret", "credential_id": credential_id, "secret_hex": encode_hex(secret),
            }),
        };
        let response = self
            .request()
            .json(&value)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(Error::Authentication);
        }
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > 16_384)
        {
            return Err(Error::Invalid);
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            let length = bytes.len().checked_add(chunk.len()).ok_or(Error::Invalid)?;
            if length > 16_384 {
                return Err(Error::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)
    }

    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier> {
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let response = self
            .request()
            .json(&serde_json::json!({
                "kind": "verifier", "credential_id": credential_id,
            }))
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            return Err(Error::Authentication);
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > 16_384)
        {
            return Err(Error::Unavailable);
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            let length = bytes.len().checked_add(chunk.len()).ok_or(Error::Invalid)?;
            if length > 16_384 {
                return Err(Error::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: VerifierResponse =
            serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        let key: [u8; 32] = decode_hex(&response.verifier_key_hex)?
            .try_into()
            .map_err(|_| Error::Invalid)?;
        Ok(DeviceVerifier::new(response.identity, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn http_auth_provider_authenticates_both_authority_requests_and_redacts_token() {
        for token in [None, Some("provider-only-secret")] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                for kind in ["secret", "verifier"] {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    loop {
                        let mut buffer = [0; 2048];
                        let n = stream.read(&mut buffer).await.unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&buffer[..n]);
                        assert!(bytes.len() < 8192);
                        if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                            let header = std::str::from_utf8(&bytes[..end]).unwrap();
                            let length: usize = header
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .map(str::to_owned)
                                })
                                .unwrap()
                                .parse()
                                .unwrap();
                            if bytes.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    if let Some(token) = token {
                        assert!(request.contains(&format!("authorization: Bearer {token}")));
                    } else {
                        assert!(!request.contains("authorization:"));
                    }
                    assert!(request.contains(&format!("\"kind\":\"{kind}\"")));
                    stream.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                }
            });
            let provider = HttpAuthProvider::with_token(
                &format!("http://{address}/auth"),
                &Limits::default(),
                token,
            )
            .unwrap();
            let request = provider.request().build().unwrap();
            assert!(!format!("{request:?}").contains("provider-only-secret"));
            if let Some(value) = &provider.authorization {
                assert!(value.is_sensitive());
            }
            let error = provider
                .authenticate(AuthenticationRequest::Secret {
                    credential_id: "device",
                    secret: b"device-secret",
                })
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Authentication));
            assert!(!format!("{error:?} {error}").contains("secret"));
            assert!(matches!(
                provider.resolve_verifier("device").await,
                Err(Error::Authentication)
            ));
            task.await.unwrap();
        }
    }

    #[test]
    fn http_auth_provider_token_validation_preserves_transport_policy() {
        let limits = Limits::default();
        for token in ["", "secret\r\nX-Evil: injected", "secret with space"] {
            let error = HttpAuthProvider::with_token(
                "https://authority.example/auth",
                &limits,
                Some(token),
            )
            .err()
            .unwrap();
            assert_eq!(format!("{error:?}"), "Configuration");
        }
        assert!(
            HttpAuthProvider::with_token("http://authority.example/auth", &limits, Some("secret"))
                .is_err()
        );
        assert!(HttpAuthProvider::with_token("http://127.0.0.1/auth", &limits, None).is_ok());
        // Optional for backwards compatibility; production HTTPS without a token warns.
        assert!(
            HttpAuthProvider::with_token("https://authority.example/auth", &limits, None).is_ok()
        );
    }

    #[test]
    fn auth_http_capacity_does_not_follow_event_ingress_capacity() {
        let limits = Limits {
            max_ingress: 1,
            max_ingress_per_tenant: 1,
            max_ingress_per_device: 1,
            max_auth_provider_requests: 3,
            ..Limits::default()
        };
        limits.validate().unwrap();
        let provider =
            HttpAuthProvider::with_token("http://127.0.0.1/auth", &limits, None).unwrap();
        let permits = (0..3)
            .map(|_| provider.slots.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        assert!(provider.slots.clone().try_acquire_owned().is_err());
        drop(permits);
        assert_eq!(provider.slots.available_permits(), 3);
    }
}
