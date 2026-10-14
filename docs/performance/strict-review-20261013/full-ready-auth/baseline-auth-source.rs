pub async fn authenticate_stream(
    s: &Services,
    request: AuthenticationRequest<'_>,
    reader: &mut Reader,
    stream: &mut BoxStream,
    stop: &CancellationToken,
) -> Result<AuthenticatedSessionCandidate> {
    let auth = s.ingress.authenticate_session(request);
    tokio::pin!(auth);
    let until = Instant::now() + Duration::from_millis(s.ingress.limits.authentication_timeout_ms);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(Error::Draining),
            read = reader.read_more(stream, until) => read?,
            result = &mut auth => {
                if s.ingress.is_draining() { return Err(Error::Draining); }
                return result;
            }
        }
    }
}
