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
        if reader.buffer.len() == reader.maximum {
            // A legal pipelined frame may fill the pre-auth reader exactly.
            // Do not ask read_more for zero room and reject ready auth. A
            // bounded probe still observes EOF or additional overload bytes.
            let mut probe = [0; 1];
            let deadline = reader.deadline(until);
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(Error::Draining),
                result = &mut auth => {
                    if Instant::now() >= deadline { return Err(Error::Timeout); }
                    if s.ingress.is_draining() { return Err(Error::Draining); }
                    return result;
                }
                _ = tokio::time::sleep_until(deadline) => return Err(Error::Timeout),
                read = stream.read(&mut probe) => {
                    let read = read.map_err(|_| Error::Unavailable)?;
                    return Err(if read == 0 { Error::Unavailable } else { Error::Overloaded });
                }
            }
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(Error::Draining),
            read = reader.read_more(stream, until) => read?,
            result = &mut auth => {
                if Instant::now() >= reader.deadline(until) { return Err(Error::Timeout); }
                if s.ingress.is_draining() { return Err(Error::Draining); }
                return result;
            }
        }
    }
}
