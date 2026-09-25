use super::*;

pub(crate) type TcpStreamSink = BusinessRpcEventSink;

struct ActiveStreamLease {
    sink: Arc<TcpStreamSink>,
    generation: u64,
}
impl Drop for ActiveStreamLease {
    fn drop(&mut self) {
        let _ = self.sink.release(self.generation);
    }
}

async fn read_frame(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let mut length = [0u8; 4];
        stream
            .read_exact(&mut length)
            .await
            .map_err(|_| Error::Unavailable)?;
        let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| Error::Invalid)?;
        if length == 0 || length > framer.maximum {
            return Err(Error::Invalid);
        }
        let mut payload = vec![0; length];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(payload)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

async fn write_frame(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    frame: &StreamServerFrame,
    timeout_ms: u64,
) -> Result<()> {
    let payload = serde_json::to_vec(frame).map_err(|_| Error::Internal)?;
    let wire = framer.encode(&payload)?;
    tokio::time::timeout(Duration::from_millis(timeout_ms), stream.write_all(&wire))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Unavailable)
}

async fn write_stream_error(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    limits: &Limits,
    code: ErrorCode,
    message: &str,
) {
    let _ = write_frame(
        stream,
        framer,
        &StreamServerFrame::Error {
            version: PROTOCOL_VERSION,
            error: ApiError {
                code,
                message: message.to_owned(),
                request_id: Some(EventId::generate().to_string()),
                required_scope: None,
            },
        },
        limits.write_timeout_ms,
    )
    .await;
}

async fn business_handshake(
    stream: &mut TcpStream,
    framer: &LengthPrefixFramer,
    token_hash: &[u8; 32],
    limits: &Limits,
    first_frame: Option<Vec<u8>>,
) -> Result<(SubscriptionId, EventFilter)> {
    let payload = match first_frame {
        Some(payload) => payload,
        None => read_frame(stream, framer, limits.connect_timeout_ms).await?,
    };
    let hello = match serde_json::from_slice::<StreamClientFrame>(&payload) {
        Ok(frame) => frame,
        Err(_) => {
            write_stream_error(
                stream,
                framer,
                limits,
                ErrorCode::InvalidRequest,
                "invalid business stream hello",
            )
            .await;
            return Err(Error::Invalid);
        }
    };
    let StreamClientFrame::Hello { version, token } = hello else {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "hello must be the first business stream frame",
        )
        .await;
        return Err(Error::Invalid);
    };
    if version != PROTOCOL_VERSION {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidProtocolVersion,
            "unsupported business stream protocol version",
        )
        .await;
        return Err(Error::Invalid);
    }
    if !bool::from(
        Sha256::digest(token.as_bytes())
            .as_slice()
            .ct_eq(token_hash),
    ) {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::Unauthenticated,
            "business stream authentication failed",
        )
        .await;
        return Err(Error::Authentication);
    }

    let payload = read_frame(stream, framer, limits.connect_timeout_ms).await?;
    let subscribe = match serde_json::from_slice::<StreamClientFrame>(&payload) {
        Ok(frame) => frame,
        Err(_) => {
            write_stream_error(
                stream,
                framer,
                limits,
                ErrorCode::InvalidRequest,
                "invalid business stream subscription",
            )
            .await;
            return Err(Error::Invalid);
        }
    };
    let StreamClientFrame::Subscribe {
        version,
        subscription_id,
        filter,
    } = subscribe
    else {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "subscribe must follow the business stream hello",
        )
        .await;
        return Err(Error::Invalid);
    };
    if version != PROTOCOL_VERSION {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidProtocolVersion,
            "unsupported business stream protocol version",
        )
        .await;
        return Err(Error::Invalid);
    }
    if filter.validate().is_err() {
        write_stream_error(
            stream,
            framer,
            limits,
            ErrorCode::InvalidRequest,
            "business stream filter exceeds protocol bounds",
        )
        .await;
        return Err(Error::Invalid);
    }
    Ok((subscription_id, filter))
}

pub(crate) async fn serve_business_connection(
    mut stream: TcpStream,
    sink: Arc<TcpStreamSink>,
    token_hash: [u8; 32],
    limits: Arc<Limits>,
    stop: CancellationToken,
    first_frame: Option<Vec<u8>>,
) -> Result<()> {
    let framer = LengthPrefixFramer {
        maximum: limits.max_tcp_frame_size,
    };
    let Ok((subscription_id, filter)) =
        business_handshake(&mut stream, &framer, &token_hash, &limits, first_frame).await
    else {
        return Ok(());
    };
    let (sender, mut receiver) = mpsc::channel(limits.sink_delivery_concurrency);
    let generation = match sink.claim(sender, filter) {
        Ok(generation) => generation,
        Err(Error::Conflict) => {
            write_stream_error(
                &mut stream,
                &framer,
                &limits,
                ErrorCode::Conflict,
                "an active business subscriber already owns the required sink",
            )
            .await;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let _lease = ActiveStreamLease {
        sink: sink.clone(),
        generation,
    };
    if write_frame(
        &mut stream,
        &framer,
        &StreamServerFrame::Ready {
            version: PROTOCOL_VERSION,
            subscription_id,
        },
        limits.write_timeout_ms,
    )
    .await
    .is_err()
    {
        sink.release(generation)?;
        return Ok(());
    }
    loop {
        let request = tokio::select! {
            _ = stop.cancelled() => None,
            request = receiver.recv() => request,
            ready = stream.readable() => {
                ready.map_err(|_| Error::Unavailable)?;
                let mut unexpected = [0u8; 1];
                match stream.try_read(&mut unexpected) {
                    Ok(0) => None,
                    Ok(_) => return Err(Error::Invalid),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => return Err(Error::Unavailable),
                }
            }
        };
        let Some(request) = request else { break };
        let delivery_id = DeliveryId::generate();
        let event_id = request.delivery.event.event_id;
        let frame = StreamServerFrame::Event {
            version: PROTOCOL_VERSION,
            delivery: EventDelivery {
                delivery_id,
                subscription_id,
                event: (*request.delivery.event).clone(),
                attempt: request.delivery.attempt,
            },
        };
        let delivered = tokio::select! {
            _ = stop.cancelled() => Err(Error::Draining),
            result = async {
                write_frame(&mut stream, &framer, &frame, limits.write_timeout_ms).await?;
                let ack: StreamClientFrame =
                    serde_json::from_slice(&read_frame(
                        &mut stream,
                        &framer,
                        limits.sink_timeout_ms,
                    ).await?)
                        .map_err(|_| Error::Invalid)?;
                match ack {
                    StreamClientFrame::Ack { version, ack }
                        if version == PROTOCOL_VERSION
                            && ack.delivery_id == delivery_id
                            && ack.subscription_id == subscription_id
                            && ack.event_id == event_id =>
                    {
                        Ok(SinkAck)
                    }
                    _ => Err(Error::Invalid),
                }
            } => result,
        };
        let failed = delivered.is_err();
        let _ = request.result.send(delivered.map_err(|error| {
            if matches!(error, Error::Invalid) {
                SinkError::Permanent
            } else {
                SinkError::Retryable
            }
        }));
        if failed {
            break;
        }
    }
    sink.release(generation)?;
    Ok(())
}

pub(crate) async fn serve_business_stream(
    listener: TcpListener,
    sink: Arc<TcpStreamSink>,
    token_hash: [u8; 32],
    limits: Arc<Limits>,
    stop: CancellationToken,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = stop.cancelled() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(error=%error, "business stream task failed");
                }
                continue;
            }
            accepted = listener.accept() => accepted.map_err(|_| Error::Unavailable)?,
        };
        if tasks.len() >= limits.max_ingress {
            drop(accepted.0);
            continue;
        }
        let (stream, _) = accepted;
        let sink = sink.clone();
        let limits = limits.clone();
        let stop = stop.child_token();
        tasks.spawn(async move {
            serve_business_connection(stream, sink, token_hash, limits, stop, None).await
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}
