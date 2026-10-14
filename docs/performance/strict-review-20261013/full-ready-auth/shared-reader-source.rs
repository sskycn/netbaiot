pub struct Reader {
    pub buffer: BytesMut,
    started: Option<Instant>,
    maximum: usize,
    read_timeout: Duration,
    budget: Option<ConnectionReadBudget>,
    allocation: Option<Arc<ReadAllocationPermit>>,
}
struct ReadFrame {
    bytes: Bytes,
    _allocation: Arc<ReadAllocationPermit>,
}
impl AsRef<[u8]> for ReadFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Reader {
    pub fn new(maximum: usize, timeout_ms: u64) -> Self {
        Self {
            buffer: BytesMut::new(),
            started: None,
            maximum,
            read_timeout: Duration::from_millis(timeout_ms),
            budget: None,
            allocation: None,
        }
    }
    pub fn with_budget(maximum: usize, timeout_ms: u64, budget: ConnectionReadBudget) -> Self {
        Self {
            budget: Some(budget),
            ..Self::new(maximum, timeout_ms)
        }
    }
    pub fn authenticate(&mut self, device: &netbaiot_core::DeviceKey) -> Result<()> {
        if let Some(budget) = &mut self.budget {
            budget.authenticate(device)?;
        }
        Ok(())
    }
    /// Decoded zero-copy fields retain the allocation's byte charge until their
    /// last alias is dropped, even if the reader or connection has gone away.
    pub fn take_frame(&mut self, length: usize) -> Result<Bytes> {
        if length > self.buffer.len() {
            return Err(Error::Invalid);
        }
        let frame = self.buffer.split_to(length).freeze();
        Ok(self.own_frame(frame))
    }
    pub fn own_frame(&self, frame: Bytes) -> Bytes {
        if let Some(allocation) = &self.allocation {
            Bytes::from_owner(ReadFrame {
                bytes: frame,
                _allocation: allocation.clone(),
            })
        } else {
            frame
        }
    }
    fn allocate(
        &mut self,
        capacity: usize,
    ) -> Result<(BytesMut, Option<Arc<ReadAllocationPermit>>)> {
        if capacity > self.maximum {
            return Err(Error::Invalid);
        }
        let allocation = self
            .budget
            .as_mut()
            .map(|budget| budget.reserve_allocation(capacity).map(Arc::new))
            .transpose()?;
        let buffer = Self::checked_buffer(capacity)?;
        Ok((buffer, allocation))
    }
    fn checked_buffer(capacity: usize) -> Result<BytesMut> {
        let buffer = BytesMut::with_capacity(capacity);
        // Do not silently undercharge a dependency/allocator capacity change.
        // The current BytesMut uses Vec::with_capacity with the exact request.
        if buffer.capacity() != capacity {
            return Err(Error::Internal);
        }
        Ok(buffer)
    }
    fn grow(&mut self, target: usize) -> Result<()> {
        let room = target
            .checked_sub(self.buffer.len())
            .ok_or(Error::Invalid)?;
        // A split buffer can have a large charged allocation behind a small
        // visible tail. Reclaim its prefix without allocating, when unshared.
        if self.buffer.try_reclaim(room) {
            return Ok(());
        }
        if self.buffer.len() <= 8 {
            let exclusive = self.allocation.as_mut().and_then(Arc::get_mut);
            if let (Some(budget), Some(previous)) = (&mut self.budget, exclusive) {
                // Reserve the larger allocation before touching the prefix.
                // The eight-byte stack staging fits the fixed allowance and
                // lets an aggregate budget fit exactly one maximum frame.
                let capacity = target.max(previous.capacity());
                budget.expand_allocation(previous, capacity)?;
                let mut prefix = [0; 8];
                let length = self.buffer.len();
                prefix[..length].copy_from_slice(&self.buffer);
                self.buffer = BytesMut::new();
                let mut next = match Self::checked_buffer(capacity) {
                    Ok(next) => next,
                    Err(error) => {
                        self.allocation = None;
                        return Err(error);
                    }
                };
                next.extend_from_slice(&prefix[..length]);
                self.buffer = next;
                return Ok(());
            }
        }
        let (allocation, replacement) = if let Some(budget) = &mut self.budget {
            if let Some(previous) = self.allocation.as_mut().and_then(Arc::get_mut) {
                (None, Some(budget.reserve_replacement(target, previous)?))
            } else {
                (Some(Arc::new(budget.reserve_allocation(target)?)), None)
            }
        } else {
            (None, None)
        };
        let mut next = Self::checked_buffer(target)?;
        next.extend_from_slice(&self.buffer);
        // No mutation before every resource check succeeds. Release the old
        // physical allocation before its permit, then end the exclusive copy.
        self.buffer = next;
        self.allocation = allocation;
        if let Some(replacement) = replacement {
            match replacement.commit() {
                Ok(allocation) => self.allocation = Some(Arc::new(allocation)),
                Err((error, replacement)) => {
                    self.buffer = BytesMut::new();
                    drop(replacement);
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub fn consumed(&mut self) {
        // Buffered tail bytes have already arrived. Processing the preceding
        // packet must not grant them a fresh read budget.
        if self.buffer.is_empty() {
            self.started = None;
            // A completed packet must not pin its read allocation for the
            // lifetime of an otherwise idle connection, including small packets.
            self.buffer = BytesMut::new();
            self.allocation = None;
        }
    }
    pub async fn read_more(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + ?Sized),
        idle_deadline: Instant,
    ) -> Result<()> {
        let target = self.buffer.len().saturating_add(4096).min(self.maximum);
        self.read_more_to(stream, idle_deadline, target).await
    }
    /// The protocol validates its header before asking for a complete frame.
    /// No socket read is allowed to grow beyond this checked target.
    pub async fn read_more_to(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + ?Sized),
        idle_deadline: Instant,
        target: usize,
    ) -> Result<()> {
        let deadline = self.deadline(idle_deadline);
        // Tokio timeout polls the I/O future first: ready bytes alone must not
        // let an already expired incomplete packet escape the whole deadline.
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        if target > self.maximum {
            return Err(Error::Invalid);
        }
        let room = target
            .checked_sub(self.buffer.len())
            .ok_or(Error::Invalid)?;
        if room == 0 {
            return Err(Error::Invalid);
        }
        let read = if self.buffer.is_empty() {
            // The idle future retains eight bytes, and no heap allocation. Most
            // MQTT control packets fit here. Allocate only after bytes arrive.
            let mut first = [0u8; 8];
            let n = room.min(first.len());
            let read = tokio::time::timeout_at(deadline, stream.read(&mut first[..n]))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Unavailable)?;
            if read != 0 {
                let (buffer, allocation) = self.allocate(read)?;
                self.buffer = buffer;
                self.allocation = allocation;
                self.buffer.extend_from_slice(&first[..read]);
            }
            read
        } else {
            if self.buffer.capacity() < target {
                self.grow(target)?;
            }
            let mut destination = (&mut self.buffer).limit(room);
            tokio::time::timeout_at(deadline, stream.read_buf(&mut destination))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Unavailable)?
        };
        // timeout_at polls ready I/O before its timer, including after a
        // suspended read is rescheduled. Never accept bytes past this deadline.
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        if read == 0 {
            return Err(Error::Unavailable);
        }
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
        Ok(())
    }
    fn deadline(&self, idle_deadline: Instant) -> Instant {
        self.started
            .map(|t| t + self.read_timeout)
            .unwrap_or(idle_deadline)
            .min(idle_deadline)
    }
}
