use {
    super::payload_ring::PayloadRing,
    crate::{
        Event,
        backend::linux::{AtomicStreamRule, StreamGuard},
        publisher::PublishError,
    },
    std::{collections::VecDeque, fmt::Debug, num::NonZeroUsize, sync::Arc},
};

/// Publishes events of a specific type to a stream.
pub(crate) struct Publisher<E: Event> {
    broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
    stream_guard: Arc<StreamGuard>,
    stream_rule: Arc<AtomicStreamRule>,
    payload: Option<PayloadState>,
}

struct PayloadState {
    ring: PayloadRing,
    // Preallocated to queue capacity; commit never allocates bookkeeping memory.
    allocations: VecDeque<(usize, u64)>,
}

impl<E: Event> Publisher<E> {
    pub(crate) fn publish(&mut self, event: &E::View<'_>) -> Result<(), PublishError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }

        if let Some(bytes) = event.payload_data() {
            return self.publish_with_payload(event, bytes);
        }
        let mut prepared = self
            .broadcast_sender
            .try_prepare_write()
            .ok_or(PublishError::FailedToSend)?;

        // SAFETY: the memfd starts zero-filled, and serialization only writes initialized
        // bytes. Event::QueueCell is valid for every bit pattern and has no padding.
        let cell = unsafe { prepared.as_mut().assume_init_mut() };

        event
            .write_event(cell.as_mut(), [0, 0])
            .map_err(PublishError::Serialization)?;

        // SAFETY: serialization succeeded, and the entire cell remains initialized,
        // including any unused bytes after the encoded event.
        unsafe { prepared.commit_initialized() };

        Ok(())
    }

    fn publish_with_payload(
        &mut self,
        event: &E::View<'_>,
        bytes: &[u8],
    ) -> Result<(), PublishError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }
        let state = self
            .payload
            .as_mut()
            .ok_or(PublishError::PayloadNotEnabled)?;
        let storage = self
            .stream_guard
            .payload_storage
            .as_ref()
            .ok_or(PublishError::PayloadNotEnabled)?;
        // Prove cell capacity before allocation, serialization, or payload copying.
        let mut prepared = self
            .broadcast_sender
            .try_prepare_write()
            .ok_or(PublishError::FailedToSend)?;
        let before = prepared.reclaimable_before();
        while state
            .allocations
            .front()
            .is_some_and(|&(sequence, _)| sequence < before)
        {
            let (_, end) = state.allocations.pop_front().unwrap();
            // Each record belongs to this lane; zero-byte records can precede an
            // empty-ring padding adjustment, so their stale ends need no action.
            state.ring.reclaim_through(end);
        }
        if state.allocations.len() == state.allocations.capacity() {
            return Err(PublishError::PayloadCapacity);
        }
        let reservation = state
            .ring
            .reserve(bytes.len() as u64)
            .map_err(|_| PublishError::PayloadCapacity)?;
        let handle = reservation.handle();
        let sequence = prepared.sequence();
        let lane = prepared.producer_index();
        // SAFETY: the memfd is zero-filled and serialization leaves initialized bytes.
        let cell = unsafe { prepared.as_mut().assume_init_mut() };
        event
            .write_event(cell.as_mut(), [handle.offset, handle.len])
            .map_err(PublishError::Serialization)?;
        // SAFETY: this producer exclusively owns its non-reused lane. The FIFO
        // allocator reuses only prefixes proven inaccessible by this lane's
        // synchronized watermark. bytes cannot alias an unpublished allocation.
        unsafe { storage.write(lane, handle, bytes) }.map_err(PublishError::PayloadStorage)?;
        let end = reservation.commit();
        state.allocations.push_back((sequence, end)); // capacity was checked above
        // SAFETY: serialization and copying succeeded; no fallible work remains.
        unsafe { prepared.commit_initialized() };
        Ok(())
    }

    /// Publishes the given batch of events.
    ///
    /// # Errors
    /// If the whole batch does not fit, no events are sent. If serialization
    /// fails, only the successfully serialized prefix is sent.
    pub(crate) fn publish_batch(&mut self, events: &[E::View<'_>]) -> Result<(), PublishError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }

        if events.iter().any(|event| event.payload_data().is_some()) {
            return Err(PublishError::PayloadBatchUnsupported);
        }
        let Ok(event_count) = NonZeroUsize::try_from(events.len()) else {
            // nothing to write
            return Ok(());
        };
        let mut prepared = self
            .broadcast_sender
            .try_prepare_write_batch(event_count)
            .ok_or(PublishError::FailedToSend)?;

        for (i, event) in events.iter().enumerate() {
            // SAFETY: the memfd starts zero-filled, and serialization only writes initialized
            // bytes. Event::QueueCell is valid for every bit pattern and has no padding.
            let cell = unsafe { prepared.as_mut(i).assume_init_mut() };

            let result = event
                .write_event(cell.as_mut(), [0, 0])
                .map_err(PublishError::Serialization);
            if let Err(error) = result {
                // SAFETY: cells before i contain successfully serialized events with
                // fully initialized bytes. The failed cell and suffix are not published.
                unsafe { prepared.commit_prefix(i) };
                return Err(error);
            }
        }

        // SAFETY: every cell contains a successfully serialized event and all bytes,
        // including unused trailing bytes, remain initialized.
        unsafe { prepared.commit() };

        Ok(())
    }
}

impl<E: Event> Publisher<E> {
    pub(super) fn new(
        broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
        stream_guard: Arc<StreamGuard>,
        stream_rule: Arc<AtomicStreamRule>,
    ) -> Self {
        let payload = PayloadRing::new(stream_guard.payload_capacity).map(|ring| PayloadState {
            ring,
            allocations: VecDeque::with_capacity(stream_guard.queue_capacity),
        });
        Self {
            payload,
            broadcast_sender,
            stream_guard,
            stream_rule,
        }
    }
}
impl<E: Event> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("broadcast_sender", &self.broadcast_sender)
            .field("stream_guard", &self.stream_guard)
            .finish()
    }
}
