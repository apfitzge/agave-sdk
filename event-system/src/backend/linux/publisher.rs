use {
    crate::{
        Event,
        backend::{AtomicStreamRule, StreamGuard},
        publisher::PublishError,
    },
    std::{fmt::Debug, num::NonZeroUsize, sync::Arc},
};

/// Publishes events of a specific type to a stream.
pub(crate) struct Publisher<E: Event> {
    broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
    stream_guard: Arc<StreamGuard>,
    stream_rule: Arc<AtomicStreamRule>,
}

impl<E: Event> Publisher<E> {
    pub(crate) fn publish(&mut self, event: &E) -> Result<(), PublishError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }

        let mut prepared = self
            .broadcast_sender
            .try_prepare_write()
            .ok_or(PublishError::FailedToSend)?;

        // SAFETY: the memfd starts zero-filled, and serialization only writes initialized
        // bytes. Event::QueueCell is valid for every bit pattern and has no padding.
        let cell = unsafe { prepared.as_mut().assume_init_mut() };

        wincode::serialize_into(cell.as_mut(), &event).map_err(PublishError::Serialization)?;

        // SAFETY: serialization succeeded, and the entire cell remains initialized,
        // including any unused bytes after the encoded event.
        unsafe { prepared.commit_initialized() };

        Ok(())
    }

    /// Publishes the given batch of events.
    ///
    /// # Errors
    /// If the whole batch does not fit, no events are sent. If serialization
    /// fails, only the successfully serialized prefix is sent.
    pub(crate) fn publish_batch(&mut self, events: &[E]) -> Result<(), PublishError> {
        if !self.stream_rule.is_on() {
            return Ok(());
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

            if let Err(error) = wincode::serialize_into(cell.as_mut(), &event) {
                // SAFETY: cells before i contain successfully serialized events with
                // fully initialized bytes. The failed cell and suffix are not published.
                unsafe { prepared.commit_prefix(i) };
                return Err(PublishError::Serialization(error));
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
        Self {
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
