use {
    crate::{Event, backend},
    std::{fmt::Debug, marker::PhantomData, rc::Rc},
};

/// Publishes events of a specific type to a stream.
///
/// [`Publisher<T>`] is [`!Send`](Send) + [`!Sync`](Sync), as a publisher is associated
/// with a thread for its entire lifetime.
pub struct Publisher<E: Event> {
    inner: Backend<E>,
    //  `Rc` is !Send + !Sync, which makes Publisher<E> also neither
    _not_send_or_sync: PhantomData<Rc<()>>,
}

#[cfg_attr(
    target_os = "linux",
    expect(
        clippy::large_enum_variant,
        reason = "keep the platform publisher inline without allocation or publication indirection"
    )
)]
enum Backend<E: Event> {
    Platform(backend::Publisher<E>),
    Stub(backend::stub::Publisher<E>),
}

impl<E: Event> Publisher<E> {
    /// Creates a no-op publisher on any platform without accessing the filesystem.
    ///
    /// Published events are discarded without serialization.
    pub fn stub() -> Self {
        Self::from_stub(backend::stub::Publisher::new())
    }

    /// Publishes an event if the stream is enabled.
    ///
    /// Returns an error without publishing the event if the queue is full or
    /// serialization fails. Marked payload slices are copied into shared memory;
    /// their source buffers need only remain valid for this call.
    pub fn publish(&mut self, event: &E::View<'_>) -> Result<(), PublishError> {
        match &mut self.inner {
            Backend::Platform(inner) => inner.publish(event),
            Backend::Stub(inner) => inner.publish(event),
        }
    }

    /// Publishes the given batch of events on the stream.
    ///
    /// # Errors
    /// If the whole batch does not fit in the queue, [`PublishError::FailedToSend`]
    /// is returned without publishing any events. If serialization fails,
    /// [`PublishError::Serialization`] is returned after publishing only the
    /// successfully serialized prefix. The failing event and remaining suffix
    /// are not published.
    ///
    /// A panic during serialization cancels the entire batch.
    /// Payload-bearing variants are currently supported only by `publish`;
    /// a batch containing one is rejected before publishing any events.
    pub fn publish_batch(&mut self, events: &[E::View<'_>]) -> Result<(), PublishError> {
        match &mut self.inner {
            Backend::Platform(inner) => inner.publish_batch(events),
            Backend::Stub(inner) => inner.publish_batch(events),
        }
    }

    pub(crate) fn new(inner: backend::Publisher<E>) -> Self {
        Self {
            inner: Backend::Platform(inner),
            _not_send_or_sync: PhantomData,
        }
    }
    pub(crate) fn from_stub(inner: backend::stub::Publisher<E>) -> Self {
        Self {
            inner: Backend::Stub(inner),
            _not_send_or_sync: PhantomData,
        }
    }
}

impl<E: Event> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Backend::Platform(inner) => Debug::fmt(inner, f),
            Backend::Stub(inner) => Debug::fmt(inner, f),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to serialize the event")]
    Serialization(wincode::WriteError),
    #[error("Failed to send the event. Back-pressured by event subscribers.")]
    FailedToSend,
    #[error("the event or stream does not support shared payloads")]
    PayloadNotEnabled,
    #[error("publish payload-bearing events individually")]
    PayloadBatchUnsupported,
    #[error("payload ring is full, payload is too large, or its positions are exhausted")]
    PayloadCapacity,
    #[error("invalid payload storage access")]
    PayloadStorage(#[source] std::io::Error),
}
