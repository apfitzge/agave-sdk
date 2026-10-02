pub use wincode::ReadError;
use {
    crate::{backend, stream_name::StreamName},
    std::{marker::PhantomData, path::PathBuf, time::Duration},
    wincode_dynamic::{Decoder, Fields, RootSchema},
};

/// A [`StreamExplorer`] listens to a given directory for event streams that are created
/// by [`EventSystem::create_stream`](crate::EventSystem::create_stream) in the same
/// directory.
///
/// To subscribe to a stream, simply use the [`StreamExplorer::available_streams] API
/// which yields an iterator over streams that can be subscribed to.
#[derive(Debug)]
pub struct StreamExplorer(backend::StreamExplorer);

impl StreamExplorer {
    pub fn new(event_system_directory: PathBuf) -> Self {
        Self(backend::StreamExplorer::new(event_system_directory))
    }

    /// Iterates over streams that are available to be subscribed to.
    ///
    /// A stream can be subscribed to with [`AvailableStream::try_connect_dynamic`] or
    /// [`AvailableStream::try_connect_typed`].
    pub fn available_streams(&self) -> impl Iterator<Item = AvailableStream> + '_ {
        self.0.available_streams().map(AvailableStream)
    }
}

/// Marker for dynamically reflecting over stream messages.
/// It exposes numeric payload offset/length fields, with no allocation accessor.
/// See [`Subscriber`] for details on subscriber modes.
///
/// ```compile_fail
/// use agave_event_system::subscriber::{Dynamic, StreamMessage};
/// fn invalid(message: &StreamMessage<'_, Dynamic>) {
///     let _ = message.payload();
/// }
/// ```
#[derive(Debug)]
pub struct Dynamic;

/// Marker for decoding stream messages into a statically typed `T`.
/// See [`Subscriber`] for details on subscriber modes.
pub struct Typed<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for Typed<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Typed")
            .field("_marker", &self._marker)
            .finish()
    }
}

/// A subscriber to a specific event stream. This
/// subscriber can be created with either [`Dynamic`] mode
/// which lets users dynamically reflect over messages on the stream,
/// or the [`Typed<T>`] mode which returns the T directly if the user
/// knows what T is at compile time.
pub struct Subscriber<Mode> {
    backend: backend::Subscriber,
    mode: PhantomData<Mode>,
}

impl<Mode> std::fmt::Debug for Subscriber<Mode> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Subscriber")
            .field("mode", &std::any::type_name::<Mode>())
            .field("backend", &self.backend)
            .finish()
    }
}

impl<Mode> Subscriber<Mode> {
    /// The name of the stream the subscriber is listening on.
    pub fn stream_name(&self) -> &StreamName {
        self.backend.stream_name()
    }
    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.backend.type_name()
    }

    /// Returns a message if there is any unseen message in the stream.
    pub fn try_recv(&mut self) -> Result<StreamMessage<'_, Mode>, TryRecvError> {
        self.backend.try_recv().map(StreamMessage::new)
    }

    /// Returns a message if there is any unseen message in the stream within the given timeout duration.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<StreamMessage<'_, Mode>, RecvTimeoutError> {
        self.backend.recv_timeout(timeout).map(StreamMessage::new)
    }

    fn new(backend: backend::Subscriber) -> Self {
        Self {
            backend,
            mode: PhantomData,
        }
    }
}

/// A message received from a stream with [`Subscriber::try_recv`].
pub struct StreamMessage<'a, Mode> {
    backend: backend::StreamMessage<'a>,
    mode: PhantomData<Mode>,
}

impl<Mode> std::fmt::Debug for StreamMessage<'_, Mode> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamMessage")
            .field("mode", &std::any::type_name::<Mode>())
            .field("backend", &self.backend)
            .finish()
    }
}

impl<'a, Mode> StreamMessage<'a, Mode> {
    /// [`PublisherMetadata`] of the publisher of this message.
    pub fn publisher_metadata(&self) -> PublisherMetadata<'_> {
        PublisherMetadata(self.backend.publisher_metadata())
    }

    fn new(backend: backend::StreamMessage<'a>) -> Self {
        Self {
            backend,
            mode: PhantomData,
        }
    }
}

impl<T: crate::Event> StreamMessage<'_, Typed<T>> {
    /// Decode an event, borrowing marked payload slices from this held cell.
    /// The returned event cannot retain those slices after the cell is released.
    ///
    /// ```compile_fail
    /// use agave_event_system::{event, subscriber::{Subscriber, Typed}};
    /// #[event]
    /// struct Update<'a> { #[payload] data: &'a [u8] }
    /// fn invalid(subscriber: &mut Subscriber<Typed<Update<'static>>>) {
    ///     let held = subscriber.try_recv().unwrap();
    ///     let update = held.decode().unwrap();
    ///     drop(held);
    ///     println!("{:?}", update.data);
    /// }
    /// ```
    pub fn decode(&self) -> Result<T::View<'_>, ReadError> {
        let payload = if T::HAS_PAYLOAD {
            self.backend
                .shared_payload()
                .map_err(|_| ReadError::InvalidValue("invalid shared payload"))?
        } else {
            &[]
        };
        T::decode_event(self.backend.payload(), payload)
    }
}

impl<'a> StreamMessage<'a, Dynamic> {
    /// Decodes the message into a [`DecodedMessage`], whose fields can be
    /// reflected over.
    pub fn decode<'de>(&'de self) -> Result<DecodedMessage<'a, 'de>, ReadError> {
        Ok(match Decoder::new(self.backend.schema()) {
            Decoder::Struct(schema_decoder) => DecodedMessage::Struct {
                fields: schema_decoder.fields(self.backend.payload()),
            },
            Decoder::Enum(enum_decoder) => {
                let variant_decoder = enum_decoder.decode_variant(self.backend.payload())?;
                DecodedMessage::Enum {
                    variant_name: variant_decoder.variant_name(),
                    fields: variant_decoder.fields(),
                }
            }
        })
    }
}

/// Metadata of the [`Publisher`](crate::publisher::Publisher) lane that a [`StreamMessage`] was published on.
#[derive(Clone, Copy, Debug)]
pub struct PublisherMetadata<'a>(backend::PublisherMetadata<'a>);

impl PublisherMetadata<'_> {
    /// The lane of the publisher that sent this event.
    pub fn lane(&self) -> usize {
        self.0.lane()
    }

    /// The thread id of the publisher that sent the event.
    pub fn thread_id(&self) -> u64 {
        self.0.thread_id()
    }

    /// The number of events the publisher could not publish on this lane because
    /// subscribers did not consume them fast enough.
    pub fn rejected_items(&self) -> u64 {
        self.0.rejected_items()
    }
}

/// A discovered stream that can be connected to.
#[derive(Debug)]
pub struct AvailableStream(backend::AvailableStream);

impl AvailableStream {
    /// The name of the available stream.
    pub fn stream_name(&self) -> &StreamName {
        self.0.stream_name()
    }

    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.0.type_name()
    }

    /// Attempts to connect to the stream with dynamic decoding of the messages.
    ///
    /// This method should be used over [`try_connect_typed`](Self::try_connect_typed) when you don't know what
    /// concrete rust type is sent on the stream.
    pub fn try_connect_dynamic(self) -> Result<Subscriber<Dynamic>, TryConnectError> {
        self.0.try_connect().map(Subscriber::<Dynamic>::new)
    }

    /// Attempts to connect to the stream with static decoding of the messages into a concrete type `T`.
    ///
    /// If you don't know at compile time what type the stream contains you should instead use
    /// [`try_connect_dynamic`](Self::try_connect_dynamic).
    pub fn try_connect_typed<T: wincode_dynamic::SchemaDynamic>(
        self,
    ) -> Result<Subscriber<Typed<T>>, TryConnectTypedError> {
        let expected_schema = T::schema();
        let actual_schema = self.0.stream_schema();

        if expected_schema != *actual_schema {
            return Err(TryConnectTypedError::SchemaMismatch(Box::new(
                SchemaMismatch {
                    expected: expected_schema,
                    actual: actual_schema.clone(),
                },
            )));
        }

        self.0
            .try_connect()
            .map(Subscriber::<Typed<T>>::new)
            .map_err(TryConnectTypedError::Connection)
    }
}

/// Errors that can arise when trying to receive an event on a specific event stream
/// through a subscriber.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    #[error("the stream has no new message")]
    Empty,
}

/// An error if no new message is observed on a stream within a timeout duration.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[error("the stream had no new message within the timeout")]
pub struct RecvTimeoutError;

#[derive(Debug, thiserror::Error)]
pub enum TryConnectTypedError {
    #[error("stream schema does not match the requested type")]
    SchemaMismatch(Box<SchemaMismatch>),

    #[error(transparent)]
    Connection(#[from] TryConnectError),
}

#[derive(Debug)]
pub struct SchemaMismatch {
    pub expected: RootSchema,
    pub actual: RootSchema,
}

#[derive(Debug, thiserror::Error)]
pub enum TryConnectError {
    #[error("the stream has no available subscriber slots")]
    SubscriberSlotsExhausted,
}

/// A decoded dynamically typed message. Messages can either be an enum or a struct.
#[derive(Debug)]
pub enum DecodedMessage<'a, 'de> {
    Struct {
        fields: Fields<'a, 'de, &'de [u8]>,
    },
    Enum {
        fields: Fields<'a, 'de, &'de [u8]>,
        variant_name: &'a str,
    },
}
