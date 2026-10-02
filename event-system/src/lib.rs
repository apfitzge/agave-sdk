#![cfg_attr(docsrs, feature(doc_cfg))]
//! An event system implemented on Linux.
//!
//! On all other targets, the public API is available but all operations are
//! no-ops.

// This is needed to use the `#[event]` macro in order to resolve the [`Event`] trait.
// The reason is that the macro expands to use [`agave_event_system::Event`],
// but in the self crate it is referred to as [`crate::Event`].
#[cfg(test)]
extern crate self as agave_event_system;

pub use {
    crate::{
        event_system::{
            CreateEventSystemError, CreateStreamError, EventQueueError, EventSystem, StreamConfig,
        },
        publisher_factory::PublisherFactory,
        queue_cell::event_queue_cell_size,
        timestamp::monotonic_timestamp_ns,
    },
    agave_event_system_derive::event,
};
// wincode and wincode-dynamic are part of public API.
pub use {wincode, wincode_dynamic};
use {
    wincode::{SchemaWrite, config::DefaultConfig},
    wincode_dynamic::SchemaDynamic,
};

pub mod publisher;
pub mod stream_name;
pub mod stream_policy;
pub mod subscriber;

#[doc(hidden)]
pub mod __private {
    pub use crate::{payload::PayloadSlice, stream_name::macro_support::stream_name};

    pub mod event_macro {
        pub use {wincode::*, wincode_dynamic::*};
    }
}

#[cfg_attr(target_os = "linux", path = "backend/linux.rs")]
#[cfg_attr(not(target_os = "linux"), path = "backend/stub.rs")]
mod backend;
#[cfg(target_os = "linux")] // cache_padded is only used by linux backend as of now
pub(crate) mod cache_padded;
mod event_system;
mod payload;
mod publisher_factory;
mod queue_cell;
mod timestamp;

/// An event type that can be sent on an event stream.
///
/// The [`Event`] trait should only be implemented with the [`event`] macro.
///
/// ```
/// # use agave_event_system::event;
/// #[event]
/// #[derive(Debug, PartialEq, Eq)]
/// enum SlotEvents {
///     Completed { slot: u64 },
/// }
/// ```
///
/// Events containing dynamically sized values must declare a
/// `max_serialized_size` strictly greater than the statically known portion of
/// their encoding.
///
/// ```
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 1024)]
/// struct Message {
///     contents: String,
/// }
/// ```
///
/// Omitting the bound for a dynamically sized event is a compile-time error.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event]
/// struct UnboundedMessage {
///     contents: String,
/// }
/// ```
///
/// The bound must also exceed wincode's dynamic serialized-size lower bound.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 8)]
/// struct UndersizedMessage {
///     fixed: u64,
///     contents: String,
/// }
/// ```
///
/// Mark a borrowed byte slice with `#[payload]`. Publication copies its bytes
/// into shared memory; typed decoding borrows them from the held message.
/// Dynamic decoding sees only numeric `account_data_offset` and `account_data_len`.
///
/// ```
/// use agave_event_system::{EventSystem, StreamConfig, event, stream_name, subscriber::StreamExplorer};
/// #[event]
/// struct AccountUpdate<'a> {
///     slot: u64,
///     #[payload]
///     account_data: &'a [u8],
/// }
/// # #[cfg(target_os = "linux")] {
/// let directory = tempfile::tempdir().unwrap();
/// let system = EventSystem::new(directory.path()).unwrap();
/// system.set_stream_policy("on".parse().unwrap());
/// let factory = system.create_stream_with_payloads::<AccountUpdate>(
///     stream_name!("accounts"),
///     StreamConfig { capacity: 8, publisher_slots: 1, subscriber_slots: 1 },
///     1024 * 1024,
/// ).unwrap();
/// let mut publisher = factory.try_create_publisher().unwrap();
/// let mut subscriber = StreamExplorer::new(directory.path().into())
///     .available_streams().next().unwrap().try_connect_typed::<AccountUpdate>().unwrap();
/// let bytes = b"account data".to_vec();
/// publisher.publish(&AccountUpdate { slot: 42, account_data: &bytes }).unwrap();
/// drop(bytes);
/// let held = subscriber.try_recv().unwrap();
/// let update = held.decode().unwrap();
/// assert_eq!(update.slot, 42);
/// assert_eq!(update.account_data, b"account data");
/// # }
/// ```
///
/// A marker must designate a borrowed byte slice, with at most one per struct/variant.
///
/// ```compile_fail
/// #[agave_event_system::event]
/// struct Invalid { #[payload] data: u64 }
/// ```
///
/// ```compile_fail
/// #[agave_event_system::event]
/// struct Two<'a> { #[payload] a: &'a [u8], #[payload] b: &'a [u8] }
/// ```
///
/// Generated wire field names must not collide with metadata fields.
///
/// ```compile_fail
/// #[agave_event_system::event]
/// struct Collision<'a> { #[payload] x: &'a [u8], x_offset: u64 }
/// ```
///
/// # Safety
///
/// [`Event::QueueCell`] must be valid for every bit pattern, contain no
/// uninitialized padding, and expose its complete representation through
/// [`AsMut<[u8]>`]. The [`event`] macro satisfies these requirements by using
/// `[u8; N]`.
/// `HAS_PAYLOAD` must be true whenever `PAYLOAD_FIELDS` is nonempty.
/// `PAYLOAD_FIELDS` must identify the payload handle field in each applicable
/// variant, consistently with the runtime schema: two adjacent u64 fields,
/// offset then length. `payload_data` must return the corresponding source bytes.
/// Every `View` must have the same wire schema, markers, and queue cell layout.
/// Its header writer must encode placeholders, never caller-supplied handles.
pub unsafe trait Event:
    Sized + SchemaDynamic + SchemaWrite<DefaultConfig, Src = Self>
{
    /// The fixed-size storage used for an encoded event in a queue.
    ///
    /// This associated type works around the lack of stable generic const
    /// expressions. The [`event`] macro defines it as a byte array sized from
    /// [`SchemaDynamic::SERIALIZED_SIZE`].
    type QueueCell: Copy + AsMut<[u8]>;

    /// The public event with its payload borrowed for the given lifetime.
    /// Fixed-size events use Self; payload events substitute their lifetimes.
    type View<'a>: Event<QueueCell = Self::QueueCell>;

    /// Decode metadata and attach the payload already resolved by the held guard.
    #[doc(hidden)]
    fn decode_event<'a>(header: &[u8], payload: &'a [u8]) -> wincode::ReadResult<Self::View<'a>>;

    /// Whether any variant contains a `#[payload]` field.
    const HAS_PAYLOAD: bool = false;

    /// Generated marker metadata: (variant name, offset field name). Structs use None;
    /// tuple fields use their numeric index as the name.
    #[doc(hidden)]
    const PAYLOAD_FIELDS: &'static [(Option<&'static str>, &'static str)] = &[];

    /// The source bytes of this value's marked payload, if present.
    #[doc(hidden)]
    fn payload_data(&self) -> Option<&[u8]> {
        None
    }
}
