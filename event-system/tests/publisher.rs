#![cfg(target_os = "linux")]

mod common;

use {
    agave_event_system::{
        Event, StreamConfig, event,
        publisher::{PublishError, Publisher},
        subscriber::{StreamExplorer, Subscriber, TryRecvError, Typed},
        wincode::{SchemaRead, SchemaWrite, WriteResult, config::DefaultConfig, io::Writer},
        wincode_dynamic::SchemaDynamic,
    },
    common::{TEST_STREAM_NAME, TestContext, TestContextBuilder},
    rstest::rstest,
    std::{
        assert_matches,
        panic::{AssertUnwindSafe, catch_unwind},
    },
};

#[event(max_serialized_size = 24)]
#[derive(Debug, PartialEq)]
struct BoundedEvent {
    id: u64,
    data: Vec<u8>,
}

fn event(id: u64) -> BoundedEvent {
    BoundedEvent { id, data: vec![1] }
}

fn oversized_event() -> BoundedEvent {
    BoundedEvent {
        id: u64::MAX,
        data: vec![2; 32],
    }
}

fn setup<E: Event>(capacity: usize) -> (TestContext, Publisher<E>, Subscriber<Typed<E>>) {
    let context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let factory = context
        .event_system
        .create_stream::<E>(
            TEST_STREAM_NAME,
            StreamConfig {
                capacity,
                publisher_slots: 1,
                subscriber_slots: 1,
            },
        )
        .unwrap();
    let publisher = factory.try_create_publisher().unwrap();
    let subscriber = StreamExplorer::new(context.event_system_path())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<E>()
        .unwrap();
    (context, publisher, subscriber)
}

#[test]
fn serialization_failure_cancels_single_event_and_preserves_capacity() {
    let (_context, mut publisher, mut subscriber) = setup::<BoundedEvent>(1);
    for id in 0..3 {
        assert_matches!(
            publisher.publish(&oversized_event()),
            Err(PublishError::Serialization(_))
        );
        assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
        publisher.publish(&event(id)).unwrap();
        assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), event(id));
    }
}

#[rstest]
fn batch_error_publishes_only_successful_prefix(
    #[values(0, 1, 2)] failure_index: usize,
    #[values(0, 3)] starting_sequence: u64,
) {
    let (_context, mut publisher, mut subscriber) = setup::<BoundedEvent>(4);
    // Start near the physical end as well as at zero to exercise a wrapping batch.
    for id in 0..starting_sequence {
        publisher.publish(&event(id)).unwrap();
        assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), event(id));
    }

    let mut batch = [event(10), event(11), event(12)];
    batch[failure_index] = oversized_event();
    assert_matches!(
        publisher.publish_batch(&batch),
        Err(PublishError::Serialization(_))
    );

    // The cancelled suffix must be immediately available, even while the prefix
    // remains unread. Fill the entire remaining capacity before consuming it.
    let subsequent: Vec<_> = (20..24).map(event).skip(failure_index).collect();
    publisher.publish_batch(&subsequent).unwrap();
    for expected in batch[..failure_index].iter().chain(&subsequent) {
        assert_eq!(&subscriber.try_recv().unwrap().decode().unwrap(), expected);
    }
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn backpressure_precedes_serialization_and_preserves_held_event() {
    let (_context, mut publisher, mut subscriber) = setup::<BoundedEvent>(2);
    publisher.publish_batch(&[event(1), event(2)]).unwrap();
    let held = subscriber.try_recv().unwrap();

    assert_matches!(
        publisher.publish(&oversized_event()),
        Err(PublishError::FailedToSend)
    );
    assert_matches!(
        publisher.publish_batch(&[oversized_event()]),
        Err(PublishError::FailedToSend)
    );
    publisher.publish_batch(&[]).unwrap();
    assert_eq!(held.decode().unwrap(), event(1));
    drop(held);

    // One cell is free, but the entire two-cell batch must fit before encoding.
    assert_matches!(
        publisher.publish_batch(&[event(3), oversized_event()]),
        Err(PublishError::FailedToSend)
    );
    publisher.publish(&event(4)).unwrap();
    assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), event(2));
    assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), event(4));
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn disabled_stream_skips_serialization_and_empty_batches_publish_nothing() {
    let (context, mut publisher, mut subscriber) = setup::<BoundedEvent>(1);
    publisher.publish_batch(&[]).unwrap();
    context
        .event_system
        .set_stream_policy("off".parse().unwrap());
    publisher.publish(&oversized_event()).unwrap();
    publisher
        .publish_batch(&[event(1), oversized_event()])
        .unwrap();
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));

    context
        .event_system
        .set_stream_policy("on".parse().unwrap());
    publisher.publish_batch(&[event(2)]).unwrap();
    assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), event(2));
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
}

// A custom writer lets the test unwind after partially writing a cell.
#[derive(Debug, PartialEq, SchemaRead, SchemaDynamic)]
struct PanickingEvent {
    id: u64,
    value: u64,
}

// SAFETY: successful writes serialize exactly two u64 fields (16 bytes), matching
// size_of. TYPE_META retains its conservative default.
unsafe impl SchemaWrite<DefaultConfig> for PanickingEvent {
    type Src = Self;

    fn size_of(_src: &Self) -> WriteResult<usize> {
        Ok(16)
    }

    fn write(mut writer: impl Writer, src: &Self) -> WriteResult<()> {
        <u64 as SchemaWrite<DefaultConfig>>::write(writer.by_ref(), &src.id)?;
        assert_ne!(src.value, u64::MAX, "injected serialization panic");
        <u64 as SchemaWrite<DefaultConfig>>::write(writer, &src.value)
    }
}

// SAFETY: the cell is a byte array with no padding, valid for every bit pattern.
unsafe impl Event for PanickingEvent {
    type QueueCell = [u8; 16];
}

#[rstest]
fn serialization_panic_cancels_preparation(#[values(false, true)] batch: bool) {
    let (_context, mut publisher, mut subscriber) = setup::<PanickingEvent>(2);
    let valid = PanickingEvent { id: 1, value: 2 };
    let panicking = PanickingEvent {
        id: 3,
        value: u64::MAX,
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        if batch {
            publisher.publish_batch(&[valid, panicking]).unwrap();
        } else {
            publisher.publish(&panicking).unwrap();
        }
    }));
    assert!(result.is_err());
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));

    let subsequent = [
        PanickingEvent { id: 4, value: 5 },
        PanickingEvent { id: 6, value: 7 },
    ];
    publisher.publish_batch(&subsequent).unwrap();
    for expected in subsequent {
        assert_eq!(subscriber.try_recv().unwrap().decode().unwrap(), expected);
    }
    assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
}
