#![cfg(target_os = "linux")]

use {
    agave_event_system::{
        EventSystem, StreamConfig, event,
        publisher::PublishError,
        stream_name,
        subscriber::{DecodedMessage, StreamExplorer, Subscriber, Typed},
        wincode_dynamic::Value,
    },
    std::assert_matches,
};

#[event(max_serialized_size = 40)]
#[derive(Debug, Default)]
struct Update<'a> {
    slot: u64,
    metadata: Vec<u8>,
    #[payload]
    data: &'a [u8],
}

fn connect(directory: &std::path::Path) -> Subscriber<Typed<Update<'static>>> {
    StreamExplorer::new(directory.into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<Update>()
        .unwrap()
}

fn setup(
    capacity: usize,
    bytes: u64,
) -> (
    tempfile::TempDir,
    EventSystem,
    agave_event_system::PublisherFactory<Update<'static>>,
) {
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let factory = system
        .create_stream_with_payloads::<Update>(
            stream_name!("updates"),
            StreamConfig {
                capacity,
                publisher_slots: 2,
                subscriber_slots: 3,
            },
            bytes,
        )
        .unwrap();
    (directory, system, factory)
}

#[test]
fn held_payload_waits_for_every_consumer_and_wraps_without_overlap() {
    let (directory, _system, factory) = setup(8, 10);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut first = connect(directory.path());
    let mut second = connect(directory.path());
    publisher
        .publish(&Update {
            data: b"abcdef",
            ..Update::default()
        })
        .unwrap();
    publisher
        .publish(&Update {
            data: b"gh",
            ..Update::default()
        })
        .unwrap();
    let held = first.try_recv().unwrap();
    assert_eq!(held.decode().unwrap().data, b"abcdef");
    drop(second.try_recv().unwrap());
    assert_matches!(
        publisher.publish(&Update {
            data: b"ijklm",
            ..Update::default()
        }),
        Err(PublishError::PayloadCapacity)
    );
    drop(held);
    publisher
        .publish(&Update {
            data: b"ijklm",
            ..Update::default()
        })
        .unwrap();
    let held = first.try_recv().unwrap();
    assert_eq!(held.decode().unwrap().data, b"gh");
    drop(held);
    let wrapped = first.try_recv().unwrap();
    assert_eq!(wrapped.decode().unwrap().data, b"ijklm");
    assert_eq!(second.try_recv().unwrap().decode().unwrap().data, b"gh");
    assert_eq!(second.try_recv().unwrap().decode().unwrap().data, b"ijklm");
}

#[test]
fn queue_backpressure_precedes_serialization_and_payload_work() {
    let (directory, _system, factory) = setup(1, 4);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = connect(directory.path());
    publisher
        .publish(&Update {
            data: b"data",
            ..Update::default()
        })
        .unwrap();
    let invalid = Update {
        metadata: vec![0; 100],
        ..Update::default()
    };
    assert_matches!(
        publisher.publish(&Update {
            data: b"oversized",
            ..invalid
        }),
        Err(PublishError::FailedToSend)
    );
    let held = subscriber.try_recv().unwrap();
    assert_eq!(held.decode().unwrap().data, b"data");
    drop(held);
    assert_matches!(
        publisher.publish(&Update {
            metadata: vec![0; 100],
            data: b"data",
            ..Update::default()
        }),
        Err(PublishError::Serialization(_))
    );
    assert!(subscriber.try_recv().is_err());
    publisher
        .publish(&Update {
            data: b"next",
            ..Update::default()
        })
        .unwrap();
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"next"
    );
}

#[test]
fn disabled_stream_skips_work_and_payload_batches_are_explicitly_rejected() {
    let (directory, system, factory) = setup(4, 4);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = connect(directory.path());
    system.set_stream_policy("off".parse().unwrap());
    let invalid = Update {
        metadata: vec![0; 100],
        data: b"oversized",
        ..Update::default()
    };
    publisher.publish(&invalid).unwrap();
    publisher.publish_batch(&[invalid]).unwrap();
    assert!(subscriber.try_recv().is_err());
    system.set_stream_policy("on".parse().unwrap());
    assert_matches!(
        publisher.publish_batch(&[Update::default()]),
        Err(PublishError::PayloadBatchUnsupported)
    );
    assert!(subscriber.try_recv().is_err());
    publisher
        .publish(&Update {
            data: b"real",
            ..Update::default()
        })
        .unwrap();
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"real"
    );
}

#[test]
fn independent_lanes_and_retired_producer_keep_held_payloads_valid() {
    let (directory, system, factory) = setup(2, 4);
    let mut first = factory.try_create_publisher().unwrap();
    let mut second = factory.try_create_publisher().unwrap();
    let mut subscriber = connect(directory.path());
    first
        .publish(&Update {
            data: b"aaaa",
            ..Update::default()
        })
        .unwrap();
    let held = subscriber.try_recv().unwrap();
    assert_matches!(
        first.publish(&Update {
            data: b"x",
            ..Update::default()
        }),
        Err(PublishError::PayloadCapacity)
    );
    second
        .publish(&Update {
            data: b"bbbb",
            ..Update::default()
        })
        .unwrap();
    drop(first);
    assert!(factory.try_create_publisher().is_none());
    drop(second);
    drop(factory);
    drop(system);
    assert_eq!(held.decode().unwrap().data, b"aaaa");
    drop(held);
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"bbbb"
    );
}

#[test]
fn dynamic_consumers_see_numeric_handles_and_large_payloads_roundtrip() {
    const SIZE: usize = 10 * 1024 * 1024;
    let (directory, _system, factory) = setup(2, SIZE as u64);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut typed = connect(directory.path());
    let mut dynamic = StreamExplorer::new(directory.path().into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_dynamic()
        .unwrap();
    let bytes = vec![42; SIZE];
    publisher
        .publish(&Update {
            data: &bytes,
            ..Update::default()
        })
        .unwrap();
    assert_eq!(typed.try_recv().unwrap().decode().unwrap().data, bytes);
    let held = dynamic.try_recv().unwrap();
    // Dynamic readers pin the cell/allocation without accessing payload bytes.
    assert_matches!(
        publisher.publish(&Update {
            data: b"x",
            ..Update::default()
        }),
        Err(PublishError::PayloadCapacity)
    );
    let DecodedMessage::Struct { fields } = held.decode().unwrap() else {
        panic!()
    };
    let values: Vec<_> = fields.map(|field| field.unwrap()).collect();
    assert_eq!(values[2].name(), "data_offset");
    assert_eq!(values[3].name(), "data_len");
    assert_matches!(values[2].value(), Value::U64(0));
    assert_matches!(values[3].value(), Value::U64(len) if *len == SIZE as u64);
}

#[test]
fn reclamation_without_consumers_and_late_join_do_not_expose_old_payloads() {
    let (directory, _system, factory) = setup(2, 4);
    let mut publisher = factory.try_create_publisher().unwrap();
    for value in 0..20 {
        publisher
            .publish(&Update {
                data: &[value; 4],
                ..Update::default()
            })
            .unwrap();
    }
    let mut subscriber = connect(directory.path());
    assert!(subscriber.try_recv().is_err());
    publisher
        .publish(&Update {
            data: b"late",
            ..Update::default()
        })
        .unwrap();
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"late"
    );
    drop(subscriber);
    publisher
        .publish(&Update {
            data: b"gone",
            ..Update::default()
        })
        .unwrap();
}

#[test]
fn serialization_panic_cancels_payload_and_cell_reservations() {
    use agave_event_system::wincode::{SchemaRead, SchemaWrite, WriteResult, io::Writer};
    struct PanicWriter;
    // SAFETY: successful writes serialize exactly one ordinary u64.
    unsafe impl<C: agave_event_system::wincode::config::ConfigCore> SchemaWrite<C> for PanicWriter {
        type Src = u64;
        fn size_of(_: &u64) -> WriteResult<usize> {
            Ok(8)
        }
        fn write(writer: impl Writer, src: &u64) -> WriteResult<()> {
            assert_ne!(*src, u64::MAX, "injected serialization panic");
            <u64 as SchemaWrite<C>>::write(writer, src)
        }
    }
    // SAFETY: delegates initialization and decoding to the u64 schema.
    unsafe impl<'de, C: agave_event_system::wincode::config::ConfigCore> SchemaRead<'de, C>
        for PanicWriter
    {
        type Dst = u64;
        fn read(
            reader: impl agave_event_system::wincode::io::Reader<'de>,
            dst: &mut std::mem::MaybeUninit<u64>,
        ) -> agave_event_system::wincode::ReadResult<()> {
            <u64 as SchemaRead<'de, C>>::read(reader, dst)
        }
    }
    #[event(max_serialized_size = 32)]
    struct PanicEvent<'a> {
        #[payload]
        data: &'a [u8],
        #[wincode(with = "PanicWriter")]
        value: u64,
    }
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let factory = system
        .create_stream_with_payloads::<PanicEvent>(
            stream_name!("panic"),
            StreamConfig {
                capacity: 1,
                publisher_slots: 1,
                subscriber_slots: 1,
            },
            4,
        )
        .unwrap();
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = StreamExplorer::new(directory.path().into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<PanicEvent>()
        .unwrap();
    let mut event = PanicEvent {
        data: b"fail",
        value: u64::MAX,
    };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publisher.publish(&event).unwrap();
        }))
        .is_err()
    );
    assert!(subscriber.try_recv().is_err());
    event.value = 1;
    event.data = b"good";
    publisher.publish(&event).unwrap();
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"good"
    );
}

#[test]
fn payload_opt_in_is_required_and_empty_payloads_are_supported() {
    #[event]
    struct Fixed {
        value: u64,
    }
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let config = StreamConfig {
        capacity: 2,
        publisher_slots: 1,
        subscriber_slots: 1,
    };
    assert!(
        system
            .create_stream_with_payloads::<Fixed>(stream_name!("bad"), config, 4)
            .is_err()
    );
    assert!(
        system
            .create_stream_with_payloads::<Update>(stream_name!("zero"), config, 0)
            .is_err()
    );
    let factory = system
        .create_stream::<Update>(stream_name!("plain"), config)
        .unwrap();
    let mut publisher = factory.try_create_publisher().unwrap();
    assert_matches!(
        publisher.publish(&Update {
            data: b"x",
            ..Update::default()
        }),
        Err(PublishError::PayloadNotEnabled)
    );
    let (directory, _system, factory) = setup(2, 4);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = connect(directory.path());
    publisher
        .publish(&Update {
            data: b"",
            ..Update::default()
        })
        .unwrap();
    let held = subscriber.try_recv().unwrap();
    assert_eq!(held.decode().unwrap().data, b"");
    publisher
        .publish(&Update {
            data: b"full",
            ..Update::default()
        })
        .unwrap();
    drop(held);
    assert_eq!(
        subscriber.try_recv().unwrap().decode().unwrap().data,
        b"full"
    );
}

#[test]
fn enum_markers_support_named_tuple_and_payload_free_variants() {
    #[event(max_serialized_size = 80)]
    #[wincode(tag_encoding = "u8")]
    #[derive(Debug)]
    enum Updates<'a> {
        #[cfg(any())]
        Disabled(#[payload] &'a [u8]),
        Named {
            label: String,
            #[payload]
            body: &'a [u8],
            slot: u64,
        },
        Tuple(u32, #[payload] &'a [u8], u8),
        Done,
    }
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let factory = system
        .create_stream_with_payloads::<Updates>(
            stream_name!("enum"),
            StreamConfig {
                capacity: 4,
                publisher_slots: 1,
                subscriber_slots: 1,
            },
            16,
        )
        .unwrap();
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = StreamExplorer::new(directory.path().into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<Updates>()
        .unwrap();
    for label in ["x", "a longer variable-sized prefix"] {
        publisher
            .publish(&Updates::Named {
                label: label.into(),
                body: b"named",
                slot: 7,
            })
            .unwrap();
        let held = subscriber.try_recv().unwrap();

        let Updates::Named {
            label: actual,
            body,
            slot,
        } = held.decode().unwrap()
        else {
            panic!()
        };
        assert_eq!(actual, label);
        assert_eq!(body, b"named");
        assert_eq!(slot, 7);
    }
    publisher.publish(&Updates::Tuple(12, b"tuple", 3)).unwrap();
    let held = subscriber.try_recv().unwrap();
    assert_matches!(held.decode().unwrap(), Updates::Tuple(12, b"tuple", 3));
    drop(held);
    publisher
        .publish_batch(&[Updates::Done, Updates::Done])
        .unwrap();
    for _ in 0..2 {
        assert_matches!(
            subscriber.try_recv().unwrap().decode().unwrap(),
            Updates::Done
        );
    }
}

#[test]
fn borrowed_input_is_copied_and_decoding_borrows_the_same_allocation() {
    let (directory, _system, factory) = setup(2, 4);
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = connect(directory.path());
    let mut bytes = b"data".to_vec();
    publisher
        .publish(&Update {
            data: &bytes,
            ..Update::default()
        })
        .unwrap();
    bytes.fill(0);
    drop(bytes);
    let held = subscriber.try_recv().unwrap();
    let first = held.decode().unwrap();
    let second = held.decode().unwrap();
    assert_eq!(first.data, b"data");
    assert_eq!(first.data.as_ptr(), second.data.as_ptr());
}

#[test]
fn tuple_struct_payload_marker_is_not_a_prefix() {
    #[event]
    struct Tuple<'a>(u32, #[payload] &'a [u8], u16);
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let factory = system
        .create_stream_with_payloads::<Tuple>(
            stream_name!("tuple"),
            StreamConfig {
                capacity: 1,
                publisher_slots: 1,
                subscriber_slots: 1,
            },
            4,
        )
        .unwrap();
    let mut publisher = factory.try_create_publisher().unwrap();
    let mut subscriber = StreamExplorer::new(directory.path().into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<Tuple>()
        .unwrap();
    publisher.publish(&Tuple(42, b"data", 7)).unwrap();
    let held = subscriber.try_recv().unwrap();
    let Tuple(id, handle, trailer) = held.decode().unwrap();
    assert_eq!((id, handle.len(), trailer), (42, 4, 7));
    assert_eq!(handle, b"data");
}

#[test]
fn reader_markers_cannot_redirect_resolution_to_unallocated_bytes() {
    mod producer {
        use super::*;
        #[event]
        pub struct Same<'a> {
            #[payload]
            pub left: &'a [u8],
            pub right_offset: u64,
            pub right_len: u64,
        }
    }
    mod reader {
        use super::*;
        #[event]
        pub struct Same<'a> {
            pub left_offset: u64,
            pub left_len: u64,
            #[payload]
            pub right: &'a [u8],
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let system = EventSystem::new(directory.path()).unwrap();
    system.set_stream_policy("on".parse().unwrap());
    let factory = system
        .create_stream_with_payloads::<producer::Same>(
            stream_name!("markers"),
            StreamConfig {
                capacity: 1,
                publisher_slots: 1,
                subscriber_slots: 1,
            },
            4,
        )
        .unwrap();
    let mut publisher = factory.try_create_publisher().unwrap();
    // Identical numeric wire schemas, but different marker locations. The
    // producer's marker determines the only allocation this cell may access.
    let mut subscriber = StreamExplorer::new(directory.path().into())
        .available_streams()
        .next()
        .unwrap()
        .try_connect_typed::<reader::Same>()
        .unwrap();
    publisher
        .publish(&producer::Same {
            left: b"safe",
            right_offset: u64::MAX,
            right_len: u64::MAX,
        })
        .unwrap();
    let held = subscriber.try_recv().unwrap();
    let event = held.decode().unwrap();
    assert_eq!(event.right, b"safe");
    assert_eq!((event.left_offset, event.left_len), (0, 4));
}

#[test]
fn generated_writer_emits_handles_in_wire_order_without_cloning_metadata() {
    use agave_event_system::{Event, wincode};

    #[event(max_serialized_size = 128)]
    #[wincode(tag_encoding = "u8")]
    enum Header<'a> {
        Named {
            prefix: String,
            #[payload]
            data: &'a [u8],
            trailer: u32,
        },
        Tuple(Vec<u8>, #[payload] &'a [u8], u16),
        Done {
            sequence: u64,
        },
    }

    for prefix in ["", "variable-length metadata before the handle"] {
        let event = Header::Named {
            prefix: prefix.into(),
            data: b"source",
            trailer: 42,
        };
        let mut encoded = [0; 128];
        event.write_event(&mut encoded, [123, 6]).unwrap();
        let expected = wincode::serialize(&(0u8, prefix.to_owned(), 123u64, 6u64, 42u32)).unwrap();
        assert_eq!(&encoded[..expected.len()], expected);
    }
    let event = Header::Tuple(vec![1, 2, 3], b"data", 7);
    let mut encoded = [0; 128];
    event.write_event(&mut encoded, [321, 4]).unwrap();
    let expected = wincode::serialize(&(1u8, vec![1u8, 2, 3], 321u64, 4u64, 7u16)).unwrap();
    assert_eq!(&encoded[..expected.len()], expected);

    let event = Header::Done { sequence: 9 };
    event.write_event(&mut encoded, [0, 0]).unwrap();
    let expected = wincode::serialize(&event).unwrap();
    assert_eq!(&encoded[..expected.len()], expected);
}
