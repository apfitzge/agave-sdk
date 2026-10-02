//! Continuous shared-payload example. Arguments: producers consumers max-bytes seconds [validate|metadata].
//! Defaults: 2 producers, 2 consumers, payloads up to 64 KiB, run until Ctrl-C.
#[cfg(target_os = "linux")]
mod linux {
    use {
        agave_event_system::{
            EventSystem, StreamConfig, event, publisher::PublishError, stream_name,
            subscriber::StreamExplorer,
        },
        std::{
            sync::{
                Barrier,
                atomic::{AtomicBool, AtomicU64, Ordering},
            },
            thread,
            time::{Duration, Instant},
        },
    };

    static RUNNING: AtomicBool = AtomicBool::new(true);

    extern "C" fn stop(_: libc::c_int) {
        RUNNING.store(false, Ordering::Relaxed);
    }

    // A validation panic must stop the remaining workers and reporter too.
    struct StopOnDrop;
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            RUNNING.store(false, Ordering::Relaxed);
        }
    }

    #[event]
    struct Update<'a> {
        producer: u64,
        sequence: u64,
        #[payload]
        data: &'a [u8],
    }

    #[derive(Default)]
    struct Counters {
        sent: AtomicU64,
        bytes: AtomicU64,
        queue_drops: AtomicU64,
        payload_drops: AtomicU64,
    }

    pub fn run() {
        let mut args = std::env::args().skip(1);
        let mut number = |default| {
            args.next().map_or(default, |s| {
                s.parse::<usize>().expect("expected an unsigned integer")
            })
        };
        let producers = number(2);
        let consumers = number(2);
        let max_bytes = number(65_536);
        let seconds = number(0);
        let mode = args.next().unwrap_or_else(|| "validate".into());
        let validate_payload = match mode.as_str() {
            "validate" => true,
            "metadata" => false,
            _ => panic!("consumer mode must be validate or metadata"),
        };
        assert!(
            args.next().is_none(),
            "usage: payloads [producers] [consumers] [max-bytes] [seconds] [validate|metadata]"
        );
        assert!(producers > 0, "at least one producer is required");
        assert!(
            (1..=10_485_760).contains(&max_bytes),
            "max-bytes must be 1..=10485760"
        );
        // SAFETY: the handler only stores to a lock-free atomic; it performs no
        // allocation, I/O, locking, or access to thread-local state.
        unsafe {
            assert_ne!(
                libc::signal(libc::SIGINT, stop as *const () as libc::sighandler_t),
                libc::SIG_ERR
            );
            assert_ne!(
                libc::signal(libc::SIGTERM, stop as *const () as libc::sighandler_t),
                libc::SIG_ERR
            );
        }
        let directory = tempfile::tempdir().unwrap();
        let system = EventSystem::new(directory.path()).unwrap();
        system.set_stream_policy("on".parse().unwrap());
        let factory = system
            .create_stream_with_payloads::<Update>(
                stream_name!("example.updates"),
                StreamConfig {
                    capacity: 256,
                    publisher_slots: producers,
                    subscriber_slots: consumers,
                },
                (max_bytes as u64).checked_mul(4).unwrap(),
            )
            .unwrap();
        let counts: Vec<_> = (0..producers).map(|_| Counters::default()).collect();
        let received: Vec<_> = (0..consumers).map(|_| AtomicU64::new(0)).collect();
        let ready = Barrier::new(producers.checked_add(consumers).unwrap());
        println!(
            "{producers} publishers, {consumers} broadcast consumers, payloads up to {max_bytes} \
             bytes, consumer mode={mode}"
        );
        println!(
            "Stream directory: {} (Ctrl-C to stop)",
            directory.path().display()
        );
        thread::scope(|scope| {
            for count in &received {
                let ready = &ready;
                let directory = directory.path();
                scope.spawn(move || {
                    let mut subscriber = StreamExplorer::new(directory.into())
                        .available_streams()
                        .next()
                        .unwrap()
                        .try_connect_typed::<Update>()
                        .unwrap();
                    let mut previous = vec![None; producers];
                    ready.wait();
                    let _stop = StopOnDrop;
                    while RUNNING.load(Ordering::Relaxed) {
                        let Ok(held) = subscriber.try_recv() else {
                            thread::yield_now();
                            continue;
                        };
                        let update = held.decode().unwrap();
                        let last = &mut previous[update.producer as usize];
                        assert!(last.is_none_or(|last| update.sequence > last));
                        *last = Some(update.sequence);
                        if validate_payload {
                            let expected = update.producer.wrapping_add(update.sequence) as u8;
                            assert!(
                                update.data.iter().all(|&byte| byte == expected),
                                "payload changed while its cell was held"
                            );
                        }
                        count.fetch_add(1, Ordering::Relaxed);
                        // The borrowed update and its cell are released together.
                    }
                });
            }
            for (producer, count) in counts.iter().enumerate() {
                let factory = factory.clone();
                let ready = &ready;
                scope.spawn(move || {
                    // Publishers are thread-bound: create each on its own thread.
                    let mut publisher = factory.try_create_publisher().unwrap();
                    let sizes: Vec<_> = [1, 64, 1_232, 4_096, 65_536, 1_048_576, 10_485_760]
                        .into_iter()
                        .filter(|&size| size < max_bytes)
                        .chain(std::iter::once(max_bytes))
                        .collect();
                    let mut bytes = vec![0; max_bytes];
                    let mut sequence = 0u64;
                    ready.wait();
                    let _stop = StopOnDrop;
                    for size in sizes.iter().cycle() {
                        if !RUNNING.load(Ordering::Relaxed) {
                            break;
                        }
                        let data = &mut bytes[..*size];
                        data.fill((producer as u64).wrapping_add(sequence) as u8);
                        match publisher.publish(&Update {
                            producer: producer as u64,
                            sequence,
                            data,
                        }) {
                            Ok(()) => {
                                count.sent.fetch_add(1, Ordering::Relaxed);
                                count.bytes.fetch_add(*size as u64, Ordering::Relaxed);
                            }
                            Err(PublishError::FailedToSend) => {
                                count.queue_drops.fetch_add(1, Ordering::Relaxed);
                                thread::yield_now();
                            }
                            Err(PublishError::PayloadCapacity) => {
                                count.payload_drops.fetch_add(1, Ordering::Relaxed);
                                thread::yield_now();
                            }
                            Err(error) => panic!("publish failed: {error}"),
                        }
                        sequence = sequence.checked_add(1).unwrap();
                    }
                });
            }
            let start = Instant::now();
            let mut report = start;
            while RUNNING.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                let elapsed = report.elapsed().as_secs_f64();
                report = Instant::now();
                let mut sent = 0u64;
                let mut bytes = 0u64;
                let mut queue_drops = 0u64;
                let mut payload_drops = 0u64;
                for count in &counts {
                    sent = sent.wrapping_add(count.sent.swap(0, Ordering::Relaxed));
                    bytes = bytes.wrapping_add(count.bytes.swap(0, Ordering::Relaxed));
                    queue_drops =
                        queue_drops.wrapping_add(count.queue_drops.swap(0, Ordering::Relaxed));
                    payload_drops =
                        payload_drops.wrapping_add(count.payload_drops.swap(0, Ordering::Relaxed));
                }
                let rates: Vec<_> = received
                    .iter()
                    .map(|count| {
                        format!("{:.0}", count.swap(0, Ordering::Relaxed) as f64 / elapsed)
                    })
                    .collect();
                println!(
                    "sent {:.0}/s, {:.1} MiB/s, consumer events/s [{}], drops queue={queue_drops} \
                     payload={payload_drops}",
                    sent as f64 / elapsed,
                    bytes as f64 / elapsed / 1_048_576.0,
                    rates.join(", ")
                );
                if seconds != 0 && start.elapsed().as_secs() >= seconds as u64 {
                    RUNNING.store(false, Ordering::Relaxed);
                }
            }
        });
    }
}

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}
#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("This shared-memory example requires Linux.");
}
