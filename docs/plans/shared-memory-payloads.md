# Shared-memory payload allocator PoC

## Scope

Prove a separate FIFO payload allocator while preserving fixed-size broadcast
cells and ordinary shaq consumer attachment/drop. No process monitor, consumer
registry, crash recovery, coordinator thread, or registration notifications.
A crashed consumer may pin cells and payloads indefinitely; this is accepted for
the PoC. Live slow consumers retain their allocations until they advance.

## Implemented path

- Local shaq 4.4.0 branch at
  `/home/apfitzge/dev/shaq/worktrees/variable-sized-payload-broadcast`, including
  bounded queue regions, prepared writes, and synchronized reclamation watermarks.
  Shaq is unchanged. See [the historical handoff](shaq-payload-prerequisites.md).
- `backend/linux/stream_layout.rs`: version-5 stream header, relocated broadcast
  queue, and optional per-producer payload regions in the same sealed file.
  Descriptors record offset, usable capacity, page-aligned stride, and lane count.
  Disabled payload storage adds no bytes. Earlier formats are rejected; version 5 stores markers for adjacent numeric
  offset/length fields alongside the ordinary event schema.
- `backend/linux/payload_ring.rs`: producer-local logical positions, contiguous
  allocation, wrap-padding accounting, cancellable reservations, and explicit
  FIFO-prefix reclamation. Zero-length handles are `(0, 0)`; oversized, full, or
  exhausted rings fail without blocking. Empty rings can skip wrap padding.
- `backend/linux/payload_storage.rs`: owned writable producer/read-only consumer
  mappings. Access checks lane bounds and excludes page padding. Mapping lifetime
  is independent of the file descriptor; payload lifetime is still tied to cells.
- `backend/linux/publisher.rs`: per-producer FIFO records of `(event sequence,
  allocation end)`, preallocated to queue capacity. Records strictly below that
  lane's synchronized broadcast watermark are reclaimed. Consumers publish no
  allocator-specific progress and allocations carry no reference counts.

Shaq already permanently retires producer lanes on drop, so no additional lane
reuse mechanism is needed. A dropped producer cannot overwrite old payloads;
consumer mappings keep those bytes alive while held cells can still access them.

## Public API and wire convention

Use a borrowed byte slice as both the input and output event field:

```rust
#[event]
struct Update<'a> {
    slot: u64,
    #[payload]
    data: &'a [u8],
}
```

`publish(&Update { slot, data: &bytes })` copies the bytes into the producing
lane's payload ring. The caller can reuse or drop `bytes` immediately afterward.
`held.decode()` returns `Update<'_>` with `data` borrowed from the held broadcast
cell. Dropping the cell while using the slice is a compile-time error.

The macro generates an internal wire type with ordinary `data_offset: u64` and
`data_len: u64` fields in place of `data`. Offsets are lane-relative. Dynamic
consumers see only those numeric fields: they have no payload accessor or raw
handle-resolution API. The public PayloadHandle and publish_with_payload APIs
are removed. Fixed-size events retain their existing representations and derives.
Payload events derive header serialization, with an adapter writing placeholder
integers, and use guard-aware decoding instead of ordinary SchemaRead.

Structs, tuple structs, and named/tuple enum variants remain supported, with at
most one payload per struct or variant. Tuple payloads expand into two numeric
positions. Non-payload metadata remains owned; type/const generic events are not
supported by the macro. Event lifetime parameters describe payload borrows.
Generated field-name collisions are compile-time errors.

`Event::View<'a>` represents the event with a fresh payload lifetime: fixed-size
implementations use Self, and payload implementations substitute their lifetime
parameters. Publication accepts this view and decoding returns it. This permits
`Publisher<Update<'static>>` to publish temporary input slices without requiring
those slices to be static. Stream creation may need an explicit event type where
it was previously inferred solely from a subsequent publication. Generic callers
that pass E directly can use `for<'a> Event<View<'a> = E>` for fixed-size events.

Generated marker metadata identifies the offset field by variant and name.
The producer persists that metadata with the schema. Both sides locate the two
integers through the schema decoder, including dynamic metadata prefixes and
configured enum tags. Resolution always follows the producer's markers and the
held cell's producer lane; a reader cannot redirect it using its own markers.

`EventSystem::create_stream_with_payloads` adds a nonzero byte capacity per
producer, without changing StreamConfig. Normal `publish` checks enablement,
prepares a broadcast cell, reclaims eligible allocations, reserves bytes,
serializes metadata, fills the handle, copies the source slice, records ownership,
and publishes. Serialization errors/panics cancel both reservations. No fallible
work or bookkeeping allocation follows the allocator commit. Disabled streams
return before allocation, copying, or serialization.

Payload publication remains single-event for this PoC. Batches containing a
payload-bearing variant return PayloadBatchUnsupported before publishing anything;
fixed-size and payload-free variant batches retain their existing behavior.

## Validation and remaining work

Unit tests cover exact fit, exhaustion, wrap padding, oversize requests, checked
arithmetic, cancellation, mapping visibility, lane isolation, and 10 MiB payloads.
Integration tests cover held guards across multiple consumers, independent
producers, retired lanes, late joins, graceful release, queue/payload pressure,
serialization errors and panics, disabled streams, zero-length payloads, dynamic
numeric metadata, source-buffer independence, repeated zero-copy decoding, and
rejection of unsupported payload batches. Compile-fail docs
check payload-borrow lifetime and invalid marker declarations.

The allocator PoC is connected end to end. The continuous example below exercises
multiple publishers and consumers with variable-sized payloads. Account-db integration and a general-purpose
allocator remain separate projects. Crash recovery remains out of scope.

Marker regression coverage includes dynamic handle positions, named and tuple
enum variants, payload-free variants, custom enum tag encoding, and mismatched
producer/consumer marker declarations.


## Continuous multithreaded example

```sh
cargo run --release -p agave-event-system --example payloads
# Optional positional arguments: producers consumers max-payload-bytes seconds [validate|metadata]
cargo run --release -p agave-event-system --example payloads -- 4 2 10485760
# Two publishers, no consumers:
cargo run --release -p agave-event-system --example payloads -- 2 0
# Two publishers and two consumers that decode but do not scan payload bytes:
cargo run --release -p agave-event-system --example payloads -- 2 2 65536 0 metadata
```

Defaults are two publisher threads, two typed consumer threads, payloads up to
64 KiB, and continuous execution. Stop with Ctrl-C, or supply a nonzero duration
in seconds. Consumer mode defaults to `validate`. The `metadata` mode still
uses typed decoding (including handle resolution) and checks sequence ordering,
but never reads the borrowed payload bytes. Source filling, publication, counters,
and yielding behavior are identical in both modes. The temporary stream directory
is printed and removed on shutdown.

Each publisher has its own lane and payload ring, cycles through payload sizes
from one byte to the configured maximum (up to 10 MiB), and publishes borrowed
slices. All consumers attach before publication starts. Each independently reads
the broadcast, checks increasing per-producer sequence numbers (gaps are allowed
for drops), and verifies every payload byte while holding its cell. Publishers
continue after queue or payload exhaustion, counting the dropped events.

Once per second the example reports successful publications, payload MiB/s,
per-consumer event rates, and separate queue/payload drop counts. These are live
activity counters, not benchmark results; source filling, full payload validation,
thread scheduling, and reporting all contribute to the workload.

A local release-build comparison with 2 publishers, 2 consumers, and a 64 KiB
maximum used three five-second runs per mode, excluding each run's first report:

| Consumer mode | Published events/s | Published MiB/s | Queue drops/s | Payload drops/s |
| --- | ---: | ---: | ---: | ---: |
| Full validation | 2.22 million | 2,111 | 422,461 | 819,695 |
| Metadata only | 2.81 million | 38,033 | 0 | 85 |

Only the payload scan was disabled. The successful event mix also changed:
average published payload size rose from about 1,000 to 14,186 bytes. This is
consistent with larger allocations being dropped disproportionately under
validation-induced backpressure; the 18x byte-rate increase is not an 18x
increase in event rate. These short local runs are workload observations, not
portable performance guarantees.
