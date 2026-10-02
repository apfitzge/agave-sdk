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

The allocator PoC is connected end to end. Next work is measurement: compare
fixed-size events and payload sizes through 10 MiB, and exercise representative
account-update/transaction metadata. Account-db integration and a general-purpose
allocator remain separate projects. Crash recovery remains out of scope.

Marker regression coverage includes dynamic handle positions, named and tuple
enum variants, payload-free variants, custom enum tag encoding, and mismatched
producer/consumer marker declarations.
