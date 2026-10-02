# Shared-memory payload allocator PoC

## Scope

Prove a separate FIFO payload allocator while preserving fixed-size broadcast
cells and ordinary shaq consumer attachment/drop. No process monitor, consumer
registry, crash recovery, coordinator thread, or registration notifications.
A crashed consumer may pin cells and payloads indefinitely; this is accepted for
the PoC. Live slow consumers also retain their allocations until they advance.

## Completed prerequisites

- Local shaq 4.4.0 branch at
  `/home/apfitzge/dev/shaq/worktrees/variable-sized-payload-broadcast`, including
  bounded queue regions, prepared writes, and reclamation watermarks.
  Leave shaq unchanged. See [the historical handoff](shaq-payload-prerequisites.md).
- `backend/linux/stream_layout.rs`: version-2 stream header with the relocated
  broadcast queue and optional per-producer payload regions in the same sealed
  file. Descriptors record offset, usable capacity, page-aligned stride, and lane
  count. Disabled payload storage adds no bytes. Version 1 is rejected.
- Internal `create_sealed_queue_with_payloads` sizes those regions. Existing
  stream creation still requests zero payload capacity; public opt-in and mapping
  will be added with payload publication. Discovery checks the advertised payload
  lane count against the queue.
- `backend/linux/publisher.rs`: reserve before serialization and commit only
  successfully serialized events. Disabled streams return before reservation.

The standalone offset allocator is implemented in
`backend/linux/payload_ring.rs`: contiguous byte ranges, wrap-padding accounting,
cancellable reservations, checked logical positions, and explicit FIFO-prefix
reclamation. Its state is entirely producer-local; it does not map or copy bytes.
`backend/linux/payload_storage.rs` now owns writable producer or read-only
consumer mappings of the payload region. It checks file seals and lane bounds,
excludes page padding, and provides internal unsafe copy/borrow operations whose
caller must prove allocation ownership and lifetime. Separate-mapping tests cover
10 MiB payloads, lane isolation, descriptor lifetime, invalid handles, and disabled
storage. Broadcast bookkeeping and safe typed access remain to be integrated. Seven unit tests cover the allocator's boundary and failure cases.

## Allocator

Add one contiguous circular byte region per producer lane. Keep checked logical
head/tail positions and `(broadcast sequence, allocation end)` records local to
its producer. Shared structures contain offsets and lengths, never pointers.

Allocations should support 1 byte through 10 MiB subject to configured capacity.
Define a zero-length handle explicitly. Avoid straddling the physical end and
charge wrap padding against capacity. Reject oversize requests, exhausted
capacity, and arithmetic overflow without blocking. Roll back unpublished
reservations on failure/panic. One mutable publisher allows one reservation.

Reclaim records strictly below that lane's synchronized shaq reclamation
watermark. Consumers publish no allocator-specific progress. For the PoC,
prevent reuse of payload-enabled producer lanes during a stream's lifetime,
rather than resetting allocator storage while old cells may remain accessible.

## Layout and publication

Extend StreamLayout with immutable payload-region descriptors, checked bounds,
and a new format version. Keep queue and per-producer payload rings in the same
sealed file. Allocate no payload region when payload support is disabled.

The opt-in single-event publication path should:

1. Check the stream is enabled.
2. Reserve a broadcast cell before copying any payload.
3. Reclaim eligible allocations, reserve bytes, and copy the payload.
4. Serialize fixed metadata containing numeric `(offset, len)` fields.
5. Commit and retain the allocation record, with no fallible bookkeeping after
   publication. Roll back on earlier failure.

Existing fixed-size publication and batch APIs stay unchanged. Untyped consumers
inspect the handle as ordinary numeric schema fields. Typed payload access belongs
on StreamMessage and borrows from its held cell guard; an owned decoded event
must not grant payload access. Validate handles against the appropriate lane.

## Validation

Test exact fit, exhaustion, wrap padding, oversize requests, arithmetic overflow,
and rollback. Integration tests should cover held guards, multiple consumers,
independent producers, late joins, graceful release, serialization failure, and
disabled streams avoiding copies. Add a compile-fail test for payload borrows
outliving their message guard. Benchmark fixed-size events and payloads through
10 MiB after the complete publication/reclamation path works.
