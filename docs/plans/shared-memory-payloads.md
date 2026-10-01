# Variable-sized shared-memory payload support

Status: implementation plan; no implementation performed.
Inspection baseline: agave-event-system in this worktree, locked shaq 4.4.0,
and wincode-dynamic 0.3.0. Recheck the target shaq checkout before implementation.

The first upstream work is specified in [the shaq handoff](shaq-payload-prerequisites.md).

## Local prerequisite branch status

The prerequisites are implemented in the local checkout
`/home/apfitzge/dev/shaq/worktrees/variable-sized-payload-broadcast` (note
`worktrees`, plural), branch `variable-sized-payload-broadcast`:

- `d89de6f`: bounded file regions (`BroadcastConfig::layout`, `create_at`,
  `join_at`, `join_untyped_at`).
- `cc26222`: cancellable prepared writes and explicit/prefix commit.
- `7ec9fdf`: synchronized reclamation watermarks and prepared sequence identity.

Source inspection confirmed these APIs and accompanying test modules are present;
tests were not rerun as part of this status update. This is not a full correctness
review of the prerequisite implementation.

Before wiring this checkout into event-system, resolve its baseline mismatch:
it declares package version `3.0.0`, while this workspace requires `4.4.0`, and
does not contain the `ProducerId`, `LaneMetadata`, queue-identifier, or lane
metadata APIs used here. A plain Cargo patch is therefore insufficient. Prefer
porting/rebasing the prerequisite commits onto the compatible 4.4.0 API baseline,
preserving identifiers and publisher metadata, then testing integration. The
event-system dependency has not been changed.

## Goal and constraints

Preserve fixed-size broadcast cells. Put variable-sized transaction/account data
in an optional shared-memory byte arena per producer lane, and publish numeric
offset/length fields in the existing event encoding.

- Existing fixed-size events should require no source changes.
- Metadata-only consumers must not need to touch payload bytes.
- Dynamic consumers inspect handles as ordinary numeric fields.
- Typed consumers borrow payload bytes only while holding the corresponding
  broadcast event guard.
- Publication and allocation are nonblocking. Drop an event on either capacity
  failure; avoid copying if broadcast capacity is already unavailable.
- Allocate/copy payloads only when the stream is enabled.
- Live slow consumers retain their cells and payloads. Confirmed-dead processes
  must eventually stop constraining reclamation.
- Consumers join through shared files; no required daemon/control socket.
- Do not integrate accounts-db storage or build a general-purpose allocator yet.

## Findings from the existing code

### Event-system

- `event-system/src/backend/linux.rs`: `create_sealed_queue` creates a sealed
  memfd containing only a shaq queue. A staging directory atomically publishes
  the schema file and a `queue-<identifier>` symlink to the producer's FD.
- `backend/linux/subscriber.rs::open_queue` checks seals and the queue identifier.
  Directory-FD-relative lookup prevents pairing a replacement stream's queue
  with the previous stream's schema.
- `backend/linux/publisher.rs::{publish,publish_batch}` checks the stream rule
  before reserving. Both serialize directly into reserved cells. On serialization
  error, guard drop still publishes incomplete bytes; a batch publishes its
  entire reservation, including any unprocessed suffix.
- Public `Publisher` is `!Send + !Sync`; publication uses `&mut self`.
- `subscriber.rs::StreamMessage` retains a backend `SliceReadGuard`. Typed
  `decode()` returns an owned header. Dynamic decoding reflects schema fields.
- `lib.rs::Event` is `'static`; `event-system-derive/src/lib.rs` rejects generic
  event types. The macro generates wincode read/write and dynamic schema derives,
  with a byte-array `QueueCell`.
- Bounded dynamic data already works inline through
  `#[event(max_serialized_size = ...)]`; retain that capability.
- wincode-dynamic 0.3.0 describes primitives, strings, primitive vectors, and
  primitive arrays, not arbitrary nested handle structs.
- `StreamConfig` is publicly constructed with struct literals. Adding a required
  field would break callers. Non-Linux public APIs have no-op implementations in
  `backend/stub.rs`.

### shaq

- `src/broadcast.rs::{Broadcast,SharedQueue,QueueLayout}` and `src/shmem.rs::Region`
  assume the queue starts at mapping/file offset zero. Creation resizes the file
  to the queue size. Internal layout offsets are already relative to a base.
- Each `Producer` exclusively owns one `ProducerLane`. Dropping it permanently
  retires the lane; lane indexes are a lifetime budget, not recycled slots.
- Consumers scan lanes round-robin but read each lane in publication order.
- Read-guard drop advances that consumer's cursor on the relevant lane.
- `LaneConsumerState` stores reserve limits (`next_to_read + capacity`), using
  `usize::MAX` as the unclaimed sentinel. Producers Acquire-load limits;
  consumers Release-store progress.
- Joining samples the reservation frontier twice, with a SeqCst fence and
  provisional limit between samples. Producer reservation has the other half
  of this handshake. New consumers skip already-reserved events.
- `WriteGuard` and `WriteBatch` publish unconditionally on drop.
- Consumer ownership has only free/joining/active states, no PID or generation.
- `recover_consumer`, `recover_slice_consumer`, and `force_release` exist, but
  require proof of death and external serialization with joins/drops/recovery.
- Sequence comparisons explicitly do not support counter wraparound.

## 1. Queue region and enclosing file format

### Changes

In shaq, expose checked queue size/alignment and add creation/join APIs accepting
a queue offset and extent within an already-sized file. Region creation must not
resize the enclosing file. Preserve zero-offset APIs as wrappers.

Retain a mapping owner and a bounded queue view. Update
`SharedQueue::{initialize,from_region,join_region_with}` to use the view base and
extent. Validate minimum header size before creating a reference, and validate
the queue layout against its own region rather than the entire file. A full-file
mapping plus a checked view supports aligned offsets without requiring mmap
offsets to be page-aligned.

In event-system, add an outer superblock and layout:

1. Superblock and immutable region descriptors.
2. Consumer registry.
3. Broadcast queue.
4. One payload arena per producer lane.

Superblock fields: magic/version/header length/total length, stream incarnation,
queue offset/length/format, registry offset/length/count, payload capability and
format, per-lane arena offsets/capacities, and payload-handle field semantics.

Use fixed-width offsets/lengths and checked arithmetic. Reject overlap,
misalignment, inconsistent counts, unsupported formats, and out-of-file ranges.
Page-align major regions and isolate registry entries/cache-hot atomics. Keep
producer allocator cursors local, away from shared consumer-progress lines.

### Ordering and compatibility

Initialize everything, publish readiness with Release, apply resize seals, then
publish the stream directory. Joiners Acquire-load readiness. Retain separate
schema files, directory-FD lookup, and identifier checks.

This is a new event-system file format. Old clients must reject it. Relocation
alone need not change shaq's internal format, but managed ownership may require
a version change. Fixed-width outer descriptors do not make shaq's native-usize
layout architecture-independent. Prefer explicit legacy rejection initially.

### Verification

Test nonzero offsets with typed/untyped clients, prefix/suffix sentinels, distinct
mapping addresses, malformed/truncated/overlapping layouts, seals, and identifier
reuse. Benchmark fixed-event throughput to verify no new per-event layout work.

## 2. Consumer registry and Linux monitoring

### Ownership and lifecycle

Add `backend/linux/consumer_registry.rs` and `consumer_monitor.rs`. A producer
process monitor service scans per-stream registries and watches pidfds with
epoll. Streams/publishers keep the service alive even after the public EventSystem
handle is dropped. Avoid monitor/StreamGuard reference cycles.

Map registry slots one-to-one to queue consumer indexes. Do not register a PID
only after `slice_consumer()` returns: death during its join already leaves
constraints behind.

Proposed managed protocol:

`Free -> Requested -> Challenge -> Acknowledged -> Joining -> Active`

Graceful disconnect transfers `Active -> Leaving`; terminal cleanup uses
`Reaping -> Free(next generation)`.

- Atomically claim with PID, generation, and state; never claim anonymously and
  fill identity later with ordinary stores.
- A packed AtomicU64 can carry a positive Linux PID, bounded generation, and
  eight states. Retire before generation overflow; never permit ABA.
- Consumer transitions use generation-checked CAS.
- Monitor owns queue admission and terminal cleanup.
- Before acknowledgment there are no lane cursors or payload access rights.
- Monitor initializes the selected index's lane cursors with the existing join
  handshake. Consumer attaches to those cursors after admission and becomes the
  sole progress writer.
- Graceful disconnect transfers ownership only after guards are gone.
- Confirmed death permits clearing all lane limits. Only then publish the next
  free generation.

Add audited managed admission/attachment/release APIs in shaq. Do not casually
wrap today's unsafe index-only `force_release` with an external generation check:
its current concurrency contract is insufficient.

### PID reuse protocol

A shared PID followed by `pidfd_open` can identify a replacement process.
Use acknowledgment after opening the pidfd:

1. Consumer atomically publishes Requested with its identity/generation.
2. Monitor opens the pidfd.
3. Monitor changes that exact registration to Challenge.
4. Original consumer CAS-acknowledges that exact generation/state.
5. Monitor admits it only after acknowledgment.

The acknowledgment proves the original process was alive after the pidfd was
opened, so its PID could not have been reused at open time. Death afterward is
observable through the already-correct pidfd.

Unacknowledged requests may be cancelled as retryable connection attempts; they
do not participate in reclamation. Cancellation and acknowledgment race through
the same CAS word. If acknowledgment wins, retain the verified pidfd and follow
the admitted path. Never time out/revoke an acknowledged or active live consumer.

Do not rely solely on PID plus `/proc/PID/stat` starttime, which is reported in
clock ticks rather than as a guaranteed unique incarnation ID. Use process-wide
pidfds, without PIDFD_THREAD, so another thread's exit cannot revoke a live borrow.

Monitor event tokens include stream incarnation, slot, generation, and a local
watch identity; stale queued events must not act on reused slots or FDs.

### Ordering and failures

Use Acquire/Release registration transitions. Initialize lane cursors before
Release-publishing admission; attachment observes it with Acquire. Preserve the
SeqCst join handshake and existing Release/Acquire progress ordering. Clear lane
limits before publishing reuse.

- ESRCH during pending admission: reject the attempt.
- FD/resource/permission failures: retry or report admission failure, not death.
- Process SIGKILL: remove constraints after confirmed process exit.
- Live slow/stopped process, leaked guard, or abandoned live subscriber: retain
  constraints; no leases.
- Monitor failure: fail closed.
- Producer-process death: no allocator recovery/reuse; existing mappings can
  remain readable.
- Initially prohibit using inherited endpoints/guards across fork; require fresh
  registration. Require graceful disconnect before exec to avoid abandoned
  registrations belonging to a still-live PID.

Connection setup now needs monitor participation. Decide between bounded setup
waiting with explicit errors and a pending-connection API. Publication remains
nonblocking regardless.

### Verification

Kill child processes at each registration/join/drop stage and while holding a
cell. Inject PID reuse and stale epoll events. Test slot reuse, multiple
subscriptions per process, leader-thread exit, stopped live consumers, monitor
resource failures, and concurrent joins/publishing/reaping. Model atomic races.

## 3. FIFO payload allocator

Add `backend/linux/payload_ring.rs`; keep its arithmetic/state machine separately
testable without mappings. Bind arenas to `Producer::index()`, not thread ID.

Initial scope: one contiguous allocation per event, bytes only, up to 10 MiB
subject to configured capacity. Pack multiple components into one blob with
checked subranges. No per-allocation shared headers, refcounts, or free lists.

Shared metadata: arena offset/capacity/alignment/format, bytes, event handle, and
optional diagnostics. Local metadata: logical head/tail, bounded allocation
records, pending transaction, conservative reclamation cache.

Lanes are never reused, so allocator state need not survive publisher retirement.
The mapping keeps final published payloads alive; no new producer reallocates
that arena.

### Arithmetic

Let H be next allocation position, T reclaimed position, C capacity, N length.
Maintain `0 <= H - T <= C`.

Compute alignment padding, physical position modulo C, and any padding needed to
wrap so the payload does not straddle the end. Include all padding in the logical
allocation end. Accept only when `allocation_end - T <= C` using checked math.

- C must be a multiple of alignment. Byte alignment suffices for borrowed bytes.
- When empty, normalize head and tail together across end padding so a size-C
  allocation can succeed.
- Use a canonical zero-length handle without allocation.
- Non-straddling fragmentation may cause failure despite sufficient total free
  bytes.
- Reject oversize requests and counter exhaustion; do not silently wrap.
- No in-band wrap markers are required: local logical endpoints account for it.

Preallocate allocation records bounded by broadcast capacity. Each successful
payload event records `(event_sequence, allocation_end)` before publication.
Pending allocation rolls back on failure, including padding. Exclusive producer
borrowing prevents later allocations from making rollback ambiguous. Published
allocations are reclaimed only through broadcast progress.

Reserve arena address space at stream creation; sealed files cannot grow later.
Individual allocation/copy occurs only for enabled publications. Decide prefault
policy separately from logical allocation policy.

### Verification

Property-test interval ownership, wrap/padding, fragmentation, empty
normalization, rollback, exact-fit/full/empty states, zero/one/10-MiB lengths,
alignment, and counter exhaustion. Benchmark allocation separately from copying,
mixed sizes, padding overhead, per-producer memory, page faults, and NUMA effects.

## 4. Integration and typed access

### Transactional broadcast preparation

Add a new prepared-write API in shaq rather than changing old publish-on-drop
guards silently. It exclusively borrows the producer, checks capacity with the
join handshake, and leaves shared frontiers unchanged during preparation.
Explicit commit advances reservation then publication; drop cancels.

Do not roll back an already-visible reservation cursor: a joining consumer may
have adopted that frontier and skip a later event reusing a cancelled sequence.

Capacity remains available after preparation because there is one lane writer,
existing consumers only advance/release, and new consumers join the unchanged
frontier and permit a full ring of future writes. Prove this with concurrency
tests, including coexistence with legacy APIs. Address forgotten legacy guards
or any other case where reservation and publication differ before preparation.

For batches, support a validated initialized-prefix commit or explicitly choose
another failure policy. Preserve the documented successful-prefix behavior where
practical. Never publish incomplete cells or unprocessed suffixes.

### Broadcast-driven reclamation

Expose prepared sequence identity and a synchronized producer operation such as
`reclaimable_before()`. For lane capacity B, turn claimed reserve limits back
into read cursors, ignore unclaimed sentinels, and clamp to publication:

`R = min(publication_frontier, all participating next_to_read cursors)`

No consumers means R equals publication. Sequences strictly below R are safe.
Include provisional join limits and preserve the handshake fencing. This must
be a shaq API with a concurrency contract, not an external scan of private state.

Remove local records with `sequence < R` and advance allocator tail to their
allocation endpoints. Do this on demand during publication attempts. Waiting
for physical broadcast-cell overwrite is insufficient: payload capacity can
exhaust first. Consumers publish no allocator-specific progress.

Add explicit queue-counter exhaustion handling before unsupported wrap/sentinel
collision. No global cross-lane reclamation sequence is valid.

### Publish transaction

1. Check stream policy.
2. Prepare broadcast capacity.
3. Refresh reclamation and release eligible payload records.
4. Validate length and reserve payload storage.
5. Copy/write payload.
6. Serialize header with the library-generated handle.
7. Install local allocation record.
8. Commit broadcast publication; no fallible work remains.

Pre-commit failures cancel both transactions. Keep syscalls, registry scans,
blocking locks, and heap growth off the producer path. Add explicit errors or
counters for queue full, arena full, oversize, and serialization failure.

The existing relaxed stream policy load gates work; it is not payload memory
synchronization. A racing disable may allow an already-started event to finish.
Disabled/no-op paths must not execute payload writer closures.

### Schema and API

Use flat `u64` fields `payload_offset` and `payload_len`, with offsets relative to
the complete mapping. Dynamic consumers retain ordinary numeric reflection.

Extend `event-system-derive/src/lib.rs` with an opt-in attribute naming those
fields. Generate a trusted payload-event contract for setting/extracting them.
Start with fixed-header structs; decide enum support explicitly. Retain normal
wincode encoding and existing fixed event macro behavior.

Suggested public shapes:

- `publish_with_payload(event, bytes)` takes ownership of the small header so
  the library can replace handles before normal serialization.
- A lazy length/writer variant runs only after both capacities are secured.
- `StreamMessage<Typed<E>>::payload_bytes(&self)` returns a slice tied to this
  message borrow. Existing owned-header `decode()` remains.

Reject ordinary publishing of declared payload events without allocation.
Caller-supplied numeric handles cannot become trusted references. Validate
payload semantics/capability metadata at typed attachment: ordinary schema
equality alone does not establish handle ownership semantics.

The accessor decodes its own held event's handle and validates conversions,
overflow, lane bounds, and non-straddling extent. Do not offer safe arbitrary
`resolve(copied_handle)`: stale handles may be in bounds while being overwritten.
Any extensible trusted-extraction trait needs sealing or an unsafe contract.

Payload/header writes precede broadcast Release publication; the consumer's
Acquire makes both visible. Guard-drop Release progress followed by producer
Acquire reclamation permits overwrite. No separate payload-ready atomics.

Prefer a new creation method/options object for `PayloadConfig` rather than
breaking existing `StreamConfig` literals. Keep old fixed stream creation and
event encodings. Mirror API behavior in `backend/stub.rs`.

### Verification

Test typed borrowing, dynamic handle reflection, metadata-only access,
compile-fail lifetime constraints, multiple consumers/lanes, late joins during
preparation/copy/commit/reclamation, independent capacity failures, no copy when
disabled/full, error/panic rollback, malformed/stale/foreign handles, publisher
retirement, and crash recovery followed by byte reuse. Benchmark fixed-event
regressions, metadata-only latency, large/mixed payloads, and slow consumers.

## Delivery order and unresolved decisions

1. shaq region APIs and format validation.
2. shaq prepared publication and reclamation watermark, with concurrency tests.
3. Managed queue consumer ownership and event-system registry/monitor.
4. FIFO allocator with property tests.
5. Payload event APIs and integration.
6. End-to-end and performance validation.

Decisions to settle before the relevant implementation:

- Kernel baseline and PID namespace support; initially require compatible PID
  namespaces.
- Connection waiting versus pending connection API.
- Macro syntax and payload-bearing enum variants in the first release.
- Arena defaults, maximum length, alignment, and prefaulting policy.
- Exact batch failure semantics.
- Whether a legacy discovery path is worth maintaining.

Keep the division of responsibility: broadcast orders cells and proves sequence
reclamation; the integration layer associates sequences with allocation ends;
the FIFO allocator manages bytes. No payload awareness is needed inside shaq.

## Linux references

- https://man7.org/linux/man-pages/man2/pidfd_open.2.html
- https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html
- https://docs.kernel.org/filesystems/proc.html
