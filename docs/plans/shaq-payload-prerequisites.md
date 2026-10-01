# Agent handoff: initial shaq prerequisites for shared-memory payloads

## Implementation status

The three changes below are present in
`/home/apfitzge/dev/shaq/worktrees/variable-sized-payload-broadcast`, at commits
`d89de6f`, `cc26222`, and `7ec9fdf`, respectively. Do not reimplement them blindly.

Integration prerequisite: this checkout declares shaq `3.0.0` and lacks the
4.4.0 queue-identifier and producer/lane-metadata APIs required by event-system.
Port/rebase these changes onto the compatible baseline before using it as the
event-system dependency. Their presence was source-inspected, not fully reviewed
or retested in this event-system session. The original implementation brief
follows for reference and validation.

## Task

Inspect the current shaq checkout, then implement the three generic broadcast
prerequisites below. This handoff is based on inspection of released shaq 4.4.0;
revalidate names, behavior, safety contracts, and existing tests in your checkout.

Work in shaq only. Keep changes reviewable, preferably as three ordered changes:

1. Queue regions at nonzero offsets in an enclosing shared file.
2. Explicitly committed/cancellable prepared writes.
3. A synchronized per-producer reclamation watermark and sequence identity.

If only taking the first change, implement item 1 and its tests, then stop and
report the remaining items. Items 2 and 3 require separate concurrency reasoning.

Do not implement a payload allocator, event-system schema changes, consumer PID
registry, pidfd monitoring, or managed consumer generations in this task. Those
come later. Do not make broadcast payload-aware.

Full downstream context: [shared-memory-payloads.md](shared-memory-payloads.md).
This handoff is self-contained if copied into another repository.

## Why these APIs are needed

agave-event-system will retain fixed-size broadcast events while storing large
variable-sized data in a separate FIFO byte arena per producer lane. Its producer
needs to establish cell capacity, allocate/copy payload bytes, serialize a handle,
and publish only on success. It must cancel on any pre-publication error.

The integration layer will retain local `(broadcast sequence, allocation end)`
records and reclaim bytes when broadcast proves those sequences cannot still be
read. Consumers should publish no allocator-specific progress.

## Existing behavior to verify

- `src/broadcast.rs`: `Broadcast`, `SharedQueue`, `QueueLayout`, `Producer`,
  `WriteGuard`, `WriteBatch`, `ConsumerCore`, and slice/typed read guards.
- `src/broadcast/producer_lane.rs`: `ProducerLane::{try_reserve,publish,reserved,
  published}`, per-lane layout and ownership.
- `src/broadcast/consumer_state.rs`: `ConsumerState` and `LaneConsumerState`,
  especially `join`, `reserve_limit`, `set_cursor`, and `release`.
- `src/shmem.rs`: `Region`, file mapping, mapping ownership/drop.

In 4.4.0, creation resizes/maps a whole file at offset zero. Every producer owns
one lane for life, and write methods exclusively borrow the producer. Consumer
limits are `next_to_read + capacity`; usize::MAX means unclaimed. Joining uses
two reservation samples and a SeqCst fence; reservation has the matching fence.
Write guards advance reservation immediately and publish unconditionally on
drop. Read guards hold cells until they advance progress on drop. Counter wrap
is unsupported. Producer lanes retire permanently.

## Change 1: bounded queue regions

### Required behavior

- Expose a checked layout query returning queue byte size and required alignment
  for a payload/configuration. Use the actual rounded queue capacity.
- Add creation and typed/untyped join APIs for an explicit offset and extent
  inside an already-sized file.
- Region creation must not resize/truncate the enclosing file or touch adjacent
  bytes. The caller owns outer file layout and sizing.
- Retain existing zero-offset APIs and their compatibility semantics.
- Carry mapping ownership plus a checked queue base and queue extent internally.
  Update initialization, header access, section pointers, and join validation.
- Validate offset/length addition, integer conversions, alignment, file bounds,
  minimum header length, and reconstructed layout before dereferencing.
- Validate against the queue extent, not spare bytes elsewhere in the file.
- Clone/endpoints/read-write guards must retain the correct mapping owner.

Mapping the whole file and selecting a checked aligned view is acceptable and
avoids requiring every queue offset to be an mmap page boundary. If mapping only
a range instead, handle OS mapping granularity separately from queue alignment.
Do not accidentally narrow the advertised API contract.

The existing queue layout is already relative to a base. Prefer reusing its
layout calculations rather than duplicating the queue format externally.
Relocation alone should not require a format change unless inspection finds an
actual stored-layout change. Preserve magic/version/identifier validation and
publish-initialization-last ordering.

### Tests

- Typed/untyped create/join at zero and multiple nonzero aligned offsets.
- Separate mappings/handles of the same region.
- Prefix/suffix sentinels and file length remain unchanged.
- Multiple independent queues within one enclosing file.
- Bad alignment, tiny/truncated regions, overflow, ranges outside file, a queue
  that fits the file but not its declared extent, invalid magic/version/layout.
- Existing file-backed and heap-backed tests continue to work.

Benchmark or otherwise verify that relocation adds no repeated per-event layout
calculations. Keep platform mapping behavior correct for supported non-Linux
targets as well.

## Change 2: cancellable prepared writes

### Required behavior

Add a separate API with explicit commit and cancel-on-drop. Keep legacy
publish-on-drop APIs compatible.

A prepared write exclusively borrows the producer and proves capacity before
expensive caller work. It does not publish anything unless explicitly committed.
Expose a single-cell API first; support a batch form if needed to avoid replacing
existing batch publication with an inefficient per-item loop.

Preferred design: preparation checks capacity and selects cells without advancing
the shared reservation frontier. Commit advances reservation then publication;
drop discards preparation without moving either frontier.

Do NOT implement cancellation by decrementing an already-visible reservation
frontier. A consumer may have joined at the advanced frontier, and could then
skip a successful later event that reuses a cancelled sequence.

### Required proof and edge cases

Establish that capacity remains valid throughout preparation:

- No other writer can reserve this lane while its producer is borrowed.
- Existing consumers only advance or release limits.
- New consumers join the unchanged frontier and permit a full ring of future
  events; prepared count cannot exceed capacity.
- Keep the SeqCst join/reservation handshake and necessary Acquire/Release edges.
- Old published cells being overwritten were proven reclaimable before writes.
- Commit contains no remaining capacity failure after expensive caller work.

Audit coexistence with legacy reservations, including `mem::forget` of a legacy
write guard: reservation and publication may differ. A new safe preparation API
must not expose an unpublished/uninitialized gap through a later commit. Specify
whether such a producer becomes unusable, preparation rejects the state, or the
legacy unsafe contract is sufficient. Do not assume normal guard drop always ran.

Likewise, forgetting a prepared guard must not create a published hole. Ensure
cancellation/unwinding cannot publish partially initialized bytes.

For batches, define explicit full and/or initialized-prefix commit semantics.
Downstream event-system currently documents successful-prefix publication on
serialization error, but its existing implementation wrongly publishes the whole
batch. A prefix-commit operation can preserve that intent. Commit may expose only
initialized cells, and must preserve ordering across wrap and future joins.

Make initialization obligations explicit: safe commit should require initialized
values or tracked initialization, otherwise provide a narrowly documented unsafe
commit. Do not convert an unchecked initialization requirement into a safe API.

### Tests

- Commit visibility and cancel-on-drop invisibility.
- Capacity failure before invoking any caller write/copy work.
- Error/panic cancellation followed by successful publication at the next valid
  sequence.
- Consumer joins during preparation, before commit, and after commit.
- Capacity-sized batches, physical ring wrap, partial serialization/commit.
- Held read guards still prevent overwrite; released guards permit progress.
- No consumers, zero consumer slots, multiple lanes.
- Forgotten guards and mixed legacy/new API use.
- Model or systematically stress the join/preparation/commit races; include
  ordering rationale in source safety comments.

## Change 3: generic reclamation watermark

### Required behavior

Expose sequence identity for prepared writes and a producer-side operation such
as `reclaimable_before()`. Exact naming is up to repository conventions.

Contract: sequences strictly below the returned value will never again be
accessible to any current or future correctly joined consumer. The watermark is
per lane, conservative, and bounded by committed publication. It must be useful
before broadcast ring exhaustion, since an external byte arena can fill first.

For lane capacity B, claimed limit L corresponds to cursor `L - B`. Ignore
unclaimed sentinels and include provisional joining limits:

`R = min(committed_publication, all participating next_to_read cursors)`

With no consumers, R equals committed publication. Do not treat an unpublished
reservation as reclaimable publication. Do not use a global cross-lane minimum
as an allocation sequence; lanes have independent sequence spaces.

Put the synchronization inside shaq. An arbitrary Acquire scan of externally
exposed atomics is not a sufficient contract. Reuse/prove the existing fence-based
join handshake so a racing join either constrains reclamation or starts beyond
the reclaimed prefix. Explain why caching a conservative result is valid under
slot release/reuse and concurrent joins.

The integration layer will pop local allocation records with `sequence < R`.
No callbacks, payload handles, allocator headers, or consumer-specific byte
cursors belong in this API.

### Counter handling

Current simple sequence comparisons do not support wrap. Check arithmetic and
reject exhaustion before sequence/reserve-limit sentinel collisions. Define the
behavior consistently for new APIs and identify any existing paths that need a
matching fix; do not silently claim ABA protection from wrapping counters.

### Tests

- No-consumer watermark equals committed publication.
- A held read guard pins its sequence; dropping it advances eligibility.
- Slowest consumer wins; disconnect releases only its own constraint.
- Joining before/after publication and while a write is prepared.
- Consumer-slot reuse and provisional join cursors.
- Independent producer lanes and batches.
- Published versus outstanding reserved sequences.
- Boundary arithmetic near sentinel/counter exhaustion.
- Concurrent reclaim/join tests demonstrating that reclaimed data cannot become
  readable to a new consumer.

## Deliverable and boundaries

Implement tests and documentation/safety contracts alongside each change. Run
the repository's applicable formatting and tests. Report APIs added, ordering
arguments, compatibility implications, checks run, and any unresolved issues.

Avoid exposing raw private shared-memory internals as a shortcut. Keep the queue
generic and preserve existing users. Managed consumer ownership/generation APIs
are a later change with a separate protocol review; this task does not make
current unsafe `force_release` safe to race with joins/drops.
