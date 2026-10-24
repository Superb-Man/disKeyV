# System structure and request flows

This guide describes the current TCP implementation. For startup commands,
configuration, and tests, use the [project README](../README.md).
For a longer trace with actual object/offset values, see
[replication examples](replication-examples.md).

## 1. Components and ownership of data

```text
Client TCP connection
        |
        v
Replica::connection_loop()          network framing and message dispatch
        |
        +-- CLIENT_PUT ------------> req_q -> worker_loop()
        +-- CLIENT_GET ------------> copy + fresh leadership confirmation
        +-- replication/control ---> follower handlers / election / recovery

Local storage on every replica
        |
        +-- SegmentStore ----------> immutable ObjectEntry values
        +-- HashTable -------------> atomic offsets into that same store
        +-- IncarnationTable ------> version-order counters
```

There are no independent storage/index services. Most implementation is
header-defined; the server coordinates these components inside one process.

| Component | Source | Responsibility |
| --- | --- | --- |
| Startup | [main.cpp](../main.cpp) | Parse roles, peers, IDs, workers; install signal handlers |
| Server orchestration | [replica.hpp](../replica/replica.hpp) | Queue, workers, TCP channels, handlers, election, GC trigger |
| Append engine | [put_path.hpp](../engine/put_path.hpp) | Create immutable records or reproduce exact received offsets |
| Arena | [segment_store.hpp](../storage/segment_store.hpp) | Own objects, allocations, layout version, progress, GC pins |
| Index | [hash_table.hpp](../index/hash_table.hpp) | Select latest key version; conditionally relocate pointers |
| Control quorum | [ownership_replication.hpp](../replication/ownership_replication.hpp) | Shared persistent ownership and read-confirmation channels |
| Recovery | [recovery_coordinator.hpp](../recovery/recovery_coordinator.hpp) | Collect evidence, fence, consolidate, repair, rebuild |
| Reclamation/transfer | [segment_gc.hpp](../storage/segment_gc.hpp) | Move live objects; capture, validate, and install state |
| Wire protocol | [message.hpp](../network/message.hpp) | Versioned frames, message shapes, limits, serialization |

### Identifiers: what each number means

| Identifier | Meaning |
| --- | --- |
| `replica_id` | Unique member identity used by voting/control messages |
| `worker_id` | Logical append/replication stream; not OS thread ID |
| `ObjectEntry::term_id` | Leadership term that created this version |
| `ObjectEntry::seq_num` | Sequence within that worker's term |
| `ObjectEntry::incarnation` | Version-order counter from a hash-selected key bucket |
| `SegmentMetadata::owner_id` | Logical worker owning the allocation |
| `SegmentMetadata::term_id` / `seg_ver` | Allocation term and store-wide generation |
| `SegmentMetadata::committed` | Local ownership-quorum confirmation, not every object's commit status |
| `seg_idx` / `obj_idx` | Physical object coordinates |
| `SegmentStore::layout_version` | Fences physical coordinates after layout changes |

Two workers can both produce sequence 1. Cross-worker sequence numbers are
not comparable. The index uses `(object.term_id, object.incarnation)` to choose
the latest version. Incarnation buckets can collide, so values can skip.

Index slots pack `((segment_index + 1) << 32) | object_index`. Zero means
empty; the segment-index bias makes segment 0/object 0 representable.

The arena is the long-lived owner of payloads. Replay directories, pending
publications, and follower indexing queues retain coordinates, not another
value history. Network batches and state snapshots may temporarily copy values.

## 2. Threads and synchronization

A replica has request workers, a TCP accept loop with connection handlers,
an election thread when enabled, and a background follower-index thread.
An active leader also has one replication channel thread per
`(worker, configured peer)`, plus shared per-peer control channels.

Four logical workers and four peers can therefore create 16 data-channel
threads on the active leader, in addition to its other threads. These are
independent TCP streams, not four globally ordered database shards.

| Mechanism | Why it exists |
| --- | --- |
| `req_q` + completion promises | Bounded backpressure; return the actual worker result |
| `replication_epoch_mutex` | Shared normal access; exclusive recovery, layout replacement, and GC |
| `store.data_mutex` | Keep appends/replay/state capture/reclamation consistent |
| `rs.election_mutex` | Serialize leadership checks, term changes, and publication |
| `ownership_mutex` | Order allocation proposals and reuse shared read/control channels |
| Per-worker follower progress mutex | Keep that stream gap-free and serialize commit notices |
| `store.pending[segment]` | Keep unpublished leader objects from being reclaimed |

The atomic index does not make the entire system lock-free. Network GET copies
the object before releasing the shared reclamation barrier. In-process users
should use `get_copy()`; a raw `get()` pointer requires the caller to keep the
shared epoch for its entire dereference lifetime.

## 3. PUT: three separate publication stages

```text
Leader worker                         Followers
-------------                         ---------
1. Confirm new segment ownership ----> accept allocation descriptor
   [skip for already confirmed segment] <---- ownership replies; require q
2. put_owned(): append record
3. publish(): expose replay offsets ---> PUT_REPL_BATCH, independent channels
                                       store exact offsets and advance prefix
                                  <--- ACK(stored contiguous prefix)
4. calculate majority watermark
5. ht.apply(): leader index publication
6. complete client promise
7. note_index_committed() ------------> INDEX_COMMIT(indexed prefix)
                                       release eligible pending offsets
                                       background ht.apply()
```

`publish()` in `ParallelReplicationGroup` means “available for replication.”
It is not the leader's hash publication.

1. `submit_put()` validates the request and waits for bounded queue space.
2. A worker drains a bounded batch and may attempt pressure-triggered GC.
3. If its active segment is absent/full/unconfirmed, `allocate_owned_segment()`
   confirms ownership with a majority before any object append.
4. `PutPath::put_owned()` assigns an object sequence/incarnation and appends
   immutable bytes. The leader retains an `ApplyRecord` and pins the segment.
5. Per-follower channels replicate concurrently using persistent TCP sessions.
   Followers reject gaps, wrong terms, incompatible layouts, and invalid offsets.
6. The q-th largest known prefix, including the local stored prefix, is the
   worker's `commit_watermark`.
7. Covered pending objects pass through `ht.apply()` under the term/storage
   checks. APPLIED or SUPERSEDED permits OK; FULL maps to INDEX_FULL.
8. The contiguous successfully indexed prefix advances `indexed_sequence`.
   Channel threads send notices through `note_index_committed()` independently.

Followers maintain two different frontiers:

| Frontier | Meaning |
| --- | --- |
| `highest_contiguous_sequence` | Gap-free stored data that can be ACKed |
| `index_committed_sequence` | Leader-indexed prefix learned through notices; eligible for local indexing |

`pending_index` holds stored offsets awaiting a notice.
`follower_index_queue` holds eligible offsets awaiting background publication.
The ready queue is bounded to 4,096 entries; overflow is processed inline instead
of growing indefinitely. The background worker processes at most 64 offsets
per batch.

A notice ACK can precede completion of the background index work. Consequently,
a diagnostic follower read immediately after leader OK may still lag.
An index-publication failure can also stop advancement of the contiguous notice
prefix; do not assume followers instantly mirror every leader slot.

## 4. GET: storage lifetime and leadership are separate checks

```text
Client -> leader
          |
          +-- shared epoch + election lock
          |     check role/term; copy indexed hit or record miss
          |
          +-- release locks
          |
          +-- fresh READ_CONFIRM round over shared control channels
          |     require recognized leader, same term, distinct responder IDs
          |
          +-- success: return owned copy or NOT_FOUND
                failure: discard copied value/version and return failure
```

Confirmation does not fetch follower values or elect a leader. A lagging follower
may confirm the recognized leader. Each normal GET needs fresh confirmation,
not a cached heartbeat or lease; a single-node quorum needs no remote reply.

Static-role followers recognize a read leader through recovery authorization.
Simply starting a static leader and followers does not authorize normal GET
confirmation. Diagnostic `get-local` intentionally skips that protocol.

## 5. Catch-up and layout replacement

On reconnect, a data channel asks `PREFIX_QUERY` for the actual stored prefix.
For compatible retained history it replays the missing current-worker-term
suffix. Exact retries are accepted idempotently; gaps are rejected.
Each channel can keep four batches outstanding.

Known-lagging channels retry in the background. A caught-up idle channel may
not detect an empty restarted follower until later traffic; prefix replay in a
new term alone cannot reconstruct every older-term key.

If physical layout or allocation state differs, repair uses state transfer:

```text
STATE_BEGIN -> receiver acquires exclusive install epoch -> ACK
sender captures state only after that ACK
STATE_BATCH -> receiver stages bounded batches
STATE_END   -> validate counts, versions, coordinates, worker bounds and index
            -> install the matching store/index/progress together -> ACK
```

A failed/incomplete transfer leaves the receiver's previous state installed.
Successful installation clears stale offset queues and reconstructs progress.
A full-state index rebuild is not a historical record of which entries were
published on the old leader; local diagnostic reads are not a commit proof.

## 6. Election and automatic recovery

```text
FOLLOWER
   |
   +-- missed heartbeats / preferred startup
   v
PRE-VOTE -> CANDIDATE + vote in higher term
                       |
                       +-- majority votes
                       v
                CANDIDATE + recovering=true
                       |
                announce winner with heartbeats
                fence -> collect -> consolidate -> repair -> rebuild
                       |
                final role/term checks
                       v
               LEADER + recovering=false
                       |
                 client PUT/GET allowed
```

The normal heartbeat interval is 150 ms. The election timeout is 3,000 ms plus
`75 ms × (replica_id % 7)`, polled every 50 ms. Failover duration additionally
includes network calls, survivor validation, and data repair.

The winner invokes `consolidate_elected_leader()` and
`RecoveryCoordinator::recover_elected(term)` before exposing leadership.
Higher terms fence old traffic. Failure during consolidation keeps client
operations gated rather than serving a partially rebuilt database.

### Concrete five-node lagging-follower failure

Assume R1 is leader, quorum 3, and GC has not run. The table follows just one
worker; other workers have independent prefixes.

| Event | R1 | R2 | R3 | R4 | R5 |
| --- | --- | --- | --- | --- | --- |
| All caught up | 100 | 100 | 100 | 100 | 100 |
| R4/R5 disconnected; next 100 writes reach quorum | 200 | 200 | 200 | 100 | 100 |
| R1 crashes; R4/R5 reconnect | lost | 200 | 200 | 100 | 100 |

The surviving four members can elect a candidate with three votes.
Do not infer that the candidate alone already has every worker's newest data.

The elected survivor contributes its local evidence and gathers at least two
valid remote summaries, keeping the configured denominator at five.
It consolidates the maximum retained prefix for each worker, repairs active
participants, rebuilds its index, and only then starts serving.

Every possible three-member recovery set among R2–R5 intersects the previously
acknowledging majority `{R1,R2,R3}` at R2 or R3, assuming those processes retained
their acknowledged data. This explains why stale voters need not supply the
fresh suffix themselves. It is not a proof covering volatile restart losses.

Recovery may also finish a suffix that was stored but never acknowledged by
the old leader. Selecting a maximum prefix does not certify that the old
leader had committed it; the new recovery attempt must repair before serving.

### Empty manual replacement versus elected survivor

For N=5 and q=3:

- An elected survivor contributes one local participant and needs two remote summaries.
- An empty `recover` process contributes no local history and needs three remote summaries.

Keep all configured endpoints, even if some are unavailable. Initial missing
responders can be excluded from the active attempt, but later failure by an
active fencing/fetch/repair participant can abort recovery. All-members-down
restart cannot recover volatile data.

Without GC, recovery validates contiguous per-worker histories and canonical
ownership/offsets. After GC, it merges retained versions using
`(term_id, incarnation)`, builds a common layout, and preserves numerical
high-water marks without demanding reclaimed objects.

## 7. Segment GC

```text
worker sees segment pressure
  -> exclusive local epoch + storage protection
  -> find SEALED, unpinned segment with <=50% indexed/live occupied slots
  -> copy still-live objects into confirmed destinations
  -> CAS-relocate index only if it still points at each original
  -> release obsolete source payloads; mark source FREE
  -> advance layout version; repair mismatching peers through state transfer
```

Example: a sealed segment stores `a@1, b@1, c@1, d@1`. Later segments hold
`a@2, b@2, c@2`. Only `d@1` remains live in the old segment, so GC relocates it
unchanged and reclaims the source. The next source allocation gets a new
generation; old packets cannot safely reuse its former coordinates.

The pressure threshold reserves enough free segments for all running worker
batches: collection starts at `max(batch headroom, segment_count / 4)`.
With the normal arena and four workers, that threshold is eight free segments.
At/below headroom, collection waits for the exclusive epoch instead of merely
trying it opportunistically.

GC is not a separate background service or a distributed RDMA collector.
It pauses local users and requires relocation space. Active/pinned/all-live
segments and an exhausted live-key index remain capacity limits.
