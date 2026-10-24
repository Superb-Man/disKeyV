# disKeyV

disKeyV is an in-memory, replicated key-value store written in C++17, inspired by
[LoLKV (NSDI 2024)](https://www.usenix.org/conference/nsdi24/presentation/alquraan).
It uses immutable segment records, majority replication over TCP, automatic
leader election and recovery, and segment-based garbage collection.

The implementation is **logless in its storage design**: replication replays
objects from `SegmentStore` using offset metadata, rather than maintaining a
separate payload history or write-ahead log. This does not make it disk-durable.
All database state, membership configuration, terms, and votes are memory-only.

This is a research/learning prototype, not a production database or a complete
reproduction of LoLKV's RDMA design, performance, or correctness proof.

## Start here

- [System structure and flows](docs/system-flow.md): components, PUT/GET ordering,
  asynchronous follower indexing, election, recovery, and GC.
- [Step-by-step replication examples](docs/replication-examples.md): concrete
  records, offsets, worker prefixes, failures, and recovery.
- [Architecture and implementation](docs/architecture.md): source files,
  variables, threads, locks, and protocol details.
- [Testing and operations](docs/testing-and-operations.md): full test recipes,
  multi-computer setup, logging, and troubleshooting.
- [Implementation analysis](docs/implementation-analysis.md): guarantees,
  limitations, and differences from LoLKV.
- [Documentation index](docs/README.md): all topic guides.

## What is implemented?

| Area | Current implementation |
| --- | --- |
| Storage | Immutable object versions in a fixed segment arena; atomic offset-based hash index |
| Writes | Bounded request queue, worker batching, ownership confirmation before segment use |
| Replication | Parallel per-worker follower channels, reused TCP connections, cumulative storage ACKs |
| Completion | Early majority completion; leader index publication before successful client reply |
| Follower indexing | Background hash publication after the leader's separate `INDEX_COMMIT` notice |
| Reads | Leader-only normal GET, value copy protected against GC, fresh same-term read quorum |
| Failover | Heartbeats, pre-vote/vote election, term fencing, recovery before serving |
| Recovery | Per-worker consolidation before GC; retained-version merge after GC |
| Catch-up | Prefix replay, plus staged state transfer when layout/ownership requires it |
| GC | Reclaim obsolete versions, relocate live records, reuse sealed segments |
| Debugging | Configurable developer logs and a separate-terminal log viewer |
| Deployment | Five-node Compose environment, Docker/Podman tests, three-laptop example |

There is no DELETE, transaction support, disk persistence, automatic index
resizing, authenticated cluster protocol, or exactly-once client deduplication.

## System structure

The supplied Compose cluster has five replicas; every node knows the other
four. A majority is three:

```text
N = configured peers + local replica
q = floor(N / 2) + 1

                       client
                         |
                   elected leader
                  queue -> workers
                         |
           parallel per-worker TCP channels
             /          / \          \
        follower A  follower B  follower C  follower D
          segments + local index on every replica
```

Service names such as `leader` and `follower1` describe startup configuration,
not permanent roles. Any election-enabled survivor can become leader.
Unavailable peers do not reduce the configured quorum.

Each replica contains:

```text
Replica
├── ReplicaState: replica ID, role, current term, vote and leader identity
├── SegmentStore: immutable objects, segment ownership, layout and progress
├── HashTable: key -> latest packed segment/object offset
├── IncarnationTable: counters for ordering versions of a key
├── TCP listener and per-connection handlers
├── Bounded client queue and request worker threads
├── Election thread
├── Background follower-index thread
└── While acting as leader:
    ├── one replication group per logical worker
    │   └── one persistent data channel per configured peer
    └── shared persistent ownership/read-confirmation channels
```

A `worker_id` identifies a logical replication stream, not an operating-system
thread ID or a permanent key shard. Workers can handle any key. Configure the
same worker count on all replicas.

### Write flow

```text
CLIENT_PUT
  -> bounded request queue
  -> worker obtains a majority-confirmed segment, if a new one is needed
  -> append immutable object locally; keep it pinned against GC
  -> send batches to followers in parallel
  -> followers store records at the supplied offsets; return cumulative ACKs
  -> leader calculates the majority watermark
  -> leader publishes covered offsets in its hash index; removes pins
  -> return OK to the client if index publication succeeded
  -> asynchronously send INDEX_COMMIT for the leader-indexed worker prefix
  -> followers publish those offsets through their background index worker
```

A storage ACK is not an index update or a global commit certificate. Follower
indexing never needs to precede record replication, and normal follower
publication waits for the leader's notice. A slow non-quorum follower need not
delay a successful PUT.

For one worker, prefixes `[12, 12, 12, 10, 8]` give a quorum watermark of 12.
Prefixes `[12, 12, 10, 10, 8]` give a watermark of 10. Sequences are ordered
within each worker/term, not globally across workers.

A failed PUT is not rolled back. In particular, an append that returned
`NO_QUORUM` may later become visible through cumulative replication or recovery.
Client retries create another version; there are no request IDs for deduplication.

### Read flow

```text
CLIENT_GET
  -> require a non-recovering leader
  -> copy the indexed value or miss under the GC/recovery barrier
  -> release the barrier
  -> obtain a fresh same-term majority confirmation
  -> return the copied result only if confirmation succeeds
```

This confirmation checks leadership, not follower value equality, and is not a
heartbeat-based read lease. Both hits and misses require confirmation.
`get-local` and `verify-local` deliberately bypass it for diagnostics; their
results can lag or expose records outside the normal leader-published view.

### Election, recovery, and GC

After majority election, the winner remains gated while it consolidates and
repairs survivor data. It becomes a client-serving leader only after recovery
succeeds and its election term is still valid.

Before GC, recovery selects the greatest stored prefix independently for each
worker, validates records and ownership, and repairs the active survivor set.
After GC, it merges retained key versions and installs a common layout instead
of demanding reclaimed history.

For five configured members, an elected survivor can contribute its local state
and needs two valid remote summaries. An empty explicit replacement needs
three remote summaries. Initially unreachable endpoints can be skipped without
shrinking membership; a later participant failure can abort the current attempt.
Do not omit failed members from configuration to manufacture a smaller quorum.

GC runs opportunistically in leader workers under segment pressure. It skips
active or pinned sources, relocates live objects from eligible sparse sealed
segments, and frees obsolete payloads. New destinations require ownership
confirmation. Layout versions fence old coordinates; lagging followers can
receive staged state transfer. GC cannot evict live keys or expand the index.

See [the detailed flows](docs/system-flow.md) for a five-node lagging-follower
and leader-crash example.

## Repository layout

```text
main.cpp                         server startup and environment configuration
cl.cpp                           client, deterministic loads and verification
replica/
  replica.hpp                    workers, TCP handlers, replication, election
  replica_state.hpp              role, term, leader and voting state
engine/                          worker metadata and immutable append paths
storage/                         objects, segments, arena, GC and state transfer
index/                           atomic latest-version offset hash index
concurrency/                     hashed incarnation counters
network/                         endpoints, sockets, protocol framing/validation
replication/                     ownership quorum and shared control channels
recovery/                        survivor consolidation and repair
logging/                         developer logging
tests/                           native regressions and container scenarios
tools/diskeyv-logcat.sh           separate-terminal log viewer
docs/                            design, flow, analysis and operations guides
Dockerfile                       Linux server/client image
compose.yaml                     five-node election-enabled deployment
compose.recovery.yaml            explicit-recovery Compose overlay
.env.three-laptops.example       one replica per laptop LAN configuration
CMakeLists.txt                   build, five native test suites, sanitizers
```

## Build and run native tests

Use Linux, CMake 3.16+, and a C++17 compiler with POSIX threads/sockets.
On macOS, use Linux containers; the current logger uses Linux thread-ID support.

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

The server and client are `build/diskeyv-server` and `build/diskeyv-client`.
The five suites are `diskeyv-correctness`, `diskeyv-index`, `diskeyv-gc`,
`diskeyv-ownership`, and `diskeyv-reads`.

## Run five replicas with Docker Compose

```bash
docker compose up --build --detach --wait
docker compose ps
docker compose logs --follow
```

| Service | Replica ID | Host endpoint |
| --- | ---: | --- |
| `leader` | 1 | `127.0.0.1:5000` |
| `follower1` | 2 | `127.0.0.1:5001` |
| `follower2` | 3 | `127.0.0.1:5002` |
| `follower3` | 4 | `127.0.0.1:5003` |
| `follower4` | 5 | `127.0.0.1:5004` |

All containers listen internally on port 5000 and use four workers by default.
Health checks show service readiness, not elected leadership. Look for
`state=leader ... recovery=complete` in recent logs.

For example, if the currently elected service is `leader`:

```bash
docker compose exec -T -e DISKEYV_HOST=127.0.0.1 leader \
  /usr/local/bin/diskeyv-client put 5000 greeting 72,101,108,108,111

docker compose exec -T -e DISKEYV_HOST=127.0.0.1 leader \
  /usr/local/bin/diskeyv-client get 5000 greeting
```

After failover, replace `leader` with the actual elected service. There is no
automatic client redirection or dynamically updated leader service alias.
A native client can use that service's published host port.

Stop this deployment with:

```bash
docker compose down
```

Stopping every replica destroys the database contents. There is no persistent
database volume.

If `docker` is a Podman compatibility wrapper and Compose cannot connect to its
API socket, use the direct Podman tests below. Creating
`/etc/containers/nodocker` hides a notice; it does not start the API socket.

## Run five replicas without containers

Run each command in a separate terminal, preferably starting followers first.
Every command includes all four other members:

```bash
DISKEYV_REPLICA_ID=2 DISKEYV_WORKERS=4 \
./build/diskeyv-server follower 5001 5000 5002 5003 5004
```

```bash
DISKEYV_REPLICA_ID=3 DISKEYV_WORKERS=4 \
./build/diskeyv-server follower 5002 5000 5001 5003 5004
```

```bash
DISKEYV_REPLICA_ID=4 DISKEYV_WORKERS=4 \
./build/diskeyv-server follower 5003 5000 5001 5002 5004
```

```bash
DISKEYV_REPLICA_ID=5 DISKEYV_WORKERS=4 \
./build/diskeyv-server follower 5004 5000 5001 5002 5003
```

```bash
DISKEYV_REPLICA_ID=1 DISKEYV_WORKERS=4 \
./build/diskeyv-server leader 5000 5001 5002 5003 5004
```

Port-only peers mean `127.0.0.1:PORT`. For separate machines, use reachable
`HOST:PORT` peers and unique IDs. See the
[LAN deployment guide](docs/testing-and-operations.md) and
[three-laptop environment example](.env.three-laptops.example).
Compose's loopback host bindings are not sufficient for a remote LAN client.

The executable accepts:

```text
diskeyv-server <leader|follower|recover> <port> <peer>...
```

`leader` means preferred initial candidate when elections are enabled.
`recover` first reconstructs an empty process from survivors; use it in a
controlled manual-recovery setup with elections disabled and the old leader
stopped. Do not restart all survivors or remove configured members to recover.

## Client commands

Examples assume the elected leader is currently on host port 5000:

```bash
./build/diskeyv-client health 5000
./build/diskeyv-client put 5000 customer:42 7,8,9
./build/diskeyv-client get 5000 customer:42

# Diagnostic only: inspect the replica listening on 5001.
./build/diskeyv-client get-local 5001 customer:42
```

```text
diskeyv-client put          <port> <key> <value_csv>
diskeyv-client get          <port> <key>
diskeyv-client get-local    <port> <key>
diskeyv-client health       <port>
diskeyv-client load         <port> <key_prefix> <count> <value_size>
diskeyv-client hotload      <port> <key> <count> <value_size> <seed>
diskeyv-client verify       <port> <key_prefix> <count> <value_size> <term>
diskeyv-client verify-local <port> <key_prefix> <count> <value_size> <term>
```

Set `DISKEYV_HOST` for a remote client. `load` generates deterministic keys and
values; `hotload` overwrites one key. `verify` checks generated values and their
object creation term, which can differ from the current leader term.

## Configuration and limits

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `DISKEYV_REPLICA_ID` | leader/recover: 1; follower: 2 | Override with a unique nonzero member ID |
| `DISKEYV_WORKERS` | leader/recover: 4; follower: 2 | 1–64; set the same count on every member |
| `DISKEYV_ELECTION_ENABLED` | `1` | `1`/`true` or `0`/`false` |
| `DISKEYV_HOST` | `127.0.0.1` | Client destination host |
| `DISKEYV_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR`, `OFF` |
| `DISKEYV_LOG_FILE` | unset | Log file instead of stderr |
| `DISKEYV_LEADER_WORKERS` | Compose: 4 | Compose override for the worker count on all five members |

Host-port overrides are `DISKEYV_LEADER_PORT` and
`DISKEYV_FOLLOWER1_PORT` through `DISKEYV_FOLLOWER4_PORT`.

| Limit | Current value |
| --- | --- |
| Wire protocol | Version 10; rebuild clients and all replicas together |
| Key / value | 1–63 bytes / up to 1 MiB |
| Default arena | 32 segments × 1,024 object slots |
| Default hash index | 4,096 slots; no resize |
| Client request queue | 1,024 entries |
| Normal replication batch | Up to 16 entries, soft 256 KiB budget |
| Outstanding window | Four batches per worker/follower channel |
| Recovery/state-transfer batch | Up to 256 entries, soft 1 MiB budget |
| Follower ready-to-index queue | 4,096 offsets; inline fallback when full |

A batch can include one legal large object beyond its soft byte budget.
Slot counts are not fixed payload-byte capacities. A workload of 100,000
overwrites differs from 100,000 unique keys: GC reclaims old versions but does
not expand the live-key index. There is no promised requests-per-second figure;
measure your hardware, value sizes, concurrency, quorum latency, and GC pressure.

## Replication and failure tests

Run failure-injection scripts only against disposable test data. The Docker
smoke script uses the repository's Compose project and tears it down; do not
run it against a deployment you want to keep. Direct Podman scripts use their
own temporary container/network names and remove them on exit.

Native correctness, ownership, read, index, and GC regressions:

```bash
ctest --test-dir build --output-on-failure
```

Docker basic replication/follower restart:

```bash
DISKEYV_CONTAINER_ENGINE=docker bash tests/docker_smoke.sh
```

Docker explicit multi-term recovery:

```bash
DISKEYV_CONTAINER_ENGINE=docker bash tests/docker_recovery_smoke.sh
```

Docker automatic leader failover:

```bash
CONTAINER_CLI=docker bash tests/10_leader_failure.sh
```

Podman basic replication/follower restart:

```bash
bash tests/podman_smoke.sh
```

Podman stress, concurrent overwrites, partition, and catch-up:

```bash
bash tests/podman_replication_stress.sh
```

Podman explicit recovery with a larger dataset:

```bash
DISKEYV_RECOVERY_RECORDS=3000 \
DISKEYV_RECOVERY_WORKERS=4 \
DISKEYV_RECOVERY_LOADERS=16 \
bash tests/podman_recovery_smoke.sh
```

Podman leader stop, election, and automatic recovery:

```bash
CONTAINER_CLI=podman bash tests/10_leader_failure.sh
```

Podman lagging followers, leader SIGKILL, election, and recovery:

```bash
DISKEYV_LAGGING_RECORDS=3000 bash tests/podman_lagging_failover.sh
```

GC/state-transfer regressions:

```bash
ctest --test-dir build -R '^diskeyv-gc$' --output-on-failure
```

There is no dedicated Docker GC script. The native GC suite uses small arenas
and real loopback TCP replicas to force reclamation and exercise post-GC repair.
The normal runtime image contains only the server/client, not CTest binaries.

## Developer logging

Use a separate terminal to follow important events:

```bash
# Add these variables to a native server command:
DISKEYV_LOG_LEVEL=DEBUG DISKEYV_LOG_FILE=/tmp/diskeyv.log \
DISKEYV_REPLICA_ID=1 DISKEYV_WORKERS=4 \
./build/diskeyv-server leader 5000 5001 5002 5003 5004
```

```bash
bash tools/diskeyv-logcat.sh /tmp/diskeyv.log
```

Use different files for different local replicas. For containers use
`docker compose logs --follow`. See [logging](docs/logging.md) for filters.

## Safety and persistence boundaries

- Use a trusted isolated network: control messages and sender IDs are not authenticated.
- Terms, votes, configuration, and objects are volatile; no durable membership or restart-safe consensus is provided.
- Configure the complete peer set consistently. Different endpoint aliases can accidentally describe the same replica.
- Normal GET requires a fresh quorum; diagnostic local reads do not provide that guarantee.
- Static-role followers require recovery authorization before they can confirm normal reads.
- Idle or older-term empty-node restart repair has limitations; prefix replay alone is not a universal restart protocol.
- Recovery may finish previously unacknowledged operations; failed requests are not guaranteed absent.
- GC pauses local access; state transfer pauses its receiver and can temporarily duplicate retained state.
- Replication is not a backup against losing all volatile copies.

“Persistent replication” here means **persistent TCP sessions**, not persistent
database storage. Developer log files are diagnostics, not a recovery WAL.
