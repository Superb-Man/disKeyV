#!/usr/bin/env bash
set -euo pipefail

test_started=$SECONDS
if ! command -v podman >/dev/null 2>&1; then
    echo "Podman is not installed" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-repl-stress-$run_id"
leader="diskeyv-repl-stress-leader-$run_id"
follower1="diskeyv-repl-stress-follower1-$run_id"
follower2="diskeyv-repl-stress-follower2-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
leader_port=${DISKEYV_STRESS_LEADER_PORT:-15300}
follower1_port=${DISKEYV_STRESS_FOLLOWER1_PORT:-15301}
follower2_port=${DISKEYV_STRESS_FOLLOWER2_PORT:-15302}
worker_count=${DISKEYV_STRESS_WORKERS:-4}
record_count=${DISKEYV_STRESS_RECORDS:-2000}
partition_count=${DISKEYV_STRESS_PARTITION_RECORDS:-1000}
loader_count=${DISKEYV_STRESS_LOADERS:-16}
hot_writes=${DISKEYV_STRESS_HOT_WRITES_PER_LOADER:-100}
value_size=${DISKEYV_STRESS_VALUE_SIZE:-128}
containers=("$leader" "$follower1" "$follower2")

for setting in "$worker_count" "$record_count" "$partition_count" \
    "$loader_count" "$hot_writes" "$value_size"; do
    if [[ ! "$setting" =~ ^[1-9][0-9]*$ ]]; then
        echo "Stress settings must be positive integers" >&2
        exit 2
    fi
done
if ((worker_count > 64 || record_count + partition_count > 3500 ||
     value_size > 1048576)); then
    echo "Stress settings exceed worker, index, or value limits" >&2
    exit 2
fi

cleanup() {
    for container in "${containers[@]}"; do
        podman rm --force "$container" >/dev/null 2>&1 || true
    done
    podman network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_healthy() {
    local container=$1
    for _ in {1..40}; do
        if podman exec "$container" \
            /usr/local/bin/diskeyv-client health 5000 >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "Container did not become healthy: $container" >&2
    podman logs "$container" >&2 || true
    return 1
}

container_for() {
    case "$1" in
        leader) echo "$leader" ;;
        follower1) echo "$follower1" ;;
        follower2) echo "$follower2" ;;
        *) return 2 ;;
    esac
}

run_client() {
    local container
    container=$(container_for "$1")
    shift
    podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
        /usr/local/bin/diskeyv-client "$@"
}

bulk_operation() {
    local operation=$1
    local replica=$2
    local prefix=$3
    local count=$4
    local expected_term=${5:-}
    local container
    container=$(container_for "$replica")
    local base_count=$((count / loader_count))
    local remainder=$((count % loader_count))
    local pids=()
    for ((loader = 0; loader < loader_count; ++loader)); do
        local shard_count=$base_count
        if ((loader < remainder)); then
            shard_count=$((shard_count + 1))
        fi
        ((shard_count == 0)) && continue
        local shard_prefix="${prefix}s${loader}-"
        if [[ "$operation" == "load" ]]; then
            podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
                /usr/local/bin/diskeyv-client load 5000 "$shard_prefix" \
                "$shard_count" "$value_size" >/dev/null &
        else
            podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
                /usr/local/bin/diskeyv-client verify 5000 "$shard_prefix" \
                "$shard_count" "$value_size" "$expected_term" \
                >/dev/null &
        fi
        pids+=("$!")
    done
    local failed=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then failed=1; fi
    done
    ((failed == 0))
}

wait_bulk_consistent() {
    local replica=$1
    local prefix=$2
    local count=$3
    local term=$4
    for _ in {1..40}; do
        if bulk_operation verify "$replica" "$prefix" "$count" "$term" \
            >/dev/null 2>&1; then
            echo "Verified $count $prefix records on $replica"
            return 0
        fi
        sleep 0.25
    done
    echo "Replica did not converge: $replica ($prefix)" >&2
    return 1
}

wait_exact_read() {
    local replica=$1
    local key=$2
    local expected=$3
    for _ in {1..40}; do
        local actual
        actual=$(run_client "$replica" get 5000 "$key" 2>/dev/null || true)
        if [[ "$actual" == "$expected" ]]; then return 0; fi
        sleep 0.25
    done
    echo "Replica did not converge for key $key: $replica" >&2
    return 1
}

echo "[1/8] Build image and start isolated replicas"
if [[ ${DISKEYV_STRESS_SKIP_BUILD:-0} != 1 ]]; then
    podman build --format docker --target runtime --tag "$image" "$repo_dir"
else
    podman image exists "$image"
fi
podman network create "$network" >/dev/null
podman run --detach --name "$follower1" \
    --network "$network" --network-alias follower1 \
    --init --read-only --tmpfs /tmp:size=16m --cpus 2 --memory 256m \
    --security-opt no-new-privileges -p "127.0.0.1:$follower1_port:5000" \
    -e DISKEYV_REPLICA_ID=2 "$image" follower 5000 >/dev/null
podman run --detach --name "$follower2" \
    --network "$network" --network-alias follower2 \
    --init --read-only --tmpfs /tmp:size=16m --cpus 2 --memory 256m \
    --security-opt no-new-privileges -p "127.0.0.1:$follower2_port:5000" \
    -e DISKEYV_REPLICA_ID=3 "$image" follower 5000 >/dev/null
podman run --detach --name "$leader" \
    --network "$network" --network-alias leader \
    --init --read-only --tmpfs /tmp:size=16m --cpus 4 --memory 512m \
    --security-opt no-new-privileges -p "127.0.0.1:$leader_port:5000" \
    -e DISKEYV_REPLICA_ID=1 -e "DISKEYV_WORKERS=$worker_count" \
    "$image" leader 5000 follower1:5000 follower2:5000 >/dev/null
wait_healthy "$follower1"
wait_healthy "$follower2"
wait_healthy "$leader"

echo "[2/8] Load $record_count unique keys over $loader_count clients"
bulk_operation load leader base- "$record_count"

echo "[3/8] Verify every unique key on all three replicas"
for replica in leader follower1 follower2; do
    wait_bulk_consistent "$replica" base- "$record_count" 1
done

echo "[4/8] Contend on one key from every load stream"
hot_pids=()
for ((loader = 0; loader < loader_count; ++loader)); do
    podman exec -e DISKEYV_HOST=127.0.0.1 "$leader" \
        /usr/local/bin/diskeyv-client hotload 5000 consistency-hot \
        "$hot_writes" "$value_size" "$((loader * 1000000))" \
        >/dev/null &
    hot_pids+=("$!")
done
for pid in "${hot_pids[@]}"; do wait "$pid"; done
leader_hot=$(run_client leader get 5000 consistency-hot)
wait_exact_read follower1 consistency-hot "$leader_hot"
wait_exact_read follower2 consistency-hot "$leader_hot"

echo "[5/8] Disconnect follower2 and continue with a leader/follower1 quorum"
podman network disconnect "$network" "$follower2"
bulk_operation load leader partition- "$partition_count"
wait_bulk_consistent leader partition- "$partition_count" 1
wait_bulk_consistent follower1 partition- "$partition_count" 1

echo "[6/8] Reconnect follower2 and verify complete prefix repair"
podman network connect --alias follower2 "$network" "$follower2"
run_client leader put 5000 follower2-repair-trigger 2
wait_bulk_consistent follower2 base- "$record_count" 1
wait_bulk_consistent follower2 partition- "$partition_count" 1
wait_exact_read follower2 consistency-hot "$leader_hot"

echo "[7/8] Prove the repaired follower can form the next quorum"
podman network disconnect "$network" "$follower1"
run_client leader put 5000 repaired-follower-quorum 3
leader_repaired=$(run_client leader get 5000 repaired-follower-quorum)
wait_exact_read follower2 repaired-follower-quorum "$leader_repaired"
podman network connect --alias follower1 "$network" "$follower1"

echo "[8/8] Disconnect both followers and require NO_QUORUM"
podman network disconnect "$network" "$follower1"
podman network disconnect "$network" "$follower2"
if run_client leader put 5000 must-not-commit 9 >/dev/null 2>&1; then
    echo "FAIL: PUT succeeded without a follower quorum" >&2
    exit 1
fi
if run_client leader get 5000 must-not-commit >/dev/null 2>&1; then
    echo "FAIL: failed PUT was published in the leader index" >&2
    exit 1
fi

echo "Replication stress and consistency test passed: "\
"$record_count base keys, $partition_count partition keys, "\
"$((loader_count * hot_writes)) hot-key overwrites, "\
"$((SECONDS - test_started)) seconds"
