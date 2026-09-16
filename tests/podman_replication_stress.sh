#!/usr/bin/env bash
set -euo pipefail

test_started=$SECONDS
command -v podman >/dev/null 2>&1 || { echo "Podman is not installed" >&2; exit 2; }
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-repl-stress-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
replica_count=5
quorum=3
aliases=(leader follower1 follower2 follower3 follower4)
containers=()
for alias in "${aliases[@]}"; do containers+=("diskeyv-stress-${alias}-$run_id"); done
worker_count=${DISKEYV_STRESS_WORKERS:-4}
record_count=${DISKEYV_STRESS_RECORDS:-2000}
partition_count=${DISKEYV_STRESS_PARTITION_RECORDS:-1000}
loader_count=${DISKEYV_STRESS_LOADERS:-16}
hot_writes=${DISKEYV_STRESS_HOT_WRITES_PER_LOADER:-100}
value_size=${DISKEYV_STRESS_VALUE_SIZE:-128}

for setting in "$worker_count" "$record_count" "$partition_count" \
    "$loader_count" "$hot_writes" "$value_size"; do
    [[ "$setting" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid stress setting" >&2; exit 2; }
done
if ((worker_count > 64 || record_count + partition_count > 3500 || value_size > 1048576)); then
    echo "Stress settings exceed worker, index, or value limits" >&2
    exit 2
fi

cleanup() {
    podman rm --force "${containers[@]}" >/dev/null 2>&1 || true
    podman network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

container_for() {
    local requested=$1 index
    for ((index = 0; index < replica_count; ++index)); do
        if [[ ${aliases[index]} == "$requested" ]]; then echo "${containers[index]}"; return; fi
    done
    return 2
}

run_client() {
    local container
    container=$(container_for "$1")
    shift
    podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
        /usr/local/bin/diskeyv-client "$@"
}

wait_healthy() {
    local container=$1
    for _ in {1..80}; do
        if podman exec "$container" /usr/local/bin/diskeyv-client health 5000 \
            >/dev/null 2>&1; then return; fi
        sleep 0.1
    done
    podman logs "$container" >&2 || true
    return 1
}

start_replica() {
    local index=$1 mode=$2 peer
    local peers=()
    for ((peer = 0; peer < replica_count; ++peer)); do
        ((peer == index)) || peers+=("${aliases[peer]}:5000")
    done
    podman run --detach --name "${containers[index]}" \
        --network "$network" --network-alias "${aliases[index]}" \
        --init --read-only --tmpfs /tmp:size=16m --cpus 2 --memory 512m \
        --security-opt no-new-privileges \
        -e "DISKEYV_REPLICA_ID=$((index + 1))" \
        -e "DISKEYV_WORKERS=$worker_count" \
        "$image" "$mode" 5000 "${peers[@]}" >/dev/null
}

probe_leader() {
    local attempt=$1 winners=() index=0
    for replica in "${aliases[@]}"; do
        if run_client "$replica" put 5000 "stress-probe-${attempt}-${index}" "$index" \
            >/dev/null 2>&1; then winners+=("$replica"); fi
        index=$((index + 1))
    done
    ((${#winners[@]} == 1)) || return 1
    echo "${winners[0]}"
}

wait_for_leader() {
    local candidate confirmation
    for attempt in {1..80}; do
        candidate=$(probe_leader "$attempt" || true)
        if [[ -n "$candidate" ]]; then
            sleep 0.2
            confirmation=$(probe_leader "confirm-$attempt" || true)
            [[ "$confirmation" == "$candidate" ]] && { echo "$candidate"; return; }
        fi
        sleep 0.1
    done
    return 1
}

bulk_operation() {
    local operation=$1 replica=$2 prefix=$3 count=$4 expected_term=${5:-}
    local container base_count remainder loader shard_count shard_prefix failed=0
    local pids=()
    container=$(container_for "$replica")
    base_count=$((count / loader_count)); remainder=$((count % loader_count))
    for ((loader = 0; loader < loader_count; ++loader)); do
        shard_count=$base_count
        ((loader < remainder)) && shard_count=$((shard_count + 1))
        ((shard_count == 0)) && continue
        shard_prefix="${prefix}s${loader}-"
        if [[ "$operation" == load ]]; then
            podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
                /usr/local/bin/diskeyv-client load 5000 "$shard_prefix" \
                "$shard_count" "$value_size" >/dev/null &
        else
            podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
                /usr/local/bin/diskeyv-client verify 5000 "$shard_prefix" \
                "$shard_count" "$value_size" "$expected_term" >/dev/null &
        fi
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do if ! wait "$pid"; then failed=1; fi; done
    ((failed == 0))
}

wait_bulk() {
    local replica=$1 prefix=$2 count=$3 term=$4
    for _ in {1..50}; do
        if bulk_operation verify "$replica" "$prefix" "$count" "$term" \
            >/dev/null 2>&1; then return; fi
        sleep 0.1
    done
    echo "Replica $replica did not converge for $prefix" >&2
    return 1
}

wait_exact_read() {
    local replica=$1 key=$2 expected=$3 actual
    for _ in {1..50}; do
        actual=$(run_client "$replica" get 5000 "$key" 2>/dev/null || true)
        [[ "$actual" == "$expected" ]] && return
        sleep 0.1
    done
    return 1
}

echo "[1/8] Build and start five election-enabled replicas"
if [[ ${DISKEYV_STRESS_SKIP_BUILD:-0} != 1 ]]; then
    podman build --format docker --target runtime --tag "$image" "$repo_dir"
else
    podman image exists "$image"
fi
podman network create "$network" >/dev/null
for ((index = 1; index < replica_count; ++index)); do start_replica "$index" follower; done
for ((index = 1; index < replica_count; ++index)); do wait_healthy "${containers[index]}"; done
start_replica 0 leader; wait_healthy "${containers[0]}"
elected=$(wait_for_leader)
followers=(); for replica in "${aliases[@]}"; do [[ "$replica" == "$elected" ]] || followers+=("$replica"); done

echo "[2/8] Load $record_count unique keys through elected leader $elected"
bulk_operation load "$elected" base- "$record_count"
base_term=$(run_client "$elected" get 5000 base-s0-0 | awk '/^Term:/ {print $2}')

echo "[3/8] Verify every key on all five replicas"
for replica in "${aliases[@]}"; do wait_bulk "$replica" base- "$record_count" "$base_term"; done

echo "[4/8] Contend on one key and compare the final version everywhere"
hot_pids=()
for ((loader = 0; loader < loader_count; ++loader)); do
    podman exec -e DISKEYV_HOST=127.0.0.1 "$(container_for "$elected")" \
        /usr/local/bin/diskeyv-client hotload 5000 consistency-hot \
        "$hot_writes" "$value_size" "$((loader * 1000000))" >/dev/null &
    hot_pids+=("$!")
done
for pid in "${hot_pids[@]}"; do wait "$pid"; done
hot_result=$(run_client "$elected" get 5000 consistency-hot)
for replica in "${aliases[@]}"; do wait_exact_read "$replica" consistency-hot "$hot_result"; done
elected=$(wait_for_leader)
followers=(); for replica in "${aliases[@]}"; do [[ "$replica" == "$elected" ]] || followers+=("$replica"); done

echo "[5/8] Disconnect one follower and continue with four replicas"
lagger=${followers[3]}; lagger_container=$(container_for "$lagger")
podman network disconnect "$network" "$lagger_container"
elected=$(wait_for_leader)
bulk_operation load "$elected" partition- "$partition_count"
partition_term=$(run_client "$elected" get 5000 partition-s0-0 | awk '/^Term:/ {print $2}')

echo "[6/8] Reconnect the follower and verify cumulative-prefix repair"
podman network connect --alias "$lagger" "$network" "$lagger_container"
elected=$(wait_for_leader)
wait_bulk "$lagger" base- "$record_count" "$base_term"
wait_bulk "$lagger" partition- "$partition_count" "$partition_term"
wait_exact_read "$lagger" consistency-hot "$hot_result"

echo "[7/8] Keep exactly quorum three, including the repaired follower"
disconnect_candidates=()
for replica in "${aliases[@]}"; do
    if [[ "$replica" != "$elected" && "$replica" != "$lagger" ]]; then
        disconnect_candidates+=("$replica")
    fi
done
for follower in "${disconnect_candidates[0]}" "${disconnect_candidates[1]}"; do
    podman network disconnect "$network" "$(container_for "$follower")"
done
elected=$(wait_for_leader)
run_client "$elected" put 5000 repaired-follower-quorum 3 >/dev/null
wait_exact_read "$lagger" repaired-follower-quorum \
    "$(run_client "$elected" get 5000 repaired-follower-quorum)"

echo "[8/8] Drop below quorum and require write rejection"
podman network disconnect "$network" "$lagger_container"
if run_client "$elected" put 5000 must-not-commit 9 >/dev/null 2>&1; then
    echo "FAIL: PUT succeeded with fewer than $quorum replicas" >&2
    exit 1
fi

echo "Five-replica stress test passed: quorum $quorum, $record_count base keys, "\
"$partition_count partition keys, $((SECONDS - test_started)) seconds"
