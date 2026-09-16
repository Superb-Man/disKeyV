#!/usr/bin/env bash
set -euo pipefail

test_started=$SECONDS
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-lagging-failover-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
record_count=${DISKEYV_LAGGING_RECORDS:-200}
replica_count=5
quorum=3
aliases=(replica1 replica2 replica3 replica4 replica5)
containers=()
for ((index = 1; index <= replica_count; ++index)); do
    containers+=("diskeyv-lagging-r${index}-$run_id")
done

cleanup() {
    for container in "${containers[@]}"; do
        podman rm --force "$container" >/dev/null 2>&1 || true
    done
    podman network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

container_for() {
    local alias=$1
    local index
    for ((index = 0; index < replica_count; ++index)); do
        [[ "${aliases[index]}" == "$alias" ]] && {
            echo "${containers[index]}"
            return 0
        }
    done
    return 1
}

run_client() {
    local alias=$1
    shift
    podman exec -e DISKEYV_HOST=127.0.0.1 "$(container_for "$alias")" \
        /usr/local/bin/diskeyv-client "$@"
}

wait_healthy() {
    local container=$1
    for _ in {1..80}; do
        if podman exec "$container" /usr/local/bin/diskeyv-client \
            health 5000 >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "Container did not become healthy: $container" >&2
    podman logs "$container" >&2 || true
    return 1
}

start_replica() {
    local index=$1 mode=$2
    local peers=()
    local peer
    for ((peer = 0; peer < replica_count; ++peer)); do
        ((peer == index)) || peers+=("${aliases[peer]}:5000")
    done
    podman run --detach --name "${containers[index]}" \
        --network "$network" --network-alias "${aliases[index]}" \
        --init --read-only --tmpfs /tmp:size=16m --memory 256m \
        --security-opt no-new-privileges:true \
        -e "DISKEYV_REPLICA_ID=$((index + 1))" \
        -e DISKEYV_WORKERS=4 -e DISKEYV_ELECTION_ENABLED=1 \
        "$image" "$mode" 5000 "${peers[@]}" >/dev/null
}

probe_leader() {
    local attempt=$1
    local winners=()
    local index
    for ((index = 0; index < replica_count; ++index)); do
        if run_client "${aliases[index]}" put 5000 \
            "lagging-election-probe-${attempt}-${index}" "$index" \
            >/dev/null 2>&1; then
            winners+=("${aliases[index]}")
        fi
    done
    ((${#winners[@]} == 1)) || return 1
    echo "${winners[0]}"
}

wait_for_leader() {
    local excluded=${1:-}
    local attempt candidate confirmation
    for attempt in {1..120}; do
        candidate=$(probe_leader "$attempt" || true)
        if [[ -n "$candidate" && "$candidate" != "$excluded" ]]; then
            sleep 0.2
            confirmation=$(probe_leader "confirm-$attempt" || true)
            if [[ "$confirmation" == "$candidate" ]]; then
                echo "$candidate"
                return 0
            fi
        fi
        sleep 0.1
    done
    echo "No stable leader was elected" >&2
    return 1
}

has_value() {
    local alias=$1 key=$2 expected=$3
    run_client "$alias" get 5000 "$key" 2>/dev/null | \
        grep -q "^Value: $expected$"
}

wait_for_value() {
    local alias=$1 key=$2 expected=$3
    for _ in {1..60}; do
        has_value "$alias" "$key" "$expected" && return 0
        sleep 0.1
    done
    return 1
}

# Check every key, its original term, and all 64 deterministic value bytes.
verify_history() {
    local alias=$1
    if ! run_client "$alias" verify 5000 quorum-only- "$record_count" 64 \
        "$committed_term"; then
        echo "FAIL: incomplete or corrupted committed history on $alias" >&2
        return 1
    fi
}

echo "[1/8] Build and start five election-enabled replicas"
if [[ ${DISKEYV_LAGGING_SKIP_BUILD:-0} != 1 ]]; then
    podman build --format docker --target runtime --tag "$image" "$repo_dir"
else
    podman image exists "$image"
fi
podman network create "$network" >/dev/null
for ((index = 1; index < replica_count; ++index)); do
    start_replica "$index" follower
done
for ((index = 1; index < replica_count; ++index)); do
    wait_healthy "${containers[index]}"
done
start_replica 0 leader
wait_healthy "${containers[0]}"

echo "[2/8] Discover leader and establish a fully replicated baseline"
initial_leader=$(wait_for_leader)
run_client "$initial_leader" put 5000 common-before-partition 1 >/dev/null
for replica in "${aliases[@]}"; do
    wait_for_value "$replica" common-before-partition 1
done

followers=()
for replica in "${aliases[@]}"; do
    [[ "$replica" == "$initial_leader" ]] || followers+=("$replica")
done
# Isolate the two lowest-ID followers so they become observably stale while
# the leader and two remaining followers retain the minimum write quorum.
laggers=("${followers[0]}" "${followers[1]}")
echo "[3/8] Disconnect lagging replicas ${laggers[*]}"
for replica in "${laggers[@]}"; do
    podman network disconnect "$network" "$(container_for "$replica")"
done

echo "[4/8] Commit $record_count records using leader plus two followers"
run_client "$initial_leader" load 5000 quorum-only- "$record_count" 64 \
    >/dev/null
committed_term=$(run_client "$initial_leader" get 5000 quorum-only-0 | \
    awk '/^Term:/ {print $2}')
[[ "$committed_term" =~ ^[1-9][0-9]*$ ]]
verify_history "$initial_leader"
run_client "$initial_leader" put 5000 committed-while-behind 4,5,6 \
    >/dev/null
for replica in "${followers[@]:2}"; do
    verify_history "$replica"
    wait_for_value "$replica" committed-while-behind 4,5,6
done
for replica in "${laggers[@]}"; do
    # Require a real NOT_FOUND response; a connection error does not prove lag.
    missing_status=0
    missing_output=$(run_client "$replica" get 5000 quorum-only-0 2>&1) || \
        missing_status=$?
    if [[ "$missing_status" != 3 || "$missing_output" != *"GET failed: NOT_FOUND"* ]]; then
        echo "FAIL: could not confirm missing committed data on $replica: $missing_output" >&2
        exit 1
    fi
done

echo "[5/8] SIGKILL leader $initial_leader while two replicas are behind"
failed_container=$(container_for "$initial_leader")
podman kill --signal KILL "$failed_container" >/dev/null
failed_exit=$(podman wait "$failed_container")
if [[ "$failed_exit" != 137 ]]; then
    echo "FAIL: expected SIGKILL exit 137, got $failed_exit" >&2
    exit 1
fi

echo "[6/8] Reconnect both lagging survivors and elect a replacement"
for replica in "${laggers[@]}"; do
    podman network connect --alias "$replica" "$network" \
        "$(container_for "$replica")"
done
replacement=$(wait_for_leader "$initial_leader")

echo "[7/8] Require the new leader to retain quorum-committed history"
verify_history "$replacement"
if ! wait_for_value "$replacement" committed-while-behind 4,5,6; then
    echo "FAIL: stale replica $replacement was elected leader and cannot read committed data" >&2
    exit 1
fi
run_client "$replacement" put 5000 after-lagging-failover 7,8,9 >/dev/null

echo "[8/8] Verify every committed record and new write on all four survivors"
for replica in "${followers[@]}"; do
    verify_history "$replica"
    wait_for_value "$replica" common-before-partition 1
    if ! wait_for_value "$replica" committed-while-behind 4,5,6; then
        echo "FAIL: $replica did not recover the pre-failover committed prefix" >&2
        exit 1
    fi
    wait_for_value "$replica" after-lagging-failover 7,8,9
done

echo "Lagging failover recovery passed: leader $initial_leader -> $replacement, "\
"SIGKILL, $record_count quorum-only records verified on all four survivors, "\
"quorum $quorum, $((SECONDS - test_started)) seconds"
