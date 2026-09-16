#!/usr/bin/env bash
set -euo pipefail

test_started=$SECONDS
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-election-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
replica_count=5
quorum=3
containers=()
replica_aliases=()
for ((index = 1; index <= replica_count; ++index)); do
    containers+=("diskeyv-election-r${index}-$run_id")
    replica_aliases+=("replica${index}")
done

if [[ -n ${CONTAINER_CLI:-} ]]; then
    runtime=$CONTAINER_CLI
elif command -v podman >/dev/null 2>&1; then
    runtime=podman
elif command -v docker >/dev/null 2>&1; then
    runtime=docker
else
    echo "Docker or Podman is required" >&2
    exit 2
fi

cleanup() {
    for container in "${containers[@]}"; do
        "$runtime" rm --force "$container" >/dev/null 2>&1 || true
    done
    "$runtime" network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_healthy() {
    local container=$1
    for _ in {1..80}; do
        if "$runtime" exec "$container" \
            /usr/local/bin/diskeyv-client health 5000 >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "Container did not become healthy: $container" >&2
    "$runtime" logs "$container" >&2 || true
    return 1
}

run_client() {
    local container=$1
    shift
    "$runtime" exec -e DISKEYV_HOST=127.0.0.1 "$container" \
        /usr/local/bin/diskeyv-client "$@"
}

# A successful PUT is a stronger leader probe than process health: followers
# return NOT_LEADER, while a leader must also obtain a replication quorum.
probe_single_leader() {
    local attempt=$1
    local winners=()
    local index=0
    for container in "${containers[@]}"; do
        if run_client "$container" put 5000 \
            "election-probe-${attempt}-${index}" "$index" \
            >/dev/null 2>&1; then
            winners+=("$container")
        fi
        index=$((index + 1))
    done
    ((${#winners[@]} == 1)) || return 1
    echo "${winners[0]}"
}

wait_for_single_leader() {
    local excluded=${1:-}
    local candidate=""
    for attempt in {1..100}; do
        candidate=$(probe_single_leader "$attempt" || true)
        if [[ -n "$candidate" && "$candidate" != "$excluded" ]]; then
            # Require the same winner twice to avoid accepting a transitional
            # observation while a higher-term heartbeat is still propagating.
            sleep 0.2
            local confirmation
            confirmation=$(probe_single_leader "confirm-$attempt" || true)
            if [[ "$confirmation" == "$candidate" ]]; then
                echo "$candidate"
                return 0
            fi
        fi
        sleep 0.1
    done
    echo "No stable single leader was elected" >&2
    for container in "${containers[@]}"; do
        "$runtime" logs "$container" >&2 2>/dev/null || true
    done
    return 1
}

assert_read() {
    local container=$1
    local key=$2
    local expected_value=$3
    local output
    output=$(run_client "$container" get 5000 "$key")
    grep -q "Value: $expected_value" <<<"$output"
}

wait_for_read() {
    local container=$1
    local key=$2
    local expected_value=$3
    for _ in {1..50}; do
        if assert_read "$container" "$key" "$expected_value" \
            >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "Replica did not converge: $container key=$key" >&2
    "$runtime" logs "$container" >&2 || true
    return 1
}

start_replica() {
    local index=$1
    local mode=$2
    local container=${containers[index]}
    local alias=${replica_aliases[index]}
    local peers=()
    local peer
    for ((peer = 0; peer < replica_count; ++peer)); do
        if ((peer != index)); then
            peers+=("${replica_aliases[peer]}:5000")
        fi
    done

    "$runtime" run --detach --name "$container" \
        --network "$network" --network-alias "$alias" \
        --init --read-only --tmpfs /tmp:size=16m --memory 256m \
        --security-opt no-new-privileges:true \
        -e "DISKEYV_REPLICA_ID=$((index + 1))" -e DISKEYV_WORKERS=1 \
        "$image" "$mode" 5000 "${peers[@]}" >/dev/null
}

echo "[1/6] Build the election-enabled image"
if [[ ${DISKEYV_ELECTION_SKIP_BUILD:-0} != 1 ]]; then
    if [[ "$runtime" == "podman" ]]; then
        "$runtime" build --format docker --target runtime --tag "$image" \
            "$repo_dir"
    else
        "$runtime" build --target runtime --tag "$image" "$repo_dir"
    fi
else
    "$runtime" image inspect "$image" >/dev/null
fi

echo "[2/6] Start five replicas with identical voting membership (quorum three)"
"$runtime" network create "$network" >/dev/null
for ((index = 1; index < replica_count; ++index)); do
    start_replica "$index" follower
done
for ((index = 1; index < replica_count; ++index)); do
    wait_healthy "${containers[index]}"
done
start_replica 0 leader
wait_healthy "${containers[0]}"

echo "[3/6] Discover the sole elected leader and commit replicated data"
initial_leader=$(wait_for_single_leader)
run_client "$initial_leader" put 5000 before-failover 7 >/dev/null
initial_read=$(run_client "$initial_leader" get 5000 before-failover)
initial_term=$(awk '/^Term:/ {print $2}' <<<"$initial_read")
[[ "$initial_term" =~ ^[1-9][0-9]*$ ]]
for container in "${containers[@]}"; do
    wait_for_read "$container" before-failover 7
done

echo "[4/6] Stop elected leader $initial_leader"
"$runtime" stop "$initial_leader" >/dev/null

echo "[5/6] Require four survivors to elect one leader with quorum $quorum"
replacement=$(wait_for_single_leader "$initial_leader")
run_client "$replacement" put 5000 after-failover 8 >/dev/null
replacement_read=$(run_client "$replacement" get 5000 after-failover)
replacement_term=$(awk '/^Term:/ {print $2}' <<<"$replacement_read")
[[ "$replacement_term" =~ ^[1-9][0-9]*$ ]]
if ((replacement_term <= initial_term)); then
    echo "Replacement term $replacement_term did not fence term $initial_term" \
        >&2
    exit 1
fi

echo "[6/6] Verify committed data on all four surviving replicas"
for container in "${containers[@]}"; do
    [[ "$container" == "$initial_leader" ]] && continue
    wait_for_read "$container" before-failover 7
    wait_for_read "$container" after-failover 8
done

echo "Five-replica election simulation passed: $initial_leader -> "\
"$replacement, term $initial_term -> $replacement_term, quorum $quorum, "\
"$((SECONDS - test_started)) seconds"
