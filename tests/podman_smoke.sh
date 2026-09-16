#!/usr/bin/env bash
set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
    echo "Podman is not installed" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-smoke-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
replica_count=5
quorum=3
replica_aliases=(leader follower1 follower2 follower3 follower4)
containers=()
for alias in "${replica_aliases[@]}"; do
    containers+=("diskeyv-${alias}-$run_id")
done

cleanup() {
    podman rm --force "${containers[@]}" >/dev/null 2>&1 || true
    podman network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

container_for_alias() {
    local requested=$1
    local index
    for ((index = 0; index < replica_count; ++index)); do
        if [[ ${replica_aliases[index]} == "$requested" ]]; then
            echo "${containers[index]}"
            return 0
        fi
    done
    return 2
}

wait_healthy() {
    local container=$1
    for _ in {1..80}; do
        if podman exec "$container" \
            /usr/local/bin/diskeyv-client health 5000 >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    podman logs "$container" >&2 || true
    return 1
}

run_client() {
    local container
    container=$(container_for_alias "$1")
    shift
    podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
        /usr/local/bin/diskeyv-client "$@"
}

start_replica() {
    local index=$1
    local mode=$2
    local peers=()
    local peer
    for ((peer = 0; peer < replica_count; ++peer)); do
        ((peer == index)) || peers+=("${replica_aliases[peer]}:5000")
    done
    podman run --detach --name "${containers[index]}" \
        --network "$network" --network-alias "${replica_aliases[index]}" \
        --init --read-only --tmpfs /tmp:size=16m \
        --security-opt no-new-privileges --cpus 1 --memory 256m \
        -e "DISKEYV_REPLICA_ID=$((index + 1))" -e DISKEYV_WORKERS=1 \
        "$image" "$mode" 5000 "${peers[@]}" >/dev/null
}

probe_single_leader() {
    local attempt=$1
    local winners=()
    local index=0
    for replica in "${replica_aliases[@]}"; do
        if run_client "$replica" put 5000 \
            "smoke-leader-probe-${attempt}-${index}" "$index" \
            >/dev/null 2>&1; then
            winners+=("$replica")
        fi
        index=$((index + 1))
    done
    ((${#winners[@]} == 1)) || return 1
    echo "${winners[0]}"
}

wait_for_single_leader() {
    local candidate confirmation
    for attempt in {1..80}; do
        candidate=$(probe_single_leader "$attempt" || true)
        if [[ -n "$candidate" ]]; then
            sleep 0.2
            confirmation=$(probe_single_leader "confirm-$attempt" || true)
            if [[ "$confirmation" == "$candidate" ]]; then
                echo "$candidate"
                return 0
            fi
        fi
        sleep 0.1
    done
    for container in "${containers[@]}"; do
        podman logs "$container" >&2 || true
    done
    return 1
}

wait_for_value() {
    local replica=$1 key=$2 expected=$3 output
    for _ in {1..50}; do
        output=$(run_client "$replica" get 5000 "$key" 2>/dev/null || true)
        if grep -q "Value: $expected" <<<"$output"; then return 0; fi
        sleep 0.1
    done
    echo "Replica $replica did not converge for $key" >&2
    return 1
}

podman build --format docker --target runtime --tag "$image" "$repo_dir"
podman network create "$network" >/dev/null
for ((index = 1; index < replica_count; ++index)); do
    start_replica "$index" follower
done
for ((index = 1; index < replica_count; ++index)); do
    wait_healthy "${containers[index]}"
done
start_replica 0 leader
wait_healthy "${containers[0]}"

elected_leader=$(wait_for_single_leader)
followers=()
for replica in "${replica_aliases[@]}"; do
    [[ "$replica" == "$elected_leader" ]] || followers+=("$replica")
done

run_client "$elected_leader" put 5000 podman-smoke 7,8,9
for replica in "${replica_aliases[@]}"; do
    wait_for_value "$replica" podman-smoke 7,8,9
done

# Restart one empty follower and force cumulative-prefix repair.
repair_follower=${followers[3]}
repair_container=$(container_for_alias "$repair_follower")
podman stop "$repair_container" >/dev/null
run_client "$elected_leader" put 5000 one-follower-down 1
podman start "$repair_container" >/dev/null
wait_healthy "$repair_container"
run_client "$elected_leader" put 5000 repair-trigger 4
wait_for_value "$repair_follower" podman-smoke 7,8,9
wait_for_value "$repair_follower" one-follower-down 1

# With the leader and two followers alive, the five-node quorum still holds.
for follower in "${followers[0]}" "${followers[1]}"; do
    podman stop "$(container_for_alias "$follower")" >/dev/null
done
run_client "$elected_leader" put 5000 repaired-follower-quorum 5

# Removing one more follower leaves only two replicas, below quorum three.
podman stop "$repair_container" >/dev/null
if run_client "$elected_leader" put 5000 no-quorum 2; then
    echo "FAIL: PUT succeeded without a replication quorum" >&2
    exit 1
fi

echo "Five-replica Podman smoke test passed (quorum $quorum)"
