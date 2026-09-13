#!/usr/bin/env bash
set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
    echo "Podman is not installed" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-smoke-$run_id"
leader="diskeyv-leader-$run_id"
follower1="diskeyv-follower1-$run_id"
follower2="diskeyv-follower2-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
leader_port=${DISKEYV_LEADER_PORT:-5000}
follower1_port=${DISKEYV_FOLLOWER1_PORT:-5001}
follower2_port=${DISKEYV_FOLLOWER2_PORT:-5002}
containers=()

cleanup() {
    if ((${#containers[@]} > 0)); then
        podman rm --force "${containers[@]}" >/dev/null 2>&1 || true
    fi
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
        sleep 0.5
    done
    echo "Container did not become healthy: $container" >&2
    podman logs "$container" >&2 || true
    return 1
}

run_client() {
    local host=$1
    shift
    podman run --rm --network "$network" \
        -e "DISKEYV_HOST=$host" \
        --entrypoint /usr/local/bin/diskeyv-client \
        "$image" "$@"
}

podman build --format docker --target runtime --tag "$image" "$repo_dir"
podman network create "$network" >/dev/null

podman run --detach --name "$follower1" \
    --network "$network" --network-alias follower1 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus 1 --memory 256m \
    -p "127.0.0.1:$follower1_port:5000" \
    -e DISKEYV_REPLICA_ID=2 \
    "$image" follower 5000 >/dev/null
containers+=("$follower1")

podman run --detach --name "$follower2" \
    --network "$network" --network-alias follower2 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus 1 --memory 256m \
    -p "127.0.0.1:$follower2_port:5000" \
    -e DISKEYV_REPLICA_ID=3 \
    "$image" follower 5000 >/dev/null
containers+=("$follower2")

wait_healthy "$follower1"
wait_healthy "$follower2"

podman run --detach --name "$leader" \
    --network "$network" --network-alias leader \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus 2 --memory 512m \
    -p "127.0.0.1:$leader_port:5000" \
    -e DISKEYV_REPLICA_ID=1 \
    -e DISKEYV_WORKERS=1 \
    "$image" leader 5000 follower1:5000 follower2:5000 >/dev/null
containers+=("$leader")
wait_healthy "$leader"

run_client leader put 5000 podman-smoke 7,8,9
leader_read=$(run_client leader get 5000 podman-smoke)
follower1_read=$(run_client follower1 get 5000 podman-smoke)
follower2_read=$(run_client follower2 get 5000 podman-smoke)

grep -q "Value: 7,8,9" <<<"$leader_read"
grep -q "Value: 7,8,9" <<<"$follower1_read"
grep -q "Value: 7,8,9" <<<"$follower2_read"

podman stop "$follower2" >/dev/null
run_client leader put 5000 one-follower-down 1

podman start "$follower2" >/dev/null
wait_healthy "$follower2"
run_client leader put 5000 repair-trigger 4
repaired_initial=$(run_client follower2 get 5000 podman-smoke)
repaired_missed=$(run_client follower2 get 5000 one-follower-down)
grep -q "Value: 7,8,9" <<<"$repaired_initial"
grep -q "Value: 1" <<<"$repaired_missed"

podman stop "$follower1" >/dev/null
run_client leader put 5000 repaired-follower-quorum 5

podman stop "$follower2" >/dev/null
if run_client leader put 5000 no-quorum 2; then
    echo "FAIL: PUT succeeded without a replication quorum" >&2
    exit 1
fi

echo "Podman smoke test passed"
