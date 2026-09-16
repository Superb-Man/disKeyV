#!/usr/bin/env bash
set -euo pipefail

test_started=$SECONDS

if ! command -v podman >/dev/null 2>&1; then
    echo "Podman is not installed" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="${UID:-$(id -u)}-$$"
network="diskeyv-recovery-$run_id"
leader="diskeyv-recovery-leader-$run_id"
follower1="diskeyv-recovery-follower1-$run_id"
follower2="diskeyv-recovery-follower2-$run_id"
follower3="diskeyv-recovery-follower3-$run_id"
follower4="diskeyv-recovery-follower4-$run_id"
image=${DISKEYV_IMAGE:-diskeyv:local}
leader_port=${DISKEYV_RECOVERY_LEADER_PORT:-15200}
follower1_port=${DISKEYV_RECOVERY_FOLLOWER1_PORT:-15201}
follower2_port=${DISKEYV_RECOVERY_FOLLOWER2_PORT:-15202}
follower3_port=${DISKEYV_RECOVERY_FOLLOWER3_PORT:-15203}
follower4_port=${DISKEYV_RECOVERY_FOLLOWER4_PORT:-15204}
worker_count=${DISKEYV_RECOVERY_WORKERS:-4}
record_count=${DISKEYV_RECOVERY_RECORDS:-3000}
loader_count=${DISKEYV_RECOVERY_LOADERS:-16}
value_size=${DISKEYV_RECOVERY_VALUE_SIZE:-256}
leader_cpus=${DISKEYV_RECOVERY_LEADER_CPUS:-4}
follower_cpus=${DISKEYV_RECOVERY_FOLLOWER_CPUS:-2}
containers=("$leader" "$follower1" "$follower2" "$follower3" "$follower4")

for numeric_setting in "$worker_count" "$record_count" "$loader_count" \
    "$value_size" "$leader_cpus" "$follower_cpus"; do
    if [[ ! "$numeric_setting" =~ ^[1-9][0-9]*$ ]]; then
        echo "Recovery load settings must be positive integers" >&2
        exit 2
    fi
done
if ((worker_count > 64 || record_count > 1000000 || value_size > 1048576)); then
    echo "Recovery load settings exceed the supported limits" >&2
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
    local attempts=${2:-40}
    for ((attempt = 0; attempt < attempts; ++attempt)); do
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
    local container
    case "$host" in
        leader) container=$leader ;;
        follower1) container=$follower1 ;;
        follower2) container=$follower2 ;;
        follower3) container=$follower3 ;;
        follower4) container=$follower4 ;;
        *) echo "Unknown replica: $host" >&2; return 2 ;;
    esac
    podman exec -e DISKEYV_HOST=127.0.0.1 "$container" \
        /usr/local/bin/diskeyv-client "$@"
}

assert_read() {
    local host=$1
    local key=$2
    local expected_term=$3
    local expected_value=$4
    local result
    result=$(run_client "$host" get 5000 "$key")
    grep -q "^Term: $expected_term$" <<<"$result"
    grep -q "^Value: $expected_value$" <<<"$result"
}

bulk_operation() {
    local operation=$1
    local host=$2
    local prefix=$3
    local expected_term=${4:-}
    local container
    case "$host" in
        leader) container=$leader ;;
        follower1) container=$follower1 ;;
        follower2) container=$follower2 ;;
        follower3) container=$follower3 ;;
        follower4) container=$follower4 ;;
        *) echo "Unknown replica: $host" >&2; return 2 ;;
    esac

    local base_count=$((record_count / loader_count))
    local remainder=$((record_count % loader_count))
    local pids=()
    for ((loader = 0; loader < loader_count; ++loader)); do
        local shard_count=$base_count
        if ((loader < remainder)); then
            shard_count=$((shard_count + 1))
        fi
        if ((shard_count == 0)); then
            continue
        fi
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
        if ! wait "$pid"; then
            failed=1
        fi
    done
    if ((failed != 0)); then
        echo "Bulk $operation failed against $host" >&2
        return 1
    fi
    echo "Bulk $operation: $record_count records on $host"
}

start_leader() {
    local mode=$1
    podman run --detach --name "$leader" \
        --network "$network" --network-alias leader \
        --init --read-only --tmpfs /tmp:size=16m \
        --security-opt no-new-privileges --cpus "$leader_cpus" --memory 512m \
        -p "127.0.0.1:$leader_port:5000" \
        -e DISKEYV_REPLICA_ID=1 \
        -e "DISKEYV_WORKERS=$worker_count" \
        -e DISKEYV_ELECTION_ENABLED=0 \
        "$image" "$mode" 5000 follower1:5000 follower2:5000 \
        follower3:5000 follower4:5000 >/dev/null
    if [[ "$mode" == "recover" ]]; then
        wait_healthy "$leader" 120
    else
        wait_healthy "$leader"
    fi
}

remove_leader() {
    podman stop "$leader" >/dev/null
    podman rm "$leader" >/dev/null
}

echo "[1/7] Build an isolated recovery image and network"
if [[ ${DISKEYV_RECOVERY_SKIP_BUILD:-0} != 1 ]]; then
    podman build --format docker --target runtime --tag "$image" "$repo_dir"
else
    podman image exists "$image"
fi
podman network create "$network" >/dev/null

echo "[2/7] Start four memory-resident followers"
podman run --detach --name "$follower1" \
    --network "$network" --network-alias follower1 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus "$follower_cpus" --memory 256m \
    -p "127.0.0.1:$follower1_port:5000" \
    -e DISKEYV_REPLICA_ID=2 \
    -e DISKEYV_ELECTION_ENABLED=0 \
    "$image" follower 5000 >/dev/null
podman run --detach --name "$follower2" \
    --network "$network" --network-alias follower2 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus "$follower_cpus" --memory 256m \
    -p "127.0.0.1:$follower2_port:5000" \
    -e DISKEYV_REPLICA_ID=3 \
    -e DISKEYV_ELECTION_ENABLED=0 \
    "$image" follower 5000 >/dev/null
podman run --detach --name "$follower3" \
    --network "$network" --network-alias follower3 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus "$follower_cpus" --memory 256m \
    -p "127.0.0.1:$follower3_port:5000" \
    -e DISKEYV_REPLICA_ID=4 -e DISKEYV_ELECTION_ENABLED=0 \
    "$image" follower 5000 >/dev/null
podman run --detach --name "$follower4" \
    --network "$network" --network-alias follower4 \
    --init --read-only --tmpfs /tmp:size=16m \
    --security-opt no-new-privileges --cpus "$follower_cpus" --memory 256m \
    -p "127.0.0.1:$follower4_port:5000" \
    -e DISKEYV_REPLICA_ID=5 -e DISKEYV_ELECTION_ENABLED=0 \
    "$image" follower 5000 >/dev/null
wait_healthy "$follower1"
wait_healthy "$follower2"
wait_healthy "$follower3"
wait_healthy "$follower4"

echo "[3/7] Commit and replicate term-1 records"
start_leader leader
run_client leader put 5000 recovered-key 1,2,3
run_client leader put 5000 term-one 11
bulk_operation load leader bulk-
for replica in leader follower1 follower2 follower3 follower4; do
    assert_read "$replica" recovered-key 1 1,2,3
    assert_read "$replica" term-one 1 11
    bulk_operation verify "$replica" bulk- 1
done

echo "[4/7] Remove the leader and consolidate into term 2"
remove_leader
start_leader recover
assert_read leader recovered-key 1 1,2,3
assert_read leader term-one 1 11
bulk_operation verify leader bulk- 1
run_client leader put 5000 recovered-key 4,5,6
run_client leader put 5000 term-two 22
bulk_operation load leader bulk-
for replica in leader follower1 follower2 follower3 follower4; do
    assert_read "$replica" recovered-key 2 4,5,6
    assert_read "$replica" term-two 2 22
    bulk_operation verify "$replica" bulk- 2
done

echo "[5/7] Recover from a majority while one follower is unreachable"
remove_leader
podman network disconnect "$network" "$follower4"
start_leader recover
for replica in leader follower1 follower2 follower3; do
    assert_read "$replica" recovered-key 2 4,5,6
    assert_read "$replica" term-two 2 22
    bulk_operation verify "$replica" bulk- 2
done
run_client leader put 5000 majority-recovery 31
majority_term=$(run_client leader get 5000 majority-recovery | \
    awk '/^Term:/ {print $2}')
for replica in leader follower1 follower2 follower3; do
    assert_read "$replica" majority-recovery "$majority_term" 31
done
remove_leader
podman network connect --alias follower4 "$network" "$follower4"

echo "[6/7] Consolidate again and repair the rejoined follower"
start_leader recover
assert_read leader recovered-key 2 4,5,6
assert_read leader term-one 1 11
assert_read leader term-two 2 22
assert_read leader majority-recovery "$majority_term" 31
bulk_operation verify leader bulk- 2
run_client leader put 5000 term-three 33
final_term=$(run_client leader get 5000 term-three | \
    awk '/^Term:/ {print $2}')
for replica in leader follower1 follower2 follower3 follower4; do
    assert_read "$replica" majority-recovery "$majority_term" 31
    assert_read "$replica" term-three "$final_term" 33
done

echo "[7/7] Recovery simulation passed: $record_count records, "\
"$worker_count workers, $loader_count parallel loaders, ports "\
"$leader_port-$follower4_port, $((SECONDS - test_started)) seconds"
