#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
engine=${DISKEYV_CONTAINER_ENGINE:-auto}
docker_command=$(command -v docker 2>/dev/null || true)

if [[ "$engine" == "podman" ]] ||
   { [[ "$engine" == "auto" ]] && command -v podman >/dev/null 2>&1 &&
     { [[ -z "$docker_command" ]] ||
       grep -a -q "exec /usr/bin/podman" "$docker_command" 2>/dev/null; }; }; then
    exec "$repo_dir/tests/podman_recovery_smoke.sh"
fi

if [[ "$engine" != "auto" && "$engine" != "docker" ]]; then
    echo "DISKEYV_CONTAINER_ENGINE must be auto, docker, or podman" >&2
    exit 2
fi
if [[ -z "$docker_command" ]]; then
    echo "Neither Docker nor Podman is available" >&2
    exit 2
fi

project="diskeyv-recovery-${UID:-$(id -u)}-$$"
export DISKEYV_LEADER_WORKERS=1
export DISKEYV_LEADER_PORT=${DISKEYV_RECOVERY_LEADER_PORT:-15200}
export DISKEYV_FOLLOWER1_PORT=${DISKEYV_RECOVERY_FOLLOWER1_PORT:-15201}
export DISKEYV_FOLLOWER2_PORT=${DISKEYV_RECOVERY_FOLLOWER2_PORT:-15202}
compose=(docker compose -p "$project" -f "$repo_dir/compose.yaml")
recovery_compose=(docker compose -p "$project" \
    -f "$repo_dir/compose.yaml" -f "$repo_dir/compose.recovery.yaml")

cleanup() {
    "${recovery_compose[@]}" down --volumes --remove-orphans \
        >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_client() {
    local host=$1
    shift
    "${compose[@]}" run --rm --no-deps \
        -e "DISKEYV_HOST=$host" client "$@"
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

echo "[1/7] Start an isolated term-1 Compose cluster"
"${compose[@]}" up --build --detach --wait

echo "[2/7] Commit and verify term-1 records"
run_client leader put 5000 recovered-key 1,2,3
run_client leader put 5000 term-one 11
for replica in leader follower1 follower2; do
    assert_read "$replica" recovered-key 1 1,2,3
    assert_read "$replica" term-one 1 11
done

echo "[3/7] Replace the failed leader and consolidate into term 2"
"${compose[@]}" stop leader
"${compose[@]}" rm --force leader
"${recovery_compose[@]}" up --detach --no-deps --wait leader
assert_read leader recovered-key 1 1,2,3
run_client leader put 5000 recovered-key 4,5,6
run_client leader put 5000 term-two 22
for replica in leader follower1 follower2; do
    assert_read "$replica" recovered-key 2 4,5,6
    assert_read "$replica" term-two 2 22
done

echo "[4/7] Remove the term-2 leader"
"${recovery_compose[@]}" stop leader
"${recovery_compose[@]}" rm --force leader

echo "[5/7] Verify recovery fails closed with one survivor unreachable"
follower2_container=$("${compose[@]}" ps -q follower2)
network_name=$(docker network ls \
    --filter "label=com.docker.compose.project=$project" \
    --filter "label=com.docker.compose.network=diskeyv" \
    --format '{{.Name}}')
if [[ -z "$follower2_container" || -z "$network_name" ]]; then
    echo "Could not resolve the isolated follower network" >&2
    exit 1
fi
docker network disconnect "$network_name" "$follower2_container"
if "${recovery_compose[@]}" run --rm --no-deps leader \
    >/dev/null 2>&1; then
    echo "FAIL: recovery served without every configured survivor" >&2
    exit 1
fi
assert_read follower1 recovered-key 2 4,5,6
docker network connect --alias follower2 "$network_name" "$follower2_container"

echo "[6/7] Consolidate into term 3 and continue writing"
"${recovery_compose[@]}" up --detach --no-deps --wait leader
assert_read leader recovered-key 2 4,5,6
assert_read leader term-one 1 11
assert_read leader term-two 2 22
run_client leader put 5000 term-three 33
for replica in leader follower1 follower2; do
    assert_read "$replica" term-three 3 33
done

echo "[7/7] Docker recovery simulation passed"
